package kube

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type crdResource struct {
	kind    domain.ControllerKind
	cluster bool
	new     func() crclient.Object
	newList func() crclient.ObjectList
}

// workflowCRDResourceRegistry constructs fresh descriptors for each call.
// Descriptors contain function values and are process-wide policy; returning a
// new slice keeps filters and tests from mutating shared routing state.
func workflowCRDResourceRegistry() []crdResource {
	return []crdResource{
		{
			kind:    domain.ControllerKindMigration,
			new:     func() crclient.Object { return &v1alpha1.Migration{} },
			newList: func() crclient.ObjectList { return &v1alpha1.MigrationList{} },
		},
		{
			kind:    domain.ControllerKindClusterMigration,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.ClusterMigration{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ClusterMigrationList{} },
		},
		{
			kind:    domain.ControllerKindPodMigration,
			new:     func() crclient.Object { return &v1alpha1.PodMigration{} },
			newList: func() crclient.ObjectList { return &v1alpha1.PodMigrationList{} },
		},
		{
			kind:    domain.ControllerKindClusterPodMigration,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.ClusterPodMigration{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ClusterPodMigrationList{} },
		},
		{
			kind:    domain.ControllerKindReservation,
			new:     func() crclient.Object { return &v1alpha1.Reservation{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ReservationList{} },
		},
		{
			kind:    domain.ControllerKindClusterReservation,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.ClusterReservation{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ClusterReservationList{} },
		},
		{
			kind:    domain.ControllerKindCopy,
			new:     func() crclient.Object { return &v1alpha1.Copy{} },
			newList: func() crclient.ObjectList { return &v1alpha1.CopyList{} },
		},
		{
			kind:    domain.ControllerKindClusterCopy,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.ClusterCopy{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ClusterCopyList{} },
		},
		{
			kind:    domain.ControllerKindBackup,
			new:     func() crclient.Object { return &v1alpha1.Backup{} },
			newList: func() crclient.ObjectList { return &v1alpha1.BackupList{} },
		},
		{
			kind:    domain.ControllerKindRestore,
			new:     func() crclient.Object { return &v1alpha1.Restore{} },
			newList: func() crclient.ObjectList { return &v1alpha1.RestoreList{} },
		},
		{
			kind:    domain.ControllerKindRename,
			new:     func() crclient.Object { return &v1alpha1.Rename{} },
			newList: func() crclient.ObjectList { return &v1alpha1.RenameList{} },
		},
		{
			kind:    domain.ControllerKindClusterMove,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.ClusterMove{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ClusterMoveList{} },
		},
	}
}

func workflowCRDResource(kind domain.ControllerKind) (crdResource, bool) {
	for _, resource := range workflowCRDResourceRegistry() {
		if resource.kind == kind {
			return resource, true
		}
	}

	return crdResource{}, false
}

func workflowCRDKind(sessionType domain.SessionType) domain.ControllerKind {
	workflow, ok := domain.ControllerWorkflowForType(sessionType)
	if !ok {
		return ""
	}

	return workflow.Kind
}

func workflowCRDKindForSession(session *domain.Session) domain.ControllerKind {
	resource, ok := domain.ControllerResourceForSession(session)
	if !ok {
		return ""
	}

	return resource.Kind
}

func resourceKey(resource crdResource, namespace, name string) crclient.ObjectKey {
	if resource.cluster {
		namespace = ""
	}
	return crclient.ObjectKey{Namespace: namespace, Name: name}
}

func resourceNamespace(resource crdResource, namespace string) string {
	if resource.cluster {
		return ""
	}
	return namespace
}

// CRDSessionStore persists the domain session envelope as one of the namespaced
// operation-specific workflow custom resources. It deliberately implements the same interface
// as ConfigMapSessionStore so existing workflows remain storage-agnostic.
//
// The generated API type and controller-runtime client keep serialization,
// status-subresource handling, resource-version conflicts, and fake-client
// behavior aligned with Kubebuilder conventions.
type CRDSessionStore struct {
	client         crclient.Client
	leaseClient    kubernetes.Interface
	supportedKinds map[domain.ControllerKind]struct{}
}

func NewCRDSessionStore(client crclient.Client) *CRDSessionStore {
	return &CRDSessionStore{client: client}
}

// WithLeaseClient enables the same per-session Kubernetes Lease fencing used
// by ConfigMap sessions.
func (s *CRDSessionStore) WithLeaseClient(client kubernetes.Interface) *CRDSessionStore {
	if s != nil {
		s.leaseClient = client
	}

	return s
}

// WithSupportedKinds restricts the store to CRDs discovered on the target
// cluster. A nil or empty list keeps the default all-kinds behavior used by
// tests and by an explicitly complete installation.
func (s *CRDSessionStore) WithSupportedKinds(kinds []domain.ControllerKind) *CRDSessionStore {
	if s == nil || len(kinds) == 0 {
		return s
	}

	s.supportedKinds = make(map[domain.ControllerKind]struct{}, len(kinds))
	for _, kind := range kinds {
		if _, ok := workflowCRDResource(kind); ok {
			s.supportedKinds[kind] = struct{}{}
		}
	}

	return s
}

func (s *CRDSessionStore) SupportsType(sessionType domain.SessionType) bool {
	if s == nil {
		return false
	}

	workflow, ok := domain.ControllerWorkflowForType(sessionType)
	if !ok || workflow.Kind == "" && workflow.ClusterKind == "" {
		return false
	}

	if len(s.supportedKinds) == 0 {
		return true
	}

	if workflow.Kind != "" {
		if _, found := s.supportedKinds[workflow.Kind]; found {
			return true
		}
	}

	_, found := s.supportedKinds[workflow.ClusterKind]

	return workflow.ClusterKind != "" && found
}

func (s *CRDSessionStore) supportsSession(session *domain.Session) bool {
	if s == nil || session == nil {
		return false
	}

	kind := workflowCRDKindForSession(session)
	if kind == "" {
		return false
	}

	return s.supportsKind(kind)
}

func (s *CRDSessionStore) supportsKind(kind domain.ControllerKind) bool {
	if s == nil || kind == "" {
		return false
	}

	if len(s.supportedKinds) == 0 {
		return true
	}

	_, ok := s.supportedKinds[kind]

	return ok
}

func (s *CRDSessionStore) resources() []crdResource {
	if s == nil || len(s.supportedKinds) == 0 {
		// Keep the process-wide registry immutable from callers. The resource
		// descriptors contain function values, so a shallow slice copy is
		// sufficient and avoids sharing the backing array with future filters.
		return workflowCRDResourceRegistry()
	}

	resources := make([]crdResource, 0, len(s.supportedKinds))
	for _, resource := range workflowCRDResourceRegistry() {
		if _, ok := s.supportedKinds[resource.kind]; ok {
			resources = append(resources, resource)
		}
	}

	return resources
}

// AcquireSessionLock makes CRD-backed sessions participate in the existing
// service-level fencing protocol. A missing lease client is rejected instead
// of silently allowing concurrent mutations.
func (s *CRDSessionStore) AcquireSessionLock(
	ctx context.Context,
	namespace, id string,
) (SessionLock, error) {
	if s == nil || s.leaseClient == nil {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"acquire session lock",
			"CRD session lease client is not configured",
		)
	}

	return (&ConfigMapSessionStore{client: s.leaseClient}).AcquireSessionLock(ctx, namespace, id)
}

// SessionLockID scopes CRD fencing by operation kind. Kubernetes allows
// same-name objects in different CRDs, and each kind has an independent
// controller/reconcile stream.
func (s *CRDSessionStore) SessionLockID(session *domain.Session) string {
	return SessionLockID(session)
}

// DeleteSessionLease removes the Lease associated with a CRD-backed session
// after cleanup. It mirrors ConfigMap session lifecycle semantics.
func (s *CRDSessionStore) DeleteSessionLease(
	ctx context.Context,
	namespace, id string,
) error {
	if s == nil || s.leaseClient == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"delete session lock",
			"CRD session lease client is not configured",
		)
	}

	return (&ConfigMapSessionStore{client: s.leaseClient}).DeleteSessionLease(ctx, namespace, id)
}

// EnsureSessionProtection applies the same deletion guard used by
// CLI-created workflows to declarative CRs. It intentionally updates only
// metadata, leaving spec and status untouched. A workflow already pending
// deletion is never re-finalized because Kubernetes may be completing a
// user-approved cleanup path.
func (s *CRDSessionStore) EnsureSessionProtection(
	ctx context.Context,
	session *domain.Session,
) error {
	if session == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"ensure session protection",
			"session is nil",
		)
	}

	if err := s.configured("ensure session protection"); err != nil {
		return err
	}

	resourceKind := session.BackendResource
	if resourceKind == "" {
		resourceKind = workflowCRDKindForSession(session)
	}

	resource, ok := workflowCRDResource(resourceKind)
	if !ok {
		return domain.NewError(
			domain.ErrorValidation,
			"ensure session protection",
			"unsupported workflow resource",
		)
	}

	current := resource.new()
	if err := s.client.Get(
		ctx,
		resourceKey(resource, session.Spec.SessionNamespace, session.ID),
		current,
	); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"ensure session protection",
			"read "+string(resource.kind),
			err,
		)
	}

	if err := ValidateWorkflowMetadata(
		current,
		session.ID,
		resourceNamespace(resource, session.Spec.SessionNamespace),
	); err != nil {
		return domain.WrapError(
			domain.ErrorConflict,
			"ensure session protection",
			string(resource.kind)+" ownership does not match the session",
			err,
		)
	}

	if session.BackendUID != "" && current.GetUID() != session.BackendUID {
		return domain.NewError(
			domain.ErrorConflict,
			"ensure session protection",
			"workflow identity changed while adding protection",
		)
	}

	if current.GetDeletionTimestamp() != nil ||
		containsString(current.GetFinalizers(), SessionFinalizer) {
		return nil
	}

	current.SetFinalizers(ensureSessionFinalizer(current.GetFinalizers()))

	if err := s.client.Update(ctx, current); apierrors.IsConflict(err) {
		return domain.NewError(
			domain.ErrorConflict,
			"ensure session protection",
			"workflow changed while adding protection",
		)
	} else if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"ensure session protection",
			"add session protection finalizer",
			err,
		)
	}

	// Adding the finalizer is a metadata update and therefore advances the
	// resource version. Keep the in-memory session aligned so the same
	// reconcile can durably persist a captured workload snapshot or status
	// without manufacturing a conflict against its own protection update.
	session.ResourceVersion = current.GetResourceVersion()
	session.Generation = current.GetGeneration()
	session.Backend = SessionBackendCRD
	session.BackendUID = current.GetUID()
	session.BackendResource = resource.kind

	return nil
}

func (s *CRDSessionStore) configured(operation string) error {
	if s == nil || s.client == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			operation,
			"CRD session client is not configured",
		)
	}

	return nil
}

func (s *CRDSessionStore) Create(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "create session", "session is nil")
	}

	if err := s.configured("create session"); err != nil {
		return err
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if !ControllerSessionSupported(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"create session",
			"this workflow is not valid for the selected namespaced or cluster controller resource",
		)
	}

	if !s.supportsSession(session) {
		workflow, _ := domain.ControllerWorkflowForType(session.Spec.Type)

		kind := workflow.Kind
		if clusterKind := workflow.ClusterKind; clusterKind != "" &&
			workflowCRDKindForSession(session) == clusterKind {
			kind = clusterKind
		}

		return domain.NewError(
			domain.ErrorPrecondition,
			"create session",
			string(kind)+" CRD is not served by this cluster",
		)
	}

	object := sessionObjectFor(session)
	if object == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"create session",
			"unsupported workflow type",
		)
	}

	if err := s.client.Create(ctx, object); apierrors.IsAlreadyExists(err) {
		return domain.WrapError(
			domain.ErrorConflict,
			"create session",
			fmt.Sprintf("session %s already exists", session.ID),
			err,
		)
	} else if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"create session",
			fmt.Sprintf(
				"create %s: %v",
				object.GetObjectKind().GroupVersionKind().Kind,
				err,
			),
			err,
		)
	}

	// CRDs with a status subresource ignore status on the main create call.
	// Persist the initial Planned checkpoint before returning to the caller.
	session.Status.ObservedGeneration = object.GetGeneration()

	statusObject, err := s.initializeStatus(ctx, session, object)
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"create session",
			"initialize workflow status",
			err,
		)
	}

	if latest, decodeErr := DecodeWorkflow(statusObject); decodeErr == nil {
		session.Generation = latest.Generation
		session.Status = latest.Status
		session.BackendResource = latest.BackendResource
	}

	session.ResourceVersion = statusObject.GetResourceVersion()
	session.Backend = SessionBackendCRD
	session.BackendUID = statusObject.GetUID()

	return nil
}

// initializeStatus handles the short race between a CR create and the first
// controller reconcile. A controller may update the object before the CLI's
// initial status checkpoint reaches the API server; in that case the latest
// controller-owned status is authoritative and must be preserved.
func (s *CRDSessionStore) initializeStatus(
	ctx context.Context,
	session *domain.Session,
	created crclient.Object,
) (crclient.Object, error) {
	if session == nil || created == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"create session",
			"session and workflow resource are required",
		)
	}

	current, ok := created.DeepCopyObject().(crclient.Object)
	if !ok {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"create session",
			"workflow deep copy does not implement client.Object",
		)
	}

	for range 3 {
		if !setWorkflowStatus(current, session.Spec, session.Status) {
			return nil, domain.NewError(
				domain.ErrorInternal,
				"create session",
				"unsupported workflow resource",
			)
		}

		if err := s.client.Status().Update(ctx, current); err == nil {
			return current, nil
		} else if !apierrors.IsConflict(err) {
			// Status is intentionally controller-owned. A tenant may create a
			// workflow without status update permission; the controller will
			// initialize Planned on its first reconcile.
			if apierrors.IsForbidden(err) {
				return current, nil
			}

			return nil, err
		}

		latestKind := workflowKind(current)

		latestResource, resourceOK := workflowCRDResource(latestKind)
		if !resourceOK {
			return nil, domain.NewError(
				domain.ErrorInternal,
				"create session",
				"unsupported workflow resource",
			)
		}

		latest := latestResource.new()
		if latest == nil {
			return nil, domain.NewError(
				domain.ErrorInternal,
				"create session",
				"unsupported workflow resource",
			)
		}

		if err := s.client.Get(
			ctx,
			resourceKey(latestResource, session.Spec.SessionNamespace, session.ID),
			latest,
		); err != nil {
			return nil, err
		}

		latestSession, decodeErr := DecodeWorkflow(latest)
		if decodeErr != nil {
			return nil, decodeErr
		}

		if latestSession.Status.Phase != "" {
			// The controller has already initialized or advanced status. Keep
			// that checkpoint and let the caller observe the latest RV.
			return latest, nil
		}

		current = latest
	}

	return nil, domain.NewError(
		domain.ErrorConflict,
		"create session",
		"workflow status changed while initializing",
	)
}

func (s *CRDSessionStore) Get(ctx context.Context, namespace, id string) (*domain.Session, error) {
	if err := s.configured("get session"); err != nil {
		return nil, err
	}

	resources := s.resources()
	objects := make([]crclient.Object, len(resources))
	errors := make([]error, len(resources))
	parallel.For(len(resources), func(index int) {
		resource := resources[index]
		object := resource.new()

		errors[index] = s.client.Get(
			ctx,
			resourceKey(resource, namespace, id),
			object,
		)
		if errors[index] == nil {
			objects[index] = object
		}
	})

	var found *domain.Session

	forbidden := 0
	for index, resource := range resources {
		err := errors[index]
		if apierrors.IsNotFound(err) {
			continue
		}

		if apierrors.IsForbidden(err) {
			// The caller may be bound to a single workflow Kind. Treat
			// inaccessible sibling Kinds as absent so Get can resolve the
			// resource the caller is authorized to observe.
			forbidden++
			continue
		}

		if err != nil {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				"get session",
				"read "+string(resource.kind),
				err,
			)
		}

		session, decodeErr := DecodeWorkflow(objects[index])
		if decodeErr != nil {
			return nil, decodeErr
		}

		if resource.cluster && namespace != "" && session.Spec.SessionNamespace != namespace {
			continue
		}

		if found != nil {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"get session",
				fmt.Sprintf("session %s/%s exists in multiple workflow kinds", namespace, id),
			)
		}

		found = session
	}

	if found != nil {
		return found, nil
	}

	if forbidden == len(resources) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"get session",
			fmt.Sprintf(
				"no workflow kind in session namespace %s is readable by this identity",
				namespace,
			),
		)
	}

	return nil, domain.NewError(
		domain.ErrorValidation,
		"get session",
		fmt.Sprintf("session %s/%s does not exist", namespace, id),
	)
}

// GetByKind reads exactly one workflow resource. Controllers use this method
// because reconcile requests carry only namespace/name; the watched kind is
// supplied by the controller registration and must never be inferred by
// scanning unrelated CRDs.
func (s *CRDSessionStore) GetByKind(
	ctx context.Context,
	namespace, id string,
	kind domain.ControllerKind,
) (*domain.Session, error) {
	if err := s.configured("get session"); err != nil {
		return nil, err
	}

	resource, ok := workflowCRDResource(kind)
	if !ok || !s.supportsKind(kind) {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"get session",
			fmt.Sprintf("unsupported workflow kind %q", kind),
		)
	}

	object := resource.new()
	if err := s.client.Get(ctx, resourceKey(resource, namespace, id), object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, domain.NewError(
				domain.ErrorValidation,
				"get session",
				fmt.Sprintf("session %s/%s does not exist", namespace, id),
			)
		}

		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"get session",
			"read "+string(resource.kind),
			err,
		)
	}

	session, err := DecodeWorkflow(object)
	if err != nil {
		return nil, err
	}

	return session, nil
}

func (s *CRDSessionStore) GetByType(
	ctx context.Context,
	namespace, id string,
	sessionType domain.SessionType,
) (*domain.Session, error) {
	workflow, ok := domain.ControllerWorkflowForType(sessionType)
	if !ok {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"get session",
			fmt.Sprintf("unsupported session type %q", sessionType),
		)
	}

	kinds := []domain.ControllerKind{workflow.Kind}
	if workflow.ClusterKind != "" {
		kinds = append(kinds, workflow.ClusterKind)
	}

	var found *domain.Session
	for _, kind := range kinds {
		session, err := s.GetByKind(ctx, namespace, id, kind)

		if IsSessionNotFound(err) {
			continue
		}

		if apierrors.IsForbidden(err) {
			continue
		}

		if err != nil {
			return nil, err
		}

		if found != nil {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"get session",
				fmt.Sprintf(
					"session %s/%s exists in multiple %s workflow kinds",
					namespace,
					id,
					sessionType,
				),
			)
		}

		found = session
	}

	if found != nil {
		return found, nil
	}

	return nil, domain.NewError(
		domain.ErrorValidation,
		"get session",
		fmt.Sprintf("session %s/%s does not exist", namespace, id),
	)
}

func (s *CRDSessionStore) Update(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorConflict, "update session", "session is required")
	}

	if err := s.configured("update session"); err != nil {
		return err
	}

	if err := session.Validate(); err != nil {
		return err
	}

	storedKind := session.BackendResource
	if storedKind == "" {
		storedKind = workflowCRDKindForSession(session)
	}

	storedWorkflow, ok := domain.ControllerResourceForKind(storedKind)
	if !ok {
		return domain.NewError(
			domain.ErrorValidation,
			"update session",
			"unsupported workflow resource",
		)
	}

	desiredWorkflow, ok := domain.ControllerResourceForSpec(session.Spec)
	if !ok || desiredWorkflow.Cluster != storedWorkflow.Cluster ||
		!controllerSessionSupportedForResource(session, desiredWorkflow) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"update session",
			"this workflow is not valid for the selected namespaced or cluster controller resource",
		)
	}

	if !s.supportsKind(desiredWorkflow.Kind) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"update session",
			string(desiredWorkflow.Kind)+" CRD is not served by this cluster",
		)
	}

	resource, ok := workflowCRDResource(storedKind)
	if !ok {
		return domain.NewError(
			domain.ErrorValidation,
			"update session",
			"unsupported workflow resource",
		)
	}

	existing := resource.new()
	if err := s.client.Get(
		ctx,
		resourceKey(resource, session.Spec.SessionNamespace, session.ID),
		existing,
	); apierrors.IsNotFound(
		err,
	) {
		return domain.NewError(
			domain.ErrorConflict,
			"update session",
			"session "+string(resource.kind)+" disappeared",
		)
	} else if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"update session",
			"read "+string(resource.kind)+" metadata",
			err,
		)
	}

	if err := ValidateWorkflowMetadata(
		existing,
		session.ID,
		resourceNamespace(resource, session.Spec.SessionNamespace),
	); err != nil {
		return domain.WrapError(
			domain.ErrorConflict,
			"update session",
			string(resource.kind)+" ownership does not match the session",
			err,
		)
	}

	if existing.GetResourceVersion() != session.ResourceVersion {
		return domain.NewError(
			domain.ErrorConflict,
			"update session",
			"session changed by another process",
		)
	}

	desiredKind := desiredWorkflow.Kind

	if desiredKind != storedKind {
		return s.rebindWorkflowResource(ctx, session, existing, desiredKind)
	}

	updated, ok := existing.DeepCopyObject().(crclient.Object)
	if !ok {
		return domain.NewError(
			domain.ErrorInternal,
			"update session",
			"workflow deep copy does not implement client.Object",
		)
	}

	if !setWorkflowSpec(updated, session.Spec) {
		return domain.NewError(
			domain.ErrorInternal,
			"update session",
			"unsupported workflow resource",
		)
	}

	updated.SetLabels(MergeSessionLabels(existing.GetLabels(), session.ID))
	updated.SetFinalizers(ensureSessionFinalizer(updated.GetFinalizers()))

	if !apiequality.Semantic.DeepEqual(existing, updated) {
		if err := s.client.Update(ctx, updated); apierrors.IsConflict(err) {
			return domain.NewError(
				domain.ErrorConflict,
				"update session",
				"session changed by another process",
			)
		} else if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"update session",
				fmt.Sprintf("update %s spec: %v", resource.kind, err),
				err,
			)
		}
	}

	session.Status.ObservedGeneration = updated.GetGeneration()

	status := session.Status
	if !setWorkflowStatus(updated, session.Spec, status) {
		return domain.NewError(
			domain.ErrorInternal,
			"update session",
			"unsupported workflow resource",
		)
	}

	if err := s.client.Status().Update(ctx, updated); apierrors.IsConflict(err) {
		return domain.NewError(
			domain.ErrorConflict,
			"update session",
			"session status changed by another process",
		)
	} else if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"update session",
			fmt.Sprintf("update %s status: %v", resource.kind, err),
			err,
		)
	}

	session.ResourceVersion = updated.GetResourceVersion()
	session.Generation = updated.GetGeneration()
	session.Backend = SessionBackendCRD
	session.BackendUID = updated.GetUID()

	session.BackendResource = resource.kind

	return nil
}

// rebindWorkflowResource handles an explicit workflow hand-off such as
// reserve -> copy. Operation-specific CRDs cannot change Kind in place, so a
// rebind creates the target resource with the current durable status before
// deleting the old resource. The caller already holds the session Lease.
func (s *CRDSessionStore) rebindWorkflowResource(
	ctx context.Context,
	session *domain.Session,
	previous crclient.Object,
	desiredKind domain.ControllerKind,
) error {
	if session == nil || previous == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"rebind session",
			"session and existing workflow are required",
		)
	}

	target, ok := sessionObjectForKind(session, desiredKind)
	if !ok {
		return domain.NewError(
			domain.ErrorValidation,
			"rebind session",
			"unsupported target workflow resource",
		)
	}

	target.SetAnnotations(maps.Clone(previous.GetAnnotations()))
	target.SetLabels(MergeSessionLabels(previous.GetLabels(), session.ID))
	target.SetOwnerReferences(
		append([]metav1.OwnerReference(nil), previous.GetOwnerReferences()...),
	)

	if err := s.client.Create(ctx, target); err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rebind session",
			"create "+string(desiredKind),
			err,
		)
	}

	session.Status.ObservedGeneration = target.GetGeneration()

	status := session.Status
	if !setWorkflowStatus(target, session.Spec, status) {
		failure := domain.NewError(
			domain.ErrorInternal,
			"rebind session",
			"unsupported workflow resource",
		)

		return errors.Join(failure, s.rollbackRebindTarget(ctx, target))
	}

	if err := s.client.Status().Update(ctx, target); err != nil {
		failure := domain.WrapError(
			domain.ErrorKubernetes,
			"rebind session",
			"initialize "+string(desiredKind)+" status",
			err,
		)

		return errors.Join(failure, s.rollbackRebindTarget(ctx, target))
	}

	target.SetFinalizers(ensureSessionFinalizer(target.GetFinalizers()))

	if err := s.client.Update(ctx, target); err != nil {
		failure := domain.WrapError(
			domain.ErrorKubernetes,
			"rebind session",
			"protect "+string(desiredKind),
			err,
		)

		return errors.Join(failure, s.rollbackRebindTarget(ctx, target))
	}

	// The old operation-specific object carries the session protection
	// finalizer. Remove that finalizer before deleting the old Kind so a normal
	// rebind cannot leave two discoverable records with the same name.
	if containsString(previous.GetFinalizers(), SessionFinalizer) {
		withoutProtection, ok := previous.DeepCopyObject().(crclient.Object)
		if !ok {
			failure := domain.NewError(
				domain.ErrorInternal,
				"rebind session",
				"workflow deep copy does not implement client.Object",
			)

			return errors.Join(failure, s.rollbackRebindTarget(ctx, target))
		}

		withoutProtection.SetFinalizers(
			removeSessionFinalizer(withoutProtection.GetFinalizers()),
		)

		if err := s.client.Update(ctx, withoutProtection); err != nil {
			failure := domain.WrapError(
				domain.ErrorKubernetes,
				"rebind session",
				"remove old workflow protection",
				err,
			)

			return errors.Join(failure, s.rollbackRebindTarget(ctx, target))
		}

		previous = withoutProtection
	}

	uid := previous.GetUID()

	rv := previous.GetResourceVersion()
	if err := s.client.Delete(
		ctx,
		previous,
		crclient.Preconditions{UID: &uid, ResourceVersion: &rv},
	); err != nil {
		if apierrors.IsNotFound(err) {
			return s.finishRebindSession(session, target, desiredKind)
		}

		var failure error
		if apierrors.IsConflict(err) {
			failure = domain.NewError(
				domain.ErrorConflict,
				"rebind session",
				"old workflow changed while rebinding",
			)
		} else {
			failure = domain.WrapError(
				domain.ErrorKubernetes,
				"rebind session",
				"delete "+string(workflowKind(previous)),
				err,
			)
		}

		sourceExists, protectionErr := s.restoreRebindSourceProtection(ctx, previous)
		if !sourceExists && protectionErr == nil {
			return s.finishRebindSession(session, target, desiredKind)
		}

		return errors.Join(
			failure,
			protectionErr,
			s.rollbackRebindTarget(ctx, target),
		)
	}

	return s.finishRebindSession(session, target, desiredKind)
}

func (s *CRDSessionStore) finishRebindSession(
	session *domain.Session,
	target crclient.Object,
	desiredKind domain.ControllerKind,
) error {
	session.ResourceVersion = target.GetResourceVersion()
	session.Generation = target.GetGeneration()
	session.Backend = SessionBackendCRD
	session.BackendUID = target.GetUID()
	session.BackendResource = desiredKind

	return nil
}

func (s *CRDSessionStore) rollbackRebindTarget(
	ctx context.Context,
	target crclient.Object,
) error {
	if target == nil {
		return nil
	}

	key := crclient.ObjectKeyFromObject(target)

	var current crclient.Object

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		candidate, ok := target.DeepCopyObject().(crclient.Object)
		if !ok {
			return domain.NewError(
				domain.ErrorInternal,
				"rollback rebind target",
				"workflow deep copy does not implement client.Object",
			)
		}

		if getErr := s.client.Get(ctx, key, candidate); apierrors.IsNotFound(getErr) {
			current = nil
			return nil
		} else if getErr != nil {
			return getErr
		}

		current = candidate
		if !containsString(candidate.GetFinalizers(), SessionFinalizer) {
			return nil
		}

		candidate.SetFinalizers(removeSessionFinalizer(candidate.GetFinalizers()))

		if updateErr := s.client.Update(ctx, candidate); updateErr != nil {
			return updateErr
		}

		current = candidate

		return nil
	})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rollback rebind target",
			"remove workflow protection",
			err,
		)
	}

	if current == nil {
		return nil
	}

	if err := s.client.Delete(ctx, current); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rollback rebind target",
			"delete "+string(workflowKind(current)),
			err,
		)
	}

	return nil
}

func (s *CRDSessionStore) restoreRebindSourceProtection(
	ctx context.Context,
	previous crclient.Object,
) (bool, error) {
	key := crclient.ObjectKeyFromObject(previous)
	exists := false

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, ok := previous.DeepCopyObject().(crclient.Object)
		if !ok {
			return domain.NewError(
				domain.ErrorInternal,
				"restore rebind source",
				"workflow deep copy does not implement client.Object",
			)
		}

		if getErr := s.client.Get(ctx, key, current); apierrors.IsNotFound(getErr) {
			exists = false
			return nil
		} else if getErr != nil {
			return getErr
		}

		exists = true

		if containsString(current.GetFinalizers(), SessionFinalizer) {
			return nil
		}

		current.SetFinalizers(ensureSessionFinalizer(current.GetFinalizers()))

		return s.client.Update(ctx, current)
	})
	if err != nil {
		return exists, domain.WrapError(
			domain.ErrorKubernetes,
			"restore rebind source",
			"restore workflow protection",
			err,
		)
	}

	return exists, nil
}

func (s *CRDSessionStore) List(ctx context.Context, namespace string) ([]*domain.Session, error) {
	if err := s.configured("list sessions"); err != nil {
		return nil, err
	}

	resources := s.resources()
	lists := make([]crclient.ObjectList, len(resources))
	errors := make([]error, len(resources))
	parallel.For(len(resources), func(index int) {
		resource := resources[index]
		items := resource.newList()
		lists[index] = items

		var err error
		if resource.cluster {
			err = s.client.List(ctx, items)
		} else {
			err = s.client.List(ctx, items, crclient.InNamespace(namespace))
		}

		if err != nil {
			if apierrors.IsForbidden(err) {
				// Namespaced tenant RoleBindings may intentionally omit other
				// operation Kinds. An empty result for an inaccessible Kind is
				// equivalent to no visible objects for this caller.
				return
			}

			errors[index] = domain.WrapError(
				domain.ErrorKubernetes,
				"list sessions",
				"list "+string(resource.kind),
				err,
			)
		}
	})

	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	// Different workflow Kinds are independent Kubernetes resources and may
	// legitimately share a name. Preserve every object here; type-aware reads
	// and controllers disambiguate them by Kind.
	result := make([]*domain.Session, 0)

	for index, resource := range resources {
		for _, object := range workflowListItems(lists[index]) {
			session, decodeErr := DecodeWorkflow(object)
			if decodeErr != nil {
				return nil, decodeErr
			}

			if resource.cluster && namespace != "" && session.Spec.SessionNamespace != namespace {
				continue
			}

			result = append(result, session)
		}
	}

	return result, nil
}

func (s *CRDSessionStore) Delete(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "delete session", "session is nil")
	}

	if err := s.configured("delete session"); err != nil {
		return err
	}

	resourceKind := session.BackendResource
	if resourceKind == "" {
		resourceKind = workflowCRDKindForSession(session)
	}

	resource, ok := workflowCRDResource(resourceKind)
	if !ok {
		return domain.NewError(
			domain.ErrorValidation,
			"delete session",
			"unsupported workflow resource",
		)
	}

	object := resource.new()
	if err := s.client.Get(
		ctx,
		resourceKey(resource, session.Spec.SessionNamespace, session.ID),
		object,
	); apierrors.IsNotFound(
		err,
	) {
		return nil
	} else if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"delete session",
			"read "+string(resource.kind),
			err,
		)
	}

	if err := ValidateWorkflowMetadata(
		object,
		session.ID,
		resourceNamespace(resource, session.Spec.SessionNamespace),
	); err != nil {
		return domain.WrapError(
			domain.ErrorConflict,
			"delete session",
			string(resource.kind)+" ownership does not match the session",
			err,
		)
	}

	if session.ResourceVersion != "" && object.GetResourceVersion() != session.ResourceVersion {
		return domain.NewError(
			domain.ErrorConflict,
			"delete session",
			"session "+string(resource.kind)+" changed after it was loaded",
		)
	}

	if containsString(object.GetFinalizers(), SessionFinalizer) {
		updated, ok := object.DeepCopyObject().(crclient.Object)
		if !ok {
			return domain.NewError(
				domain.ErrorInternal,
				"delete session",
				"workflow deep copy does not implement client.Object",
			)
		}

		updated.SetFinalizers(removeSessionFinalizer(updated.GetFinalizers()))

		if err := s.client.Update(ctx, updated); apierrors.IsConflict(err) {
			return domain.NewError(
				domain.ErrorConflict,
				"delete session",
				"session "+string(resource.kind)+" changed while removing protection finalizer",
			)
		} else if apierrors.IsNotFound(err) {
			return nil
		} else if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"delete session",
				"remove session protection finalizer",
				err,
			)
		}

		object = updated
	}

	uid := object.GetUID()
	rv := object.GetResourceVersion()

	err := s.client.Delete(
		ctx,
		object,
		crclient.Preconditions{UID: &uid, ResourceVersion: &rv},
	)
	if apierrors.IsNotFound(err) {
		return nil
	}

	if apierrors.IsConflict(err) {
		return domain.NewError(
			domain.ErrorConflict,
			"delete session",
			"session "+string(resource.kind)+" changed while deleting",
		)
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"delete session",
			"delete "+string(resource.kind),
			err,
		)
	}

	return nil
}

// DecodeWorkflow converts any operation-specific API object into the shared
// domain session envelope. The resource Kind is the operation discriminator;
// the API specs intentionally have no redundant type field.
func DecodeWorkflow(object crclient.Object) (*domain.Session, error) {
	if object == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"decode session",
			"workflow resource is nil",
		)
	}

	spec, status, ok := workflowSpecStatus(object)
	if !ok {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"decode session",
			"unsupported workflow resource",
		)
	}

	kind := workflowKind(object)
	clusterScope := domain.IsClusterControllerKind(kind)

	metadataNamespace := object.GetNamespace()
	if clusterScope {
		metadataNamespace = ""
	}

	if err := ValidateWorkflowMetadata(
		object,
		object.GetName(),
		metadataNamespace,
	); err != nil {
		return nil, err
	}

	// A declarative resource may omit status. Derive the same Planned
	// checkpoint used by the CLI and let the controller persist it first.
	session := domain.NewSession(object.GetName(), spec, time.Now())
	session.Generation = object.GetGeneration()
	session.ResourceVersion = object.GetResourceVersion()
	session.Backend = SessionBackendCRD

	session.BackendResource = kind
	session.BackendUID = object.GetUID()

	session.Deleting = object.GetDeletionTimestamp() != nil
	if status.Phase != "" {
		session.Status = status
	}

	if !clusterScope && session.Spec.SessionNamespace != object.GetNamespace() {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"decode session",
			string(workflowKind(object))+" spec.sessionNamespace must match metadata.namespace",
		)
	}

	if err := session.Validate(); err != nil {
		return nil, err
	}

	return session, nil
}

func ValidateWorkflowMetadata(object crclient.Object, id, namespace string) error {
	if object == nil || object.GetName() != id || object.GetNamespace() != namespace {
		return domain.NewError(
			domain.ErrorConflict,
			"decode session",
			fmt.Sprintf(
				"%s %s/%s ownership metadata does not match session %q",
				workflowKind(object),
				namespace,
				id,
				id,
			),
		)
	}

	// Labels are added by the CLI adapter for routing and inventory. They are
	// optional for users who author a Migration declaratively, while a present
	// value must still agree with the object identity.
	labels := object.GetLabels()
	if value, exists := labels[ManagedByLabel]; exists && value != ManagedByValue {
		return domain.NewError(
			domain.ErrorConflict,
			"decode session",
			fmt.Sprintf(
				"%s %s/%s ownership metadata does not match session %q",
				workflowKind(object),
				namespace,
				id,
				id,
			),
		)
	}

	if value, exists := labels[SessionKey]; exists && value != id {
		return domain.NewError(
			domain.ErrorConflict,
			"decode session",
			fmt.Sprintf(
				"%s %s/%s ownership metadata does not match session %q",
				workflowKind(object),
				namespace,
				id,
				id,
			),
		)
	}

	return nil
}

func sessionLabels(id string) map[string]string {
	return map[string]string{ManagedByLabel: ManagedByValue, SessionKey: id}
}

func sessionObjectFor(session *domain.Session) crclient.Object {
	if session == nil {
		return nil
	}

	kind := workflowCRDKindForSession(session)

	object, ok := sessionObjectForKind(session, kind)
	if !ok {
		return nil
	}

	return object
}

func sessionObjectForKind(
	session *domain.Session,
	kind domain.ControllerKind,
) (crclient.Object, bool) {
	if session == nil {
		return nil, false
	}

	meta := metav1.ObjectMeta{
		Name:       session.ID,
		Labels:     sessionLabels(session.ID),
		Finalizers: []string{SessionFinalizer},
	}
	if resource, found := workflowCRDResource(kind); found && !resource.cluster {
		meta.Namespace = session.Spec.SessionNamespace
	}

	typeMeta := metav1.TypeMeta{
		APIVersion: v1alpha1.GroupVersion.String(),
		Kind:       string(kind),
	}
	switch kind {
	case domain.ControllerKindMigration:
		return &v1alpha1.Migration{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.MigrationSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindClusterMigration:
		return &v1alpha1.ClusterMigration{
			TypeMeta: typeMeta, ObjectMeta: meta,
			Spec: v1alpha1.ClusterMigrationSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindPodMigration:
		return &v1alpha1.PodMigration{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.PodMigrationSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindClusterPodMigration:
		return &v1alpha1.ClusterPodMigration{
			TypeMeta: typeMeta, ObjectMeta: meta,
			Spec: v1alpha1.ClusterPodMigrationSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindReservation:
		return &v1alpha1.Reservation{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.ReservationSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindClusterReservation:
		return &v1alpha1.ClusterReservation{
			TypeMeta: typeMeta, ObjectMeta: meta,
			Spec: v1alpha1.ClusterReservationSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindCopy:
		return &v1alpha1.Copy{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.CopySpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindClusterCopy:
		return &v1alpha1.ClusterCopy{
			TypeMeta: typeMeta, ObjectMeta: meta,
			Spec: v1alpha1.ClusterCopySpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindBackup:
		return &v1alpha1.Backup{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.BackupSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindRestore:
		return &v1alpha1.Restore{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.RestoreSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindRename:
		return &v1alpha1.Rename{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.RenameSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindClusterMove:
		return &v1alpha1.ClusterMove{
			TypeMeta: typeMeta, ObjectMeta: meta,
			Spec: v1alpha1.ClusterMoveSpecFromDomain(session.Spec),
		}, true
	default:
		return nil, false
	}
}

func newWorkflowObject(kind domain.ControllerKind) crclient.Object {
	resource, ok := workflowCRDResource(kind)
	if !ok {
		return nil
	}

	return resource.new()
}

func workflowKind(object crclient.Object) domain.ControllerKind {
	if object == nil {
		return "Workflow"
	}

	switch object.(type) {
	case *v1alpha1.Migration:
		return domain.ControllerKindMigration
	case *v1alpha1.ClusterMigration:
		return domain.ControllerKindClusterMigration
	case *v1alpha1.PodMigration:
		return domain.ControllerKindPodMigration
	case *v1alpha1.ClusterPodMigration:
		return domain.ControllerKindClusterPodMigration
	case *v1alpha1.Reservation:
		return domain.ControllerKindReservation
	case *v1alpha1.ClusterReservation:
		return domain.ControllerKindClusterReservation
	case *v1alpha1.Copy:
		return domain.ControllerKindCopy
	case *v1alpha1.ClusterCopy:
		return domain.ControllerKindClusterCopy
	case *v1alpha1.Backup:
		return domain.ControllerKindBackup
	case *v1alpha1.Restore:
		return domain.ControllerKindRestore
	case *v1alpha1.Rename:
		return domain.ControllerKindRename
	case *v1alpha1.ClusterMove:
		return domain.ControllerKindClusterMove
	default:
		return domain.ControllerKind(object.GetObjectKind().GroupVersionKind().Kind)
	}
}

func workflowSpecStatus(object crclient.Object) (domain.SessionSpec, domain.SessionStatus, bool) {
	if object == nil {
		return domain.SessionSpec{}, domain.SessionStatus{}, false
	}

	switch typed := object.(type) {
	case *v1alpha1.Migration:
		spec := typed.Spec.Domain(typed.Namespace)
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(typed.Namespace), true
	case *v1alpha1.ClusterMigration:
		spec := typed.Spec.Domain()
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(), true
	case *v1alpha1.PodMigration:
		spec := typed.Spec.Domain(typed.Namespace)
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(typed.Namespace), true
	case *v1alpha1.ClusterPodMigration:
		spec := typed.Spec.Domain()
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(), true
	case *v1alpha1.Reservation:
		spec := typed.Spec.Domain(typed.Namespace)
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(typed.Namespace), true
	case *v1alpha1.ClusterReservation:
		spec := typed.Spec.Domain()
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(), true
	case *v1alpha1.Copy:
		spec := typed.Spec.Domain(typed.Namespace)
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(typed.Namespace), true
	case *v1alpha1.ClusterCopy:
		spec := typed.Spec.Domain()
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(), true
	case *v1alpha1.Backup:
		return typed.Spec.Domain(typed.Namespace), typed.Status.Domain(), true
	case *v1alpha1.Restore:
		return typed.Spec.Domain(typed.Namespace), typed.Status.Domain(), true
	case *v1alpha1.Rename:
		return typed.Spec.Domain(typed.Namespace), typed.Status.Domain(typed.Namespace), true
	case *v1alpha1.ClusterMove:
		return typed.Spec.Domain(), typed.Status.Domain(), true
	default:
		return domain.SessionSpec{}, domain.SessionStatus{}, false
	}
}

func setWorkflowSpec(object crclient.Object, spec domain.SessionSpec) bool {
	if object == nil {
		return false
	}

	switch typed := object.(type) {
	case *v1alpha1.Migration:
		typed.Spec = v1alpha1.MigrationSpecFromDomain(spec)
	case *v1alpha1.ClusterMigration:
		typed.Spec = v1alpha1.ClusterMigrationSpecFromDomain(spec)
	case *v1alpha1.PodMigration:
		pod := typed.Spec.Workload.Pod
		affectedPods := append(
			[]v1alpha1.LocalResourceReference(nil),
			typed.Spec.Workload.AffectedPods...,
		)
		typed.Spec = v1alpha1.PodMigrationSpecFromDomain(spec)
		typed.Spec.Workload.Pod = pod
		typed.Spec.Workload.AffectedPods = affectedPods
	case *v1alpha1.ClusterPodMigration:
		pod := typed.Spec.Workload.Pod
		affectedPods := append(
			[]v1alpha1.LocalResourceReference(nil),
			typed.Spec.Workload.AffectedPods...,
		)
		typed.Spec = v1alpha1.ClusterPodMigrationSpecFromDomain(spec)
		typed.Spec.Workload.Pod = pod
		typed.Spec.Workload.AffectedPods = affectedPods
	case *v1alpha1.Reservation:
		typed.Spec = v1alpha1.ReservationSpecFromDomain(spec)
	case *v1alpha1.ClusterReservation:
		typed.Spec = v1alpha1.ClusterReservationSpecFromDomain(spec)
	case *v1alpha1.Copy:
		typed.Spec = v1alpha1.CopySpecFromDomain(spec)
	case *v1alpha1.ClusterCopy:
		typed.Spec = v1alpha1.ClusterCopySpecFromDomain(spec)
	case *v1alpha1.Backup:
		typed.Spec = v1alpha1.BackupSpecFromDomain(spec)
	case *v1alpha1.Restore:
		typed.Spec = v1alpha1.RestoreSpecFromDomain(spec)
	case *v1alpha1.Rename:
		typed.Spec = v1alpha1.RenameSpecFromDomain(spec)
	case *v1alpha1.ClusterMove:
		typed.Spec = v1alpha1.ClusterMoveSpecFromDomain(spec)
	default:
		return false
	}

	return true
}

func setWorkflowStatus(
	object crclient.Object,
	spec domain.SessionSpec,
	status domain.SessionStatus,
) bool {
	if object == nil {
		return false
	}

	switch typed := object.(type) {
	case *v1alpha1.Migration:
		typed.Status = v1alpha1.MigrationStatusFromDomain(status, spec.Volumes)
	case *v1alpha1.ClusterMigration:
		typed.Status = v1alpha1.ClusterMigrationStatusFromDomain(status, spec.Volumes)
	case *v1alpha1.PodMigration:
		typed.Status = v1alpha1.PodMigrationStatusFromDomain(status, spec)
	case *v1alpha1.ClusterPodMigration:
		typed.Status = v1alpha1.ClusterPodMigrationStatusFromDomain(status, spec)
	case *v1alpha1.Reservation:
		typed.Status = v1alpha1.ReservationStatusFromDomain(status, spec.Volumes)
	case *v1alpha1.ClusterReservation:
		typed.Status = v1alpha1.ClusterReservationStatusFromDomain(status, spec.Volumes)
	case *v1alpha1.Copy:
		typed.Status = v1alpha1.CopyStatusFromDomain(status, spec.Volumes)
	case *v1alpha1.ClusterCopy:
		typed.Status = v1alpha1.ClusterCopyStatusFromDomain(status, spec.Volumes)
	case *v1alpha1.Backup:
		typed.Status = v1alpha1.BackupStatusFromDomain(status)
	case *v1alpha1.Restore:
		typed.Status = v1alpha1.RestoreStatusFromDomain(status)
	case *v1alpha1.Rename:
		typed.Status = v1alpha1.RenameStatusFromDomain(status)
	case *v1alpha1.ClusterMove:
		typed.Status = v1alpha1.ClusterMoveStatusFromDomain(status)
	default:
		return false
	}

	return true
}

func workflowListItems(list crclient.ObjectList) []crclient.Object {
	if list == nil {
		return nil
	}

	items := make([]crclient.Object, 0)
	switch typed := list.(type) {
	case *v1alpha1.MigrationList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.ClusterMigrationList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.PodMigrationList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.ClusterPodMigrationList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.ReservationList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.ClusterReservationList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.CopyList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.ClusterCopyList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.BackupList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.RestoreList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.RenameList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.ClusterMoveList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	}

	return items
}

var (
	_ crclient.Object     = (*v1alpha1.Migration)(nil)
	_ crclient.ObjectList = (*v1alpha1.MigrationList)(nil)
)
