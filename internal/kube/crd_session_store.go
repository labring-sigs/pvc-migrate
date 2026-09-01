package kube

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// MigrationGVR is the stable GVR for the controller-backed migration API. It
// remains exported for discovery and integration tests; CRUD operations use
// the generated typed API for the operation-specific Kind.
var MigrationGVR = schema.GroupVersionResource{
	Group:    v1alpha1.GroupVersion.Group,
	Version:  v1alpha1.GroupVersion.Version,
	Resource: domain.MigrationResource,
}

type crdResource struct {
	kind    domain.ControllerKind
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
			kind:    domain.ControllerKindPodMigration,
			new:     func() crclient.Object { return &v1alpha1.PodMigration{} },
			newList: func() crclient.ObjectList { return &v1alpha1.PodMigrationList{} },
		},
		{
			kind:    domain.ControllerKindReservation,
			new:     func() crclient.Object { return &v1alpha1.Reservation{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ReservationList{} },
		},
		{
			kind:    domain.ControllerKindCopy,
			new:     func() crclient.Object { return &v1alpha1.Copy{} },
			newList: func() crclient.ObjectList { return &v1alpha1.CopyList{} },
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
			kind:    domain.ControllerKindMove,
			new:     func() crclient.Object { return &v1alpha1.Move{} },
			newList: func() crclient.ObjectList { return &v1alpha1.MoveList{} },
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

	kind := workflowCRDKind(sessionType)
	if kind == "" {
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
			"this workflow requires an explicit cluster connection or credential resource",
		)
	}

	if !s.SupportsType(session.Spec.Type) {
		workflow, _ := domain.ControllerWorkflowForType(session.Spec.Type)

		return domain.NewError(
			domain.ErrorPrecondition,
			"create session",
			string(workflow.Kind)+" CRD is not served by this cluster",
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

	// controller-runtime reconciliation requests contain only namespace/name.
	// Keep those names unique across operation-specific CRDs so a request can
	// never resolve to an unrelated workflow kind. The probes are independent;
	// interpret their results in resource order for deterministic conflicts.
	resources := s.resources()
	errors := make([]error, len(resources))
	parallel.For(len(resources), func(index int) {
		candidate := resources[index]
		candidateObject := candidate.new()

		errors[index] = s.client.Get(
			ctx,
			crclient.ObjectKey{Namespace: session.Spec.SessionNamespace, Name: session.ID},
			candidateObject,
		)
	})

	for index, candidate := range resources {
		err := errors[index]
		if err == nil {
			return domain.NewError(
				domain.ErrorConflict,
				"create session",
				fmt.Sprintf("session %s already exists as %s", session.ID, candidate.kind),
			)
		}

		if !apierrors.IsNotFound(err) {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"create session",
				"check existing "+string(candidate.kind),
				err,
			)
		}
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
			return nil, err
		}

		latest := newWorkflowObject(workflowKind(current))
		if latest == nil {
			return nil, domain.NewError(
				domain.ErrorInternal,
				"create session",
				"unsupported workflow resource",
			)
		}

		if err := s.client.Get(
			ctx,
			crclient.ObjectKey{Namespace: session.Spec.SessionNamespace, Name: session.ID},
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
			crclient.ObjectKey{Namespace: namespace, Name: id},
			object,
		)
		if errors[index] == nil {
			objects[index] = object
		}
	})

	var found *domain.Session
	for index, resource := range resources {
		err := errors[index]
		if apierrors.IsNotFound(err) {
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

	if !ControllerSessionSupported(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"update session",
			"this workflow requires CLI/session mode",
		)
	}

	storedKind := session.BackendResource
	if storedKind == "" {
		storedKind = workflowCRDKind(session.Spec.Type)
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
		crclient.ObjectKey{Namespace: session.Spec.SessionNamespace, Name: session.ID},
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
		session.Spec.SessionNamespace,
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

	desiredKind := workflowCRDKind(session.Spec.Type)
	if desiredKind == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"update session",
			"unsupported workflow type",
		)
	}

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

	updated.SetLabels(sessionLabels(session.ID))
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

	target.SetAnnotations(previous.GetAnnotations())
	target.SetLabels(sessionLabels(session.ID))

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

		if err := s.client.List(ctx, items, crclient.InNamespace(namespace)); err != nil {
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

	result := make([]*domain.Session, 0)

	seen := make(map[string]domain.ControllerKind)
	for index, resource := range resources {
		for _, object := range workflowListItems(lists[index]) {
			session, decodeErr := DecodeWorkflow(object)
			if decodeErr != nil {
				return nil, decodeErr
			}

			if previousKind, exists := seen[session.ID]; exists {
				return nil, domain.NewError(
					domain.ErrorConflict,
					"list sessions",
					fmt.Sprintf(
						"session %s exists as both %s and %s",
						session.ID,
						previousKind,
						resource.kind,
					),
				)
			}

			seen[session.ID] = resource.kind
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
		resourceKind = workflowCRDKind(session.Spec.Type)
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
		crclient.ObjectKey{Namespace: session.Spec.SessionNamespace, Name: session.ID},
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
		session.Spec.SessionNamespace,
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

// DecodeMigration converts the generated Migration API object into the domain
// session envelope. It remains exported for callers that use the primary kind.
func DecodeMigration(object *v1alpha1.Migration) (*domain.Session, error) {
	return DecodeWorkflow(object)
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

	if err := ValidateWorkflowMetadata(
		object,
		object.GetName(),
		object.GetNamespace(),
	); err != nil {
		return nil, err
	}

	// A declarative resource may omit status. Derive the same Planned
	// checkpoint used by the CLI and let the controller persist it first.
	session := domain.NewSession(object.GetName(), spec, time.Now())
	session.Generation = object.GetGeneration()
	session.ResourceVersion = object.GetResourceVersion()
	session.Backend = SessionBackendCRD

	session.BackendResource = workflowKind(object)
	if status.Phase != "" {
		session.Status = status
	}

	if session.Spec.SessionNamespace != object.GetNamespace() {
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

// ValidateMigrationMetadata protects the identity boundary for the primary
// Migration kind. ValidateWorkflowMetadata applies the same checks to all
// operation-specific kinds.
func ValidateMigrationMetadata(object *v1alpha1.Migration, id, namespace string) error {
	return ValidateWorkflowMetadata(object, id, namespace)
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

func sessionObject(session *domain.Session) *v1alpha1.Migration {
	object, _ := sessionObjectForKind(session, domain.ControllerKindMigration)
	migration, _ := object.(*v1alpha1.Migration)
	return migration
}

func sessionObjectFor(session *domain.Session) crclient.Object {
	if session == nil {
		return nil
	}

	object, ok := sessionObjectForKind(session, workflowCRDKind(session.Spec.Type))
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
		Namespace:  session.Spec.SessionNamespace,
		Labels:     sessionLabels(session.ID),
		Finalizers: []string{SessionFinalizer},
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
	case domain.ControllerKindPodMigration:
		return &v1alpha1.PodMigration{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.PodMigrationSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindReservation:
		return &v1alpha1.Reservation{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.ReservationSpecFromDomain(session.Spec),
		}, true
	case domain.ControllerKindCopy:
		return &v1alpha1.Copy{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.CopySpecFromDomain(session.Spec),
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
	case domain.ControllerKindMove:
		return &v1alpha1.Move{
			TypeMeta:   typeMeta,
			ObjectMeta: meta,
			Spec:       v1alpha1.MoveSpecFromDomain(session.Spec),
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
	case *v1alpha1.PodMigration:
		return domain.ControllerKindPodMigration
	case *v1alpha1.Reservation:
		return domain.ControllerKindReservation
	case *v1alpha1.Copy:
		return domain.ControllerKindCopy
	case *v1alpha1.Backup:
		return domain.ControllerKindBackup
	case *v1alpha1.Restore:
		return domain.ControllerKindRestore
	case *v1alpha1.Rename:
		return domain.ControllerKindRename
	case *v1alpha1.Move:
		return domain.ControllerKindMove
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
		spec := typed.Spec.Domain()
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(), true
	case *v1alpha1.PodMigration:
		spec := typed.Spec.Domain()
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(), true
	case *v1alpha1.Reservation:
		spec := typed.Spec.Domain()
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(), true
	case *v1alpha1.Copy:
		spec := typed.Spec.Domain()
		typed.Status.ApplyToDomainSpec(&spec)
		return spec, typed.Status.Domain(), true
	case *v1alpha1.Backup:
		return typed.Spec.Domain(), typed.Status.Domain(), true
	case *v1alpha1.Restore:
		return typed.Spec.Domain(), typed.Status.Domain(), true
	case *v1alpha1.Rename:
		return typed.Spec.Domain(), typed.Status.Domain(), true
	case *v1alpha1.Move:
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
	case *v1alpha1.PodMigration:
		pod := typed.Spec.Workload.Pod
		affectedPods := append(
			[]v1alpha1.ObjectReference(nil),
			typed.Spec.Workload.AffectedPods...,
		)
		typed.Spec = v1alpha1.PodMigrationSpecFromDomain(spec)
		typed.Spec.Workload.Pod = pod
		typed.Spec.Workload.AffectedPods = affectedPods
	case *v1alpha1.Reservation:
		typed.Spec = v1alpha1.ReservationSpecFromDomain(spec)
	case *v1alpha1.Copy:
		typed.Spec = v1alpha1.CopySpecFromDomain(spec)
	case *v1alpha1.Backup:
		typed.Spec = v1alpha1.BackupSpecFromDomain(spec)
	case *v1alpha1.Restore:
		typed.Spec = v1alpha1.RestoreSpecFromDomain(spec)
	case *v1alpha1.Rename:
		typed.Spec = v1alpha1.RenameSpecFromDomain(spec)
	case *v1alpha1.Move:
		typed.Spec = v1alpha1.MoveSpecFromDomain(spec)
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
	case *v1alpha1.PodMigration:
		typed.Status = v1alpha1.PodMigrationStatusFromDomain(status, spec)
	case *v1alpha1.Reservation:
		typed.Status = v1alpha1.ReservationStatusFromDomain(status, spec.Volumes)
	case *v1alpha1.Copy:
		typed.Status = v1alpha1.CopyStatusFromDomain(status, spec.Volumes)
	case *v1alpha1.Backup:
		typed.Status = v1alpha1.BackupStatusFromDomain(status)
	case *v1alpha1.Restore:
		typed.Status = v1alpha1.RestoreStatusFromDomain(status)
	case *v1alpha1.Rename:
		typed.Status = v1alpha1.RenameStatusFromDomain(status)
	case *v1alpha1.Move:
		typed.Status = v1alpha1.MoveStatusFromDomain(status)
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
	case *v1alpha1.PodMigrationList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.ReservationList:
		for i := range typed.Items {
			items = append(items, &typed.Items[i])
		}
	case *v1alpha1.CopyList:
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
	case *v1alpha1.MoveList:
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
