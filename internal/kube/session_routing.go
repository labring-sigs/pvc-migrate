package kube

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	SessionBackendConfigMap = "configmap"
	SessionBackendCRD       = "crd"
)

// ControllerNamespaceBoundaryError enforces the tenant boundary for a
// namespaced workflow CR. A CR's metadata namespace is its tenant identity;
// every namespaced object touched by the controller must live there. This
// makes the controller's cluster-wide watch compatible with namespaced RBAC
// while cluster-scoped workflow kinds provide the privileged cross-namespace
// path.
func ControllerNamespaceBoundaryError(session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "controller namespace", "session is nil")
	}

	namespace := session.Spec.SessionNamespace
	if namespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"controller namespace",
			"session namespace is required",
		)
	}

	clusterScope := domain.IsClusterControllerKind(session.BackendResource)
	if clusterScope {
		return validateClusterWorkflowReferences(session)
	}

	if session.Spec.SourceNamespace != namespace {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller namespace",
			"source namespace must match a namespaced workflow; use the cluster-scoped workflow kind for cross-namespace work",
		)
	}

	if session.Spec.DestinationNamespace != "" && session.Spec.DestinationNamespace != namespace {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller namespace",
			"destination namespace must match a namespaced workflow; use the cluster-scoped workflow kind for cross-namespace work",
		)
	}

	if session.Spec.TemporaryNamespace != "" && session.Spec.TemporaryNamespace != namespace {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller namespace",
			"temporary namespace must match a namespaced workflow; use the cluster-scoped workflow kind for cross-namespace work",
		)
	}

	if session.Spec.Type == domain.SessionTypeMove {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller namespace",
			"Move requires the cluster-scoped ClusterMove workflow",
		)
	}

	if err := controllerObjectStoreBoundaryError(session, namespace); err != nil {
		return err
	}

	if err := controllerVolumeBoundaryError(session.Spec.Volumes, namespace, false); err != nil {
		return err
	}

	if session.Spec.Type == domain.SessionTypeMigratePod {
		workload := session.Spec.Workload()
		for _, ref := range append([]domain.ObjectReference{workload.Pod, workload.Controller}, workload.AffectedPods...) {
			if ref.Name != "" && ref.Namespace != namespace {
				return domain.NewError(
					domain.ErrorPrecondition,
					"controller namespace",
					"workload references must be in the workflow namespace",
				)
			}
		}

		if err := controllerWorkloadIdentityError(workload); err != nil {
			return err
		}
	}

	return nil
}

func controllerObjectStoreBoundaryError(session *domain.Session, namespace string) error {
	if session.Spec.Type == domain.SessionTypeBackup && session.Spec.Backup != nil {
		payload := session.Spec.Backup
		if payload.SourcePVC.Namespace != namespace {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"backup source PVC must be in the workflow namespace",
			)
		}

		if payload.SourcePV.Namespace != "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"backup source PV must be cluster-scoped; omit namespace",
			)
		}

		if payload.CredentialsSecret.Name != "" || payload.CredentialsSecret.Namespace != "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"backup credentials Secret references are forbidden; use BackupRepository",
			)
		}

		if payload.BackupRepositoryNamespace != "" &&
			payload.BackupRepositoryNamespace != namespace {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"namespaced Backup workflows may reference only a repository in the workflow namespace",
			)
		}
	}

	if session.Spec.Type == domain.SessionTypeRestore && session.Spec.Restore != nil {
		payload := session.Spec.Restore
		if payload.DestinationPVC.Namespace != namespace {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"restore destination PVC must be in the workflow namespace",
			)
		}

		if payload.CredentialsSecret.Name != "" || payload.CredentialsSecret.Namespace != "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"restore credentials Secret references are forbidden; use BackupRepository",
			)
		}

		if payload.BackupRepositoryNamespace != "" &&
			payload.BackupRepositoryNamespace != namespace {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"namespaced Restore workflows may reference only a repository in the workflow namespace",
			)
		}
	}

	return nil
}

func controllerVolumeBoundaryError(
	volumes []domain.VolumeSpec,
	namespace string,
	cluster bool,
) error {
	for _, volume := range volumes {
		if cluster {
			if volume.SourcePVC.Namespace == "" || volume.DestinationPVC.Namespace == "" {
				return domain.NewError(
					domain.ErrorPrecondition,
					"controller namespace",
					"cluster workflows require namespace on every PVC reference",
				)
			}
		} else if volume.SourcePVC.Namespace != namespace || volume.DestinationPVC.Namespace != namespace {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"all PVC references must be in the workflow namespace",
			)
		}

		if volume.SourcePV.Namespace != "" || volume.DestinationPV.Namespace != "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"PV references must be cluster-scoped; omit namespace",
			)
		}

		if volume.TransferScope != nil &&
			(volume.TransferScope.SourcePath == "" || volume.TransferScope.DestinationPath == "") {
			return domain.NewError(
				domain.ErrorValidation,
				"controller namespace",
				"transfer scope paths must be non-empty",
			)
		}
	}

	return nil
}

func validateClusterWorkflowReferences(session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "controller namespace", "session is nil")
	}

	if err := controllerVolumeBoundaryError(session.Spec.Volumes, "", true); err != nil {
		return err
	}

	if session.Spec.Type == domain.SessionTypeBackup && session.Spec.Backup != nil {
		if session.Spec.Backup.BackupRepositoryNamespace == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"cluster backup requires an explicit BackupRepository namespace",
			)
		}

		if session.Spec.Backup.SourcePVC.Namespace == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"cluster backup requires a namespace on source PVC",
			)
		}

		if session.Spec.Backup.SourcePV.Namespace != "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller namespace",
				"PV references must be cluster-scoped; omit namespace",
			)
		}
	}

	if session.Spec.Type == domain.SessionTypeRestore && session.Spec.Restore != nil &&
		session.Spec.Restore.DestinationPVC.Namespace == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller namespace",
			"cluster restore requires a namespace on destination PVC",
		)
	}

	if session.Spec.Type == domain.SessionTypeRestore && session.Spec.Restore != nil &&
		session.Spec.Restore.BackupRepositoryNamespace == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller namespace",
			"cluster restore requires an explicit BackupRepository namespace",
		)
	}

	if session.Spec.Type == domain.SessionTypeMigratePod {
		workload := session.Spec.Workload()
		for _, ref := range append([]domain.ObjectReference{workload.Pod, workload.Controller}, workload.AffectedPods...) {
			if ref.Name != "" && ref.Namespace == "" {
				return domain.NewError(
					domain.ErrorPrecondition,
					"controller namespace",
					"cluster workload references require a namespace",
				)
			}
		}

		if err := controllerWorkloadIdentityError(workload); err != nil {
			return err
		}
	}

	return nil
}

// controllerWorkloadIdentityError prevents a tenant-controlled CR from
// selecting an arbitrary dynamic API resource. Session mode keeps the
// historical discovery behavior; the controller path accepts only the GVKs
// implemented by its workload adapters and rejects unknown versions before
// any dynamic client call is made.
func controllerWorkloadIdentityError(workload domain.WorkloadSpec) error {
	if workload.Adapter == domain.WorkloadNone {
		return nil
	}

	if err := validateControllerWorkloadReference(
		workload.Pod,
		"workload Pod",
		[]controllerWorkloadGVK{{domain.CoreAPIVersion, domain.KindPod}},
	); err != nil {
		return err
	}

	for index, ref := range workload.AffectedPods {
		if err := validateControllerWorkloadReference(
			ref,
			"affected Pod "+strconv.Itoa(index),
			[]controllerWorkloadGVK{{domain.CoreAPIVersion, domain.KindPod}},
		); err != nil {
			return err
		}
	}

	switch workload.Adapter {
	case domain.WorkloadStandalone:
		if hasControllerReference(workload.Controller) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller workload",
				"StandalonePod workflows cannot include a controller reference",
			)
		}
	case domain.WorkloadDeployment:
		if err := validateControllerWorkloadReference(
			workload.Controller,
			"workload controller",
			[]controllerWorkloadGVK{{domain.AppsAPIVersion, domain.KindDeployment}},
		); err != nil {
			return err
		}
	case domain.WorkloadGrafana:
		if err := validateControllerWorkloadReference(
			workload.Controller,
			"workload controller",
			[]controllerWorkloadGVK{{domain.AppsAPIVersion, domain.KindDeployment}},
		); err != nil {
			return err
		}

		if workload.Grafana == nil || workload.Grafana.APIVersion != domain.GrafanaAPIVersion {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller workload",
				"Grafana apiVersion is not supported by controller",
			)
		}
	case domain.WorkloadStatefulSet, domain.WorkloadVictoriaLogs:
		if err := validateControllerWorkloadReference(
			workload.Controller,
			"workload controller",
			[]controllerWorkloadGVK{{domain.AppsAPIVersion, domain.KindStatefulSet}},
		); err != nil {
			return err
		}
	case domain.WorkloadVMCluster:
		if err := validateControllerWorkloadReference(
			workload.Controller,
			"workload controller",
			[]controllerWorkloadGVK{{domain.AppsAPIVersion, domain.KindStatefulSet}},
		); err != nil {
			return err
		}

		if workload.VMCluster == nil ||
			workload.VMCluster.APIVersion != domain.VictoriaMetricsAPIVersion {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller workload",
				"VMCluster apiVersion is not supported by controller",
			)
		}
	case domain.WorkloadKubeBlocks:
		if err := validateControllerWorkloadReference(
			workload.Controller,
			"workload controller",
			[]controllerWorkloadGVK{
				{domain.KubeBlocksWorkloadsAPIVersion, domain.KindInstanceSet},
				{domain.KubeBlocksWorkloadsLegacyAPIVersion, domain.KindInstanceSet},
				{domain.AppsAPIVersion, domain.KindStatefulSet},
			},
		); err != nil {
			return err
		}

		if workload.KubeBlocks == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller workload",
				"KubeBlocks workload state is required",
			)
		}
		// InstanceSet-backed workflows pause the InstanceSet directly. MongoDB
		// native discovery therefore has no OpsRequest version to persist.
		if workload.KubeBlocks.OpsAPIVersion == "" &&
			workload.Controller.Kind == domain.KindInstanceSet {
			return nil
		}

		if !allowedKubeBlocksAPIVersion(workload.KubeBlocks.OpsAPIVersion) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller workload",
				"KubeBlocks OpsRequest apiVersion is not supported by controller",
			)
		}
	case domain.WorkloadNone:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller workload",
			"workload adapter is not supported by controller",
		)
	}

	return nil
}

type controllerWorkloadGVK struct {
	apiVersion string
	kind       string
}

func validateControllerWorkloadReference(
	ref domain.ObjectReference,
	description string,
	allowed []controllerWorkloadGVK,
) error {
	if ref.Name == "" || ref.UID == "" || ref.APIVersion == "" || ref.Kind == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller workload",
			description+" must include a complete apiVersion/kind/name/uid identity",
		)
	}

	for _, candidate := range allowed {
		if ref.APIVersion == candidate.apiVersion && ref.Kind == candidate.kind {
			return nil
		}
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"controller workload",
		description+" apiVersion/kind is not supported by controller",
	)
}

func hasControllerReference(ref domain.ObjectReference) bool {
	return ref.APIVersion != "" || ref.Kind != "" || ref.Namespace != "" || ref.Name != "" ||
		ref.UID != "" ||
		ref.ResourceVersion != ""
}

func allowedKubeBlocksAPIVersion(apiVersion string) bool {
	switch apiVersion {
	case domain.KubeBlocksClusterAPIVersion, domain.KubeBlocksOperationsAPIVersion:
		return true
	default:
		return false
	}
}

// ControllerSessionSupported describes workflows executable by the local
// controller. Namespaced sessions stay tenant-local; cross-namespace sessions
// require the matching cluster-scoped workflow CRD.
func ControllerSessionSupported(session *domain.Session) bool {
	if session == nil || session.Spec.SourceNamespace == "" || session.Spec.SessionNamespace == "" {
		return false
	}

	clusterScope := controllerSessionClusterScope(session)
	if !clusterScope && ControllerNamespaceBoundaryError(session) != nil {
		return false
	}

	if clusterScope && domain.IsClusterControllerKind(session.BackendResource) &&
		ControllerNamespaceBoundaryError(session) != nil {
		return false
	}

	if !controllerRepositoryNamespaceSupported(session, clusterScope) {
		return false
	}

	switch session.Spec.Type {
	case domain.SessionTypeBackup:
		return session.Spec.Backup != nil && session.Spec.Backup.BackupRepository != "" &&
			session.Spec.Backup.Endpoint == "" && !session.Spec.Backup.AllowInsecureEndpoint &&
			session.Spec.Backup.CredentialsSecret.Name == "" && session.Spec.Backup.CredentialsSecret.Namespace == ""
	case domain.SessionTypeRestore:
		return session.Spec.Restore != nil && session.Spec.DestinationNamespace != "" &&
			session.Spec.Restore.BackupRepository != "" &&
			session.Spec.Restore.Endpoint == "" && !session.Spec.Restore.AllowInsecureEndpoint &&
			session.Spec.Restore.CredentialsSecret.Name == "" && session.Spec.Restore.CredentialsSecret.Namespace == ""
	case domain.SessionTypeReserve, domain.SessionTypeMigrate,
		domain.SessionTypeMigratePod, domain.SessionTypeCopy, domain.SessionTypeRename,
		domain.SessionTypeMove:
		return session.Spec.DestinationNamespace != ""
	default:
		return false
	}
}

func controllerSessionClusterScope(session *domain.Session) bool {
	return domain.IsClusterControllerKind(session.BackendResource) ||
		session.Spec.SourceNamespace != session.Spec.SessionNamespace ||
		(session.Spec.DestinationNamespace != "" && session.Spec.DestinationNamespace != session.Spec.SessionNamespace) ||
		(session.Spec.TemporaryNamespace != "" && session.Spec.TemporaryNamespace != session.Spec.SessionNamespace) ||
		backupRepositoryCrossesNamespace(session) || session.Spec.Type == domain.SessionTypeMove
}

func controllerRepositoryNamespaceSupported(session *domain.Session, clusterScope bool) bool {
	repositoryNamespace := ""

	hasRepository := false

	if session.Spec.Backup != nil {
		hasRepository = true
		repositoryNamespace = session.Spec.Backup.BackupRepositoryNamespace
	}

	if session.Spec.Restore != nil {
		hasRepository = true
		repositoryNamespace = session.Spec.Restore.BackupRepositoryNamespace
	}

	if !hasRepository {
		return true
	}

	if repositoryNamespace == "" {
		return !clusterScope
	}

	return clusterScope || repositoryNamespace == session.Spec.SessionNamespace
}

// RoutingSessionStore provides auto mode. New eligible sessions go to their
// operation-specific workflow CRD, while unsupported workflows continue using
// ConfigMaps.
type RoutingSessionStore struct {
	crd             *CRDSessionStore
	configMap       *ConfigMapSessionStore
	controllerKinds map[domain.ControllerKind]struct{}
}

var (
	_ SessionLocker       = (*RoutingSessionStore)(nil)
	_ SessionLeaseCleaner = (*RoutingSessionStore)(nil)
)

func NewSessionStoreRouter(
	configMap *ConfigMapSessionStore,
	crd *CRDSessionStore,
) *RoutingSessionStore {
	return &RoutingSessionStore{configMap: configMap, crd: crd}
}

// WithControllerKinds configures the set of operation CRDs discovered on the
// cluster. Auto mode can then use a newly installed kind immediately while
// preserving ConfigMap persistence for operations whose CRD is absent.
func (s *RoutingSessionStore) WithControllerKinds(
	kinds []domain.ControllerKind,
) *RoutingSessionStore {
	if s == nil || len(kinds) == 0 {
		return s
	}

	s.controllerKinds = make(map[domain.ControllerKind]struct{}, len(kinds))
	for _, kind := range kinds {
		s.controllerKinds[kind] = struct{}{}
	}

	return s
}

func (s *RoutingSessionStore) controllerSupports(session *domain.Session) bool {
	if s == nil || s.crd == nil || !ControllerSessionSupported(session) {
		return false
	}

	if len(s.controllerKinds) == 0 {
		return true
	}

	workflow, ok := domain.ControllerWorkflowForType(session.Spec.Type)
	if !ok {
		return false
	}

	kind := workflow.Kind
	if clusterKind := workflow.ClusterKind; clusterKind != "" &&
		workflowCRDKindForSession(session) == clusterKind {
		kind = clusterKind
	}

	_, ok = s.controllerKinds[kind]

	return ok
}

func (s *RoutingSessionStore) Create(ctx context.Context, session *domain.Session) error {
	if s.controllerSupports(session) {
		return s.crd.Create(ctx, session)
	}
	return s.configMap.Create(ctx, session)
}

func (s *RoutingSessionStore) Get(
	ctx context.Context,
	namespace, id string,
) (*domain.Session, error) {
	if s == nil {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"get session",
			"session store router is not configured",
		)
	}

	if s.crd == nil {
		if s.configMap == nil {
			return nil, domain.NewError(
				domain.ErrorKubernetes,
				"get session",
				"no session backend is configured",
			)
		}

		return s.configMap.Get(ctx, namespace, id)
	}

	if s.configMap == nil {
		return s.crd.Get(ctx, namespace, id)
	}

	// CRD and ConfigMap reads are independent. Keep the CRD result first when
	// both backends contain a record, matching the routing and List semantics.
	type getResult struct {
		session *domain.Session
		err     error
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	crdResult := make(chan getResult, 1)
	configMapResult := make(chan getResult, 1)

	go func() {
		session, err := s.crd.Get(readCtx, namespace, id)
		crdResult <- getResult{session: session, err: err}
	}()
	go func() {
		session, err := s.configMap.Get(readCtx, namespace, id)
		configMapResult <- getResult{session: session, err: err}
	}()

	var (
		crd       getResult
		configMap getResult
		gotCRD    bool
		gotConfig bool
	)
	for !gotCRD || !gotConfig {
		select {
		case crd = <-crdResult:
			gotCRD = true

			if crd.err == nil {
				return crd.session, nil
			}

			if !IsSessionNotFound(crd.err) {
				return nil, crd.err
			}
		case configMap = <-configMapResult:
			gotConfig = true
		}
	}

	return configMap.session, configMap.err
}

func (s *RoutingSessionStore) Update(ctx context.Context, session *domain.Session) error {
	if session != nil && session.Backend == SessionBackendCRD && s.crd != nil {
		return s.crd.Update(ctx, session)
	}
	return s.configMap.Update(ctx, session)
}

func (s *RoutingSessionStore) List(
	ctx context.Context,
	namespace string,
) ([]*domain.Session, error) {
	if s.crd == nil {
		return s.configMap.List(ctx, namespace)
	}

	var results [2][]*domain.Session

	var errors [2]error
	parallel.For(2, func(index int) {
		if index == 0 {
			results[index], errors[index] = s.configMap.List(ctx, namespace)
			return
		}

		results[index], errors[index] = s.crd.List(ctx, namespace)
	})

	// Keep the historical error precedence: ConfigMap failures are reported
	// before CRD failures even though both reads run concurrently.
	if errors[0] != nil {
		return nil, errors[0]
	}

	if errors[1] != nil {
		return nil, errors[1]
	}

	configSessions := results[0]
	crdSessions := results[1]

	// Get checks the CRD backend first. Preserve that same precedence when
	// listing so a session that was explicitly moved to controller storage is
	// represented once instead of appearing as two independent records.
	result := make([]*domain.Session, 0, len(configSessions)+len(crdSessions))

	byID := make(map[string]int, len(configSessions)+len(crdSessions))
	for _, session := range configSessions {
		if session == nil {
			continue
		}

		byID[session.ID] = len(result)
		result = append(result, session)
	}

	for _, session := range crdSessions {
		if session == nil {
			continue
		}

		if index, exists := byID[session.ID]; exists {
			result[index] = session
			continue
		}

		byID[session.ID] = len(result)
		result = append(result, session)
	}

	return result, nil
}

func (s *RoutingSessionStore) Delete(ctx context.Context, session *domain.Session) error {
	if session != nil && session.Backend == SessionBackendCRD && s.crd != nil {
		return s.crd.Delete(ctx, session)
	}
	return s.configMap.Delete(ctx, session)
}

// AcquireSessionLock keeps auto mode's storage routing transparent to the
// service-level fencing protocol. Both backends use the same Lease name, so
// selecting the configured Kubernetes client is sufficient even before a
// session has been persisted and acquired a backend marker.
func (s *RoutingSessionStore) AcquireSessionLock(
	ctx context.Context,
	namespace, id string,
) (SessionLock, error) {
	if s == nil {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"acquire session lock",
			"session store router is not configured",
		)
	}

	if s.crd != nil && s.crd.leaseClient != nil {
		return s.crd.AcquireSessionLock(ctx, namespace, id)
	}

	if s.configMap != nil {
		return s.configMap.AcquireSessionLock(ctx, namespace, id)
	}

	if s.crd != nil {
		return s.crd.AcquireSessionLock(ctx, namespace, id)
	}

	return nil, domain.NewError(
		domain.ErrorKubernetes,
		"acquire session lock",
		"no session backend is configured",
	)
}

func (s *RoutingSessionStore) DeleteSessionLease(
	ctx context.Context,
	namespace, id string,
) error {
	if s == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"delete session lock",
			"session store router is not configured",
		)
	}

	if s.crd != nil && s.crd.leaseClient != nil {
		return s.crd.DeleteSessionLease(ctx, namespace, id)
	}

	if s.configMap != nil {
		return s.configMap.DeleteSessionLease(ctx, namespace, id)
	}

	if s.crd != nil {
		return s.crd.DeleteSessionLease(ctx, namespace, id)
	}

	return domain.NewError(
		domain.ErrorKubernetes,
		"delete session lock",
		"no session backend is configured",
	)
}

func IsSessionNotFound(err error) bool {
	if apierrors.IsNotFound(err) {
		return true
	}

	var typed *domain.Error
	if !errors.As(err, &typed) {
		return false
	}

	return typed.Category == domain.ErrorValidation &&
		strings.Contains(strings.ToLower(typed.Message), "does not exist")
}
