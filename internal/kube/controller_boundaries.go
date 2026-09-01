package kube

import (
	"errors"
	"strconv"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
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

	resource, ok := domain.ControllerResourceForSession(session)
	if !ok {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller namespace",
			"no controller workflow API supports this operation and namespace scope",
		)
	}

	return controllerNamespaceBoundaryErrorForResource(session, resource)
}

func controllerNamespaceBoundaryErrorForResource(
	session *domain.Session,
	resource domain.ControllerResource,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "controller namespace", "session is nil")
	}

	if resource.Type != session.Spec.Type {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller namespace",
			"workflow resource does not match the session operation",
		)
	}

	namespace := session.Spec.SessionNamespace
	if namespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"controller namespace",
			"session namespace is required",
		)
	}

	if resource.Cluster {
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
// selecting an arbitrary dynamic API resource. The controller path accepts
// only the GVKs implemented by its workload adapters and rejects unknown
// versions before any dynamic client call is made.
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
	resource, ok := domain.ControllerResourceForSession(session)
	if !ok {
		return false
	}

	return controllerSessionSupportedForResource(session, resource)
}

func controllerSessionSupportedForResource(
	session *domain.Session,
	resource domain.ControllerResource,
) bool {
	if session == nil || session.Spec.SourceNamespace == "" ||
		session.Spec.SessionNamespace == "" ||
		resource.Type != session.Spec.Type {
		return false
	}

	if controllerNamespaceBoundaryErrorForResource(session, resource) != nil {
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
