package app

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// volumeReserver provisions destination claims. Operation executors keep the
// concrete CRD checkpoint; only the reservation actuation is delegated.
type volumeReserver interface {
	ReserveVolume(
		ctx context.Context,
		request kube.ReservationRequest,
		sourcePVC, sourcePV v1alpha1.ObjectReference,
		sourceCapacity string,
		pvc *corev1.PersistentVolumeClaim,
		checkpoint *v1alpha1.ClusterVolumeReservationStatus,
	) error
	ValidateVolumeReservation(
		ctx context.Context,
		request kube.ReservationRequest,
		sourcePVC, sourcePV v1alpha1.ObjectReference,
		sourceCapacity string,
		pvc *corev1.PersistentVolumeClaim,
		checkpoint v1alpha1.ClusterVolumeReservationStatus,
	) error
}

// volumeSwitcher performs the cutover and rollback actuation for one volume.
type volumeSwitcher interface {
	VerifyActivationRecovery(
		ctx context.Context,
		sessionID string,
		volumes []kube.PVCTransferBindings,
	) error
	VerifyVolumeOffline(ctx context.Context, volume kube.PVCTransferBindings) error
	VerifyVolumesOfflineForSession(
		ctx context.Context,
		sessionID string,
		volumes []kube.PVCTransferBindings,
	) error
	ActivatePVC(
		ctx context.Context,
		sessionID string,
		volume kube.PVCTransferBindings,
		desired *corev1.PersistentVolumeClaim,
		status *v1alpha1.ClusterVolumeActivationStatus,
		progress kube.ProgressFunc,
	) error
	RollbackPVC(
		ctx context.Context,
		sessionID string,
		volume kube.PVCTransferBindings,
		desired *corev1.PersistentVolumeClaim,
		status *v1alpha1.ClusterVolumeActivationStatus,
		progress kube.ProgressFunc,
	) error
}

type sessionLockContextKey struct{}

type heldSessionLock struct {
	lock      kube.SessionLock
	namespace string
	id        string
}

func withHeldSessionLock(ctx context.Context, held heldSessionLock) context.Context {
	return context.WithValue(kube.WithLeaseFence(ctx, held.lock), sessionLockContextKey{}, held)
}

// workflowDeletionContextKey marks a context whose execution runs on behalf of
// deletion convergence, which may re-enter workflows already being deleted.
type workflowDeletionContextKey struct{}

func deletionRequiresConvergence(phase v1alpha1.WorkflowPhase) bool {
	switch phase {
	case domain.PhaseActivating,
		domain.PhaseActivated,
		domain.PhaseResuming,
		domain.PhaseRenaming,
		domain.PhaseMoving,
		domain.PhaseRollingBack:
		return true
	default:
		return false
	}
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func failureReason(err error) string {
	if isDestinationNoSpaceError(err) {
		return domain.FailureDestinationCapacityExhausted
	}
	return ""
}

func invalidWorkflowResumePhase(phase v1alpha1.WorkflowPhase, operation domain.Operation) error {
	return domain.NewError(
		domain.ErrorPrecondition,
		"resume "+string(operation),
		fmt.Sprintf("phase %s cannot be resumed for operation %s", phase, operation),
	)
}

func warmCopyProbeError(
	operation domain.Operation,
	targets []kube.ToolProbeTarget,
	err error,
) error {
	if err == nil || !kube.IsConcurrentMountFailureMessage(err.Error()) {
		return err
	}

	pvcs := make([]string, 0, len(targets))
	for _, target := range targets {
		if target.PVCName == "" || target.SkipPVCMount {
			continue
		}

		ref := target.Namespace + "/" + target.PVCName
		if !slices.Contains(pvcs, ref) {
			pvcs = append(pvcs, ref)
		}
	}

	if len(pvcs) == 0 {
		return err
	}

	sort.Strings(pvcs)

	recovery := "disable warm copy after making sure the source PVC has no active Pod consumers"
	switch operation {
	case domain.OperationCopy:
		recovery = "rerun the copy without --online after the source PVC has no active Pod consumers"
	case domain.OperationMigratePod:
		recovery = "rerun the migration with --precopy-passes 0"
	}

	return domain.WrapError(
		domain.ErrorPrecondition,
		domain.ErrorOperationWarmCopyMountProbe,
		fmt.Sprintf(
			"second-Pod mount failed for source PVC(s) %s while the source workload is active: %v; abort this pre-cutover session, clean its retained resources, and %s",
			strings.Join(pvcs, ","),
			err,
			recovery,
		),
		err,
	)
}

func validateMigrationPVCAdmission(
	ctx context.Context,
	client kubernetes.Interface,
	groups map[string][]kube.PVCAdmissionChange,
) error {
	namespaces := make([]string, 0, len(groups))
	for namespace := range groups {
		namespaces = append(namespaces, namespace)
	}

	sort.Strings(namespaces)

	type policyResult struct {
		err error
	}

	results := make([]policyResult, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		namespace := namespaces[index]

		report, err := kube.CheckPVCAdmissionPolicies(ctx, client, groups[namespace])
		if err != nil {
			results[index].err = domain.WrapError(
				domain.ErrorKubernetes,
				activationPreflightPhase,
				"check application PVC admission in "+namespace,
				err,
			)

			return
		}

		if len(report.QuotaViolations) > 0 {
			results[index].err = domain.NewError(
				domain.ErrorPrecondition,
				activationPreflightPhase,
				"application PVC quota rejected the replacement: "+strings.Join(
					report.QuotaViolations,
					"; ",
				),
			)

			return
		}

		if len(report.LimitRangeViolations) > 0 {
			results[index].err = domain.NewError(
				domain.ErrorPrecondition,
				activationPreflightPhase,
				"application PVC LimitRange rejected the replacement: "+strings.Join(
					report.LimitRangeViolations,
					"; ",
				),
			)
		}
	})

	for _, result := range results {
		if result.err != nil {
			return result.err
		}
	}

	return nil
}

func resolveVolumeConsumerNode(
	source v1alpha1.ObjectReference,
	pods []corev1.Pod,
) (string, error) {
	nodes := map[string]struct{}{}
	for index := range pods {
		pod := &pods[index]
		if kube.ActivePodUsesPVC(pod, source.Name) && pod.Spec.NodeName != "" {
			nodes[pod.Spec.NodeName] = struct{}{}
		}
	}

	if len(nodes) > 1 {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"PVC %s/%s active consumers span multiple nodes",
				source.Namespace,
				source.Name,
			),
		)
	}

	for node := range nodes {
		return node, nil
	}

	return "", nil
}

func reservationToolProbeTargets(namespaces []string, targetNode string) []kube.ToolProbeTarget {
	if targetNode == "" {
		return nil
	}

	return toolProbeTargetsForNamespaces(
		namespaces,
		targetNode,
		nil,
	)
}

func sessionNeedsSourceSSHD(strategies []string) bool {
	if len(strategies) == 0 {
		return true
	}

	for _, strategy := range strategies {
		if strategy != domain.StrategyMount {
			return true
		}
	}

	return false
}

func toolProbeTargetsForNamespaces(
	namespaces []string,
	nodeName string,
	components []string,
) []kube.ToolProbeTarget {
	targets := make([]kube.ToolProbeTarget, 0, len(namespaces))
	for _, namespace := range namespaces {
		targets = append(
			targets,
			kube.ToolProbeTarget{
				Namespace:  namespace,
				NodeName:   nodeName,
				Components: slices.Clone(components),
			},
		)
	}

	return targets
}

func reservationManifest(
	destination v1alpha1.ObjectReference,
	requestedCapacity, storageClass string,
	mode corev1.PersistentVolumeMode,
	accessModes []corev1.PersistentVolumeAccessMode,
) (*corev1.PersistentVolumeClaim, error) {
	capacity, err := resource.ParseQuantity(requestedCapacity)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorValidation,
			"reserve volume",
			"parse capacity",
			err,
		)
	}

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      destination.Name,
			Namespace: destination.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClass,
			VolumeMode:       &mode,
			AccessModes:      slices.Clone(accessModes),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: capacity},
			},
		},
	}, nil
}

func validateFinalizablePV(
	pv *corev1.PersistentVolume,
	ref v1alpha1.ObjectReference,
	sessionID string,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", ref.Name),
		)
	}

	if pv.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	role := pv.Labels[kube.ResourceRoleLabel]
	if pv.Labels[kube.SessionKey] == "" && role == "" &&
		pv.Spec.PersistentVolumeReclaimPolicy == policy &&
		pv.Annotations[kube.OriginalPolicyAnnotation] == "" {
		return nil
	}

	if pv.Labels[kube.SessionKey] != sessionID ||
		(role != kube.ResourceRoleActive && role != kube.ResourceRoleSource && role != kube.ResourceRoleRename && role != kube.ResourceRoleDestination && role != kube.ResourceRoleRollback) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	return nil
}

// CleanupPodBlockerError reports a Pod that still consumes a volume cleanup
// intends to reclaim. It is a durable business blocker, not a retryable fault.
type CleanupPodBlockerError struct {
	PVCNamespace  string
	PVCName       string
	PodNamespace  string
	PodName       string
	PodPhase      corev1.PodPhase
	OwnerKind     string
	OwnerName     string
	OwnerVerified bool
	SessionOwned  bool
	Terminal      bool
	Cause         error
}

func (e *CleanupPodBlockerError) Error() string {
	if e == nil {
		return "cleanup is blocked by a Pod"
	}

	if e.Cause != nil {
		return e.Cause.Error()
	}

	return fmt.Sprintf("cleanup is blocked by Pod %s/%s", e.PodNamespace, e.PodName)
}

func (e *CleanupPodBlockerError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
