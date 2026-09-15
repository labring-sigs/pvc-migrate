package app

import (
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func podMigrationPhase(phase v1alpha1.WorkflowPhase) bool {
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved,
		domain.PhaseWarmCopying, domain.PhaseWarmCopied, domain.PhasePausing, domain.PhasePaused,
		domain.PhaseFinalSyncing, domain.PhaseFinalSynced, domain.PhaseActivating,
		domain.PhaseActivated, domain.PhaseResuming, domain.PhaseCompleted,
		domain.PhaseAborting, domain.PhaseAborted, domain.PhaseRollingBack, domain.PhaseRolledBack:
		return true
	default:
		return false
	}
}

func validatePodMigrationLifecycle(status v1alpha1.WorkflowStatus) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "pod migration", message)
	}
	if status.Phase != "" && status.Phase != domain.PhaseFailed &&
		!podMigrationPhase(status.Phase) {
		return invalid("phase does not belong to pod migration")
	}

	if status.ResumeFrom != "" && !podMigrationPhase(status.ResumeFrom) {
		return invalid("resume checkpoint does not belong to pod migration")
	}

	if status.Phase == domain.PhaseFailed && status.ResumeFrom == "" {
		return invalid("failed pod migration requires a resume checkpoint")
	}

	phase := workflowResumePhase(status)
	if status.FailureReason != "" &&
		(status.FailureReason != domain.FailureDestinationCapacityExhausted ||
			(phase != domain.PhaseFinalSyncing && phase != domain.PhaseWarmCopying)) {
		return invalid("pod migration failure reason does not belong to the current stage")
	}

	return nil
}

func validateClusterPodMigrationObject(object *v1alpha1.ClusterPodMigration) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "pod migration", message)
	}
	if object == nil || object.Name == "" || object.Namespace != "" {
		return invalid("a named cluster-scoped pod migration is required")
	}

	if err := validatePodMigrationLifecycle(object.Status.WorkflowStatus); err != nil {
		return err
	}

	if err := domain.ValidateReclaimPolicies(
		object.Spec.SourcePVReclaimPolicy, object.Spec.DestinationPVCReclaimPolicy,
	); err != nil {
		return err
	}

	if object.Spec.PrecopyPasses < 0 || object.Status.WarmPassesCompleted < 0 {
		return invalid("precopy passes and completed passes cannot be negative")
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)

	plan := object.Status.Plan
	if plan == nil {
		if len(object.Status.Volumes) != 0 || object.Status.Workload != nil ||
			object.Status.WarmPassesCompleted != 0 || object.Status.OriginalPodSnapshotHash != "" ||
			len(object.Status.OpenEBSLVMSharedMounts) != 0 ||
			(phase != "" && phase != domain.PhasePlanned && phase != domain.PhaseAborted) {
			return invalid("pod migration progress requires an execution plan")
		}

		return nil
	}

	if len(plan.Volumes) == 0 || object.Status.Phase == "" {
		return invalid("pod migration plan requires volumes and a lifecycle phase")
	}

	if err := validatePodMigrationNamespaceRoles(
		object.Spec.SourceNamespace, object.Spec.TemporaryNamespace, object.Spec.SessionNamespace,
		plan.SourceNamespace, plan.TemporaryNamespace, plan.SessionNamespace,
	); err != nil {
		return err
	}

	if plan.PrecopyPasses != object.Spec.PrecopyPasses {
		return invalid("pod migration plan must preserve the requested precopy passes")
	}

	if err := domain.ValidateReclaimPolicies(
		plan.SourcePVReclaimPolicy, plan.DestinationPVCReclaimPolicy,
	); err != nil {
		return err
	}

	if err := validatePodMigrationWorkload(object.Spec.Pod, plan.Workload); err != nil {
		return err
	}

	if err := validatePodMigrationSnapshot(
		string(plan.SourceNamespace),
		plan.Workload,
		object.Status.OriginalPodSnapshotHash,
	); err != nil {
		return err
	}

	volumes, err := validateReservationVolumes(
		plan.Volumes,
		plan.SourceNamespace,
		plan.TemporaryNamespace,
	)
	if err != nil {
		return err
	}
	// Pod volume requests are overrides for discovered volumes, not the inventory.
	if err := validatePodMigrationOverrides(object.Spec.Volumes, volumes); err != nil {
		return err
	}

	if err := validatePodMigrationCheckpoints(
		volumes,
		object.Status.Volumes,
		string(plan.SourceNamespace),
		string(plan.TemporaryNamespace),
		phase,
	); err != nil {
		return err
	}

	if err := validatePodSharedMountCheckpoints(
		plan.Volumes,
		object.Status.OpenEBSLVMSharedMounts,
	); err != nil {
		return err
	}

	return validateClusterPodWorkloadCheckpoint(
		object.Status.Workload,
		string(plan.SourceNamespace),
	)
}

func validatePodMigrationNamespaceRoles(
	source, temporary, session v1alpha1.NamespaceName,
	plannedSource, plannedTemporary, plannedSession v1alpha1.NamespaceName,
) error {
	if temporary == "" {
		temporary = source
	}

	if session == "" {
		session = source
	}

	if source == "" || plannedSource != source || plannedTemporary != temporary ||
		plannedSession != session {
		return domain.NewError(
			domain.ErrorValidation,
			"pod migration",
			"pod migration plan namespace roles must match its spec",
		)
	}

	return nil
}

func validateClusterPodWorkloadCheckpoint(
	workload *v1alpha1.ClusterPodMigrationWorkloadStatus,
	namespace string,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "pod migration", message) }
	if workload != nil {
		if workload.Pod != nil {
			if err := validatePodCheckpointReference(*workload.Pod, namespace); err != nil {
				return err
			}
		}

		seen := make(map[string]bool, len(workload.AffectedPods))
		for _, pod := range workload.AffectedPods {
			if err := validatePodCheckpointReference(pod, namespace); err != nil {
				return err
			}

			if seen[pod.Name] {
				return invalid("workload checkpoint contains duplicate Pod names")
			}

			seen[pod.Name] = true
		}
	}

	return nil
}

func validatePodMigrationOverrides(
	requests []v1alpha1.VolumeRequest, volumes map[string]v1alpha1.VolumeSpec,
) error {
	seen := make(map[string]bool, len(requests))
	for _, request := range requests {
		volume, exists := volumes[request.SourcePVC.Name]
		if !exists || seen[request.SourcePVC.Name] ||
			!reservationReferenceMatches(request.SourcePVC, volume.SourcePVC) ||
			(request.SourcePV != nil && !reservationReferenceMatches(*request.SourcePV, volume.SourcePV)) ||
			(request.DestinationPVC != nil && !reservationReferenceMatches(*request.DestinationPVC, volume.DestinationPVC)) {
			return domain.NewError(domain.ErrorValidation, "pod migration",
				"pod migration plan does not satisfy the requested volume identities")
		}

		seen[request.SourcePVC.Name] = true
	}

	return nil
}

func validatePodMigrationWorkload(
	request v1alpha1.LocalResourceReference,
	workload v1alpha1.WorkloadSpec,
) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "pod migration", message)
	}
	if workload.Pod == nil || workload.Pod.Name == "" || workload.Pod.UID == "" ||
		request.Name == "" || !reservationReferenceMatches(request, *workload.Pod) {
		return invalid("planned workload must identify the requested Pod by name and UID")
	}

	if workload.OriginalObject != nil &&
		len(workload.OriginalObject.Raw) > domain.MaxOriginalPodSnapshotBytes {
		return invalid("workload original object exceeds the snapshot byte limit")
	}

	if workload.Adapter != v1alpha1.WorkloadStandalone &&
		(workload.Controller == nil || workload.Controller.Name == "" || workload.Controller.UID == "") {
		return invalid("planned workload requires its controller name and UID")
	}

	if (workload.KubeBlocks != nil && workload.Adapter != v1alpha1.WorkloadKubeBlocks) ||
		(workload.VMCluster != nil && workload.Adapter != v1alpha1.WorkloadVMCluster) ||
		(workload.Grafana != nil && workload.Adapter != v1alpha1.WorkloadGrafana) {
		return invalid("workload contains settings for an unrelated adapter")
	}

	seen := make(map[string]bool, len(workload.AffectedPods))

	selected := false
	for _, pod := range workload.AffectedPods {
		if pod.Name == "" || pod.UID == "" || seen[pod.Name] {
			return invalid("affected Pods require distinct names and nonempty UIDs")
		}

		seen[pod.Name] = true
		selected = selected || pod == *workload.Pod
	}

	return validatePodWorkloadAdapter(workload, selected)
}

func validatePodWorkloadAdapter(workload v1alpha1.WorkloadSpec, selected bool) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "pod migration", message) }
	switch workload.Adapter {
	case v1alpha1.WorkloadStandalone, v1alpha1.WorkloadStatefulSet, v1alpha1.WorkloadVictoriaLogs:
	case v1alpha1.WorkloadDeployment:
		if workload.OriginalReplicas == nil || *workload.OriginalReplicas <= 0 || !selected {
			return invalid(
				"Deployment workload requires positive original replicas and the selected Pod in its affected set",
			)
		}
	case v1alpha1.WorkloadKubeBlocks:
		if workload.KubeBlocks == nil || workload.KubeBlocks.Cluster == "" ||
			workload.KubeBlocks.ClusterUID == "" {
			return invalid("KubeBlocks workload requires its Cluster name and UID")
		}
	case v1alpha1.WorkloadVMCluster:
		if workload.VMCluster == nil || workload.VMCluster.Name == "" ||
			workload.VMCluster.UID == "" {
			return invalid("VMCluster workload requires its name and UID")
		}
	case v1alpha1.WorkloadGrafana:
		if workload.Grafana == nil || workload.Grafana.Name == "" || workload.Grafana.UID == "" {
			return invalid("Grafana workload requires its name and UID")
		}
	default:
		return invalid(fmt.Sprintf("unsupported workload adapter %q", workload.Adapter))
	}

	return nil
}

func validatePodCheckpointReference(pod v1alpha1.ObjectReference, namespace string) error {
	if pod.Name == "" || pod.UID == "" || pod.Namespace != namespace {
		return domain.NewError(domain.ErrorValidation, "pod migration",
			"workload checkpoint requires a Pod name and UID in the source namespace")
	}

	return nil
}

func validatePodMigrationCheckpoints(
	volumes map[string]v1alpha1.VolumeSpec,
	checkpoints []v1alpha1.ClusterPodMigrationVolumeStatus,
	sourceNamespace, temporaryNamespace string,
	phase v1alpha1.WorkflowPhase,
) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "pod migration", message)
	}

	if len(checkpoints) == 0 && (phase == domain.PhasePlanned || phase == domain.PhaseReserving ||
		phase == domain.PhaseAborting || phase == domain.PhaseAborted) {
		return nil
	}

	if len(checkpoints) != len(volumes) {
		return invalid("pod migration checkpoints must cover every planned volume")
	}

	seen := make(map[string]bool, len(checkpoints))
	for _, checkpoint := range checkpoints {
		volume, exists := volumes[checkpoint.SourcePVCName]
		if !exists || seen[checkpoint.SourcePVCName] {
			return invalid("pod migration checkpoint contains an unknown or duplicate source PVC")
		}

		seen[checkpoint.SourcePVCName] = true
		if err := validateMigrationVolumeCheckpoint(
			volume,
			checkpoint.ClusterVolumeReservationStatus,
			checkpoint.Activation,
			checkpoint.Sync.Attempts,
			sourceNamespace,
			temporaryNamespace,
		); err != nil {
			return err
		}

		if err := validateMigrationVolumePhase(
			checkpoint.Reserved,
			checkpoint.Sync.FinalCompletedAt,
			checkpoint.Activation.ActivatedAt,
			checkpoint.Activation.RolledBackAt,
			phase,
		); err != nil {
			return err
		}

		if phase == domain.PhaseWarmCopied && checkpoint.Sync.WarmCompletedAt == nil {
			return invalid("warm-copied pod migration requires a completed copy for every volume")
		}
	}

	return nil
}

func clusterPodMigrationVolumeIndexes(
	volumes []v1alpha1.ClusterPodMigrationVolumeStatus,
) map[string]int {
	indexes := make(map[string]int, len(volumes))
	for index, volume := range volumes {
		indexes[volume.SourcePVCName] = index
	}

	return indexes
}

func validatePodMigrationTransition(current, next v1alpha1.WorkflowPhase) error {
	edges := map[v1alpha1.WorkflowPhase][]v1alpha1.WorkflowPhase{
		"": {domain.PhaseAborted},
		domain.PhasePlanned: {
			domain.PhaseReserving,
			domain.PhaseAborting,
			domain.PhaseAborted,
		},
		domain.PhaseReserving: {domain.PhaseReserved, domain.PhaseAborting},
		domain.PhaseReserved: {
			domain.PhaseWarmCopying,
			domain.PhasePausing,
			domain.PhaseAborting,
		},
		domain.PhaseWarmCopying: {domain.PhaseWarmCopied, domain.PhaseAborting},
		domain.PhaseWarmCopied: {
			domain.PhaseWarmCopying,
			domain.PhasePausing,
			domain.PhaseAborting,
		},
		domain.PhasePausing:      {domain.PhasePaused, domain.PhaseAborting},
		domain.PhasePaused:       {domain.PhaseFinalSyncing, domain.PhaseAborting},
		domain.PhaseFinalSyncing: {domain.PhaseFinalSynced, domain.PhaseAborting},
		domain.PhaseFinalSynced: {
			domain.PhaseFinalSyncing,
			domain.PhaseActivating,
			domain.PhaseRollingBack,
			domain.PhaseAborting,
		},
		domain.PhaseActivating:  {domain.PhaseActivated, domain.PhaseRollingBack},
		domain.PhaseActivated:   {domain.PhaseResuming, domain.PhaseRollingBack},
		domain.PhaseResuming:    {domain.PhaseCompleted, domain.PhaseRollingBack},
		domain.PhaseCompleted:   {domain.PhaseRollingBack},
		domain.PhaseRollingBack: {domain.PhaseRolledBack},
		domain.PhaseAborting:    {domain.PhaseAborted},
	}
	if ((current == next || next == domain.PhaseFailed) && podMigrationPhase(current)) ||
		slices.Contains(edges[current], next) {
		return nil
	}

	return domain.NewError(domain.ErrorPrecondition, "pod migration",
		fmt.Sprintf("cannot transition from %s to %s", current, next))
}
