package app

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func validatePodMigrationObject(object *v1alpha1.PodMigration) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "pod migration", message)
	}
	if object == nil || object.Name == "" || object.Namespace == "" {
		return invalid("a named namespaced pod migration is required")
	}

	if err := validatePodMigrationLifecycle(object.Status.WorkflowStatus); err != nil {
		return err
	}

	if err := domain.ValidateUnusedStoragePolicy(object.Spec.UnusedStoragePolicy); err != nil {
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

	if plan.PrecopyPasses != object.Spec.PrecopyPasses {
		return invalid("pod migration plan must preserve the requested precopy passes")
	}

	if err := domain.ValidateUnusedStoragePolicy(plan.UnusedStoragePolicy); err != nil {
		return err
	}

	if err := validatePodMigrationWorkload(object.Spec.Pod, plan.Workload); err != nil {
		return err
	}

	if err := validatePodMigrationSnapshot(
		object.Namespace,
		plan.Workload,
		object.Status.OriginalPodSnapshotHash,
	); err != nil {
		return err
	}

	volumes, err := validateReservationVolumes(
		plan.Volumes,
		v1alpha1.NamespaceName(object.Namespace),
		v1alpha1.NamespaceName(object.Namespace),
	)
	if err != nil {
		return err
	}

	if err := validatePodMigrationOverrides(object.Spec.Volumes, volumes); err != nil {
		return err
	}

	if err := validatePodSharedMountCheckpoints(
		plan.Volumes,
		object.Status.OpenEBSLVMSharedMounts,
	); err != nil {
		return err
	}

	if err := validatePodWorkloadCheckpoint(object.Status.Workload, object.Namespace); err != nil {
		return err
	}

	return validateLocalPodMigrationCheckpoints(volumes, object.Status.Volumes, phase)
}

func validatePodWorkloadCheckpoint(
	workload *v1alpha1.PodMigrationWorkloadStatus,
	namespace string,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "pod migration", message) }
	if workload != nil {
		if workload.Pod != nil {
			if err := validatePodCheckpointReference(
				qualifiedResourceReference(*workload.Pod, namespace), namespace,
			); err != nil {
				return err
			}
		}

		seen := make(map[string]bool, len(workload.AffectedPods))
		for _, pod := range workload.AffectedPods {
			if err := validatePodCheckpointReference(
				qualifiedResourceReference(pod, namespace), namespace,
			); err != nil {
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

func validateLocalPodMigrationCheckpoints(
	volumes map[string]v1alpha1.VolumeSpec,
	checkpoints []v1alpha1.PodMigrationVolumeStatus,
	phase v1alpha1.WorkflowPhase,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "pod migration", message) }

	if len(checkpoints) == 0 &&
		(phase == domain.PhasePlanned || phase == domain.PhaseReserving ||
			phase == domain.PhaseAborting || phase == domain.PhaseAborted) {
		return nil
	}

	if len(checkpoints) != len(volumes) {
		return invalid("pod migration checkpoints must cover every planned volume")
	}

	seen := make(map[string]bool, len(volumes))
	for _, checkpoint := range checkpoints {
		volume, exists := volumes[checkpoint.SourcePVCName]
		if !exists || seen[checkpoint.SourcePVCName] {
			return invalid("pod migration checkpoint contains an unknown or duplicate source PVC")
		}

		seen[checkpoint.SourcePVCName] = true
		if err := validateLocalMigrationCheckpoint(volume, checkpoint.VolumeReservationStatus,
			checkpoint.Activation, checkpoint.Sync.Attempts); err != nil {
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

func podMigrationVolumeIndexes(volumes []v1alpha1.PodMigrationVolumeStatus) map[string]int {
	indexes := make(map[string]int, len(volumes))
	for index, volume := range volumes {
		indexes[volume.SourcePVCName] = index
	}

	return indexes
}
