package app

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func validateMigrationObject(object *v1alpha1.Migration) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "migration", message) }
	if object == nil || object.Name == "" || object.Namespace == "" {
		return invalid("a named namespaced migration is required")
	}

	if err := validateMigrationLifecycle(object.Status.WorkflowStatus); err != nil {
		return err
	}

	if err := domain.ValidateUnusedStoragePolicy(object.Spec.UnusedStoragePolicy); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)

	plan := object.Status.Plan
	if plan == nil {
		if len(object.Status.Volumes) != 0 ||
			(phase != "" && phase != domain.PhasePlanned && phase != domain.PhaseAborted) {
			return invalid("migration progress requires an execution plan")
		}

		return nil
	}

	if len(plan.Volumes) == 0 || object.Status.Phase == "" {
		return invalid("migration plan requires volumes and a lifecycle phase")
	}

	if err := domain.ValidateUnusedStoragePolicy(plan.UnusedStoragePolicy); err != nil {
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

	if err := validateMigrationRequestedVolumes(object.Spec.Volumes, volumes); err != nil {
		return err
	}

	if len(object.Status.Volumes) == 0 &&
		(phase == domain.PhasePlanned || phase == domain.PhaseReserving || phase == domain.PhaseAborting || phase == domain.PhaseAborted) {
		return nil
	}

	if len(object.Status.Volumes) != len(volumes) {
		return invalid("migration checkpoints must cover every planned volume")
	}

	seen := make(map[string]bool, len(volumes))
	for _, checkpoint := range object.Status.Volumes {
		volume, exists := volumes[checkpoint.SourcePVCName]
		if !exists || seen[checkpoint.SourcePVCName] {
			return invalid("migration checkpoint contains an unknown or duplicate source PVC")
		}

		seen[checkpoint.SourcePVCName] = true
		if err := validateLocalMigrationCheckpoint(
			volume,
			checkpoint.VolumeReservationStatus,
			checkpoint.Activation,
			checkpoint.Sync.Attempts,
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
	}

	return nil
}

func validateLocalMigrationCheckpoint(
	volume v1alpha1.VolumeSpec,
	checkpoint v1alpha1.VolumeReservationStatus,
	activation v1alpha1.VolumeActivationStatus,
	copyAttempts int,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "migration", message) }
	if ref := checkpoint.DestinationPVC; ref != nil &&
		(ref.Name != volume.DestinationPVC.Name || ref.UID == "") {
		return invalid("migration destination PVC checkpoint is invalid")
	}

	if ref := checkpoint.DestinationPV; ref != nil && (ref.Name == "" || ref.UID == "") {
		return invalid("migration destination PV checkpoint is invalid")
	}

	if checkpoint.Reserved &&
		(checkpoint.DestinationPVC == nil || checkpoint.DestinationPV == nil) {
		return invalid("reserved migration volume requires destination PVC and PV identities")
	}

	if ref := activation.ActivePVC; ref != nil &&
		(ref.Name != volume.SourcePVC.Name || ref.UID == "") {
		return invalid("migration active PVC must preserve the source identity name")
	}

	if copyAttempts < 0 {
		return invalid("migration copy attempts cannot be negative")
	}

	if copyAttempts > 0 && !checkpoint.Reserved {
		return invalid("migration copy attempts require reserved destination identities")
	}

	if (activation.ActivatedAt != nil || activation.RolledBackAt != nil) &&
		activation.ActivePVC == nil {
		return invalid("completed migration cutover requires an active PVC identity")
	}

	return nil
}

func migrationVolumeIndexes(volumes []v1alpha1.MigrationVolumeStatus) map[string]int {
	indexes := make(map[string]int, len(volumes))
	for index, volume := range volumes {
		indexes[volume.SourcePVCName] = index
	}

	return indexes
}
