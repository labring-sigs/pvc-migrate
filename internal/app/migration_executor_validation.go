package app

import (
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func migrationPhase(phase v1alpha1.WorkflowPhase) bool {
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved,
		domain.PhaseFinalSyncing, domain.PhaseFinalSynced, domain.PhaseActivating,
		domain.PhaseActivated, domain.PhaseResuming, domain.PhaseCompleted,
		domain.PhaseAborting, domain.PhaseAborted, domain.PhaseRollingBack, domain.PhaseRolledBack:
		return true
	default:
		return false
	}
}

func validateMigrationLifecycle(status v1alpha1.WorkflowStatus) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "migration", message)
	}
	if status.Phase != "" && status.Phase != domain.PhaseFailed && !migrationPhase(status.Phase) {
		return invalid("phase does not belong to offline migration")
	}

	if status.ResumeFrom != "" && !migrationPhase(status.ResumeFrom) {
		return invalid("resume checkpoint does not belong to offline migration")
	}

	if status.Phase == domain.PhaseFailed && status.ResumeFrom == "" {
		return invalid("failed migration requires a resume checkpoint")
	}

	if status.FailureReason != "" &&
		(status.FailureReason != domain.FailureDestinationCapacityExhausted ||
			workflowResumePhase(status) != domain.PhaseFinalSyncing) {
		return invalid("migration failure reason does not belong to the current stage")
	}

	return nil
}

func validateClusterMigrationObject(object *v1alpha1.ClusterMigration) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "migration", message)
	}
	if object == nil || object.Name == "" || object.Namespace != "" {
		return invalid("a named cluster-scoped migration is required")
	}

	if err := validateMigrationLifecycle(object.Status.WorkflowStatus); err != nil {
		return err
	}

	if err := domain.ValidateUnusedStoragePolicy(object.Spec.UnusedStoragePolicy); err != nil {
		return err
	}

	plan := object.Status.Plan

	phase := workflowResumePhase(object.Status.WorkflowStatus)
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

	if err := validateMigrationPlanNamespaces(object.Spec, plan); err != nil {
		return err
	}

	if err := domain.ValidateUnusedStoragePolicy(plan.UnusedStoragePolicy); err != nil {
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

	if err := validateMigrationRequestedVolumes(object.Spec.Volumes, volumes); err != nil {
		return err
	}

	return validateMigrationCheckpoints(
		volumes,
		object.Status.Volumes,
		string(plan.SourceNamespace),
		string(plan.TemporaryNamespace),
		phase,
	)
}

func validateMigrationPlanNamespaces(
	spec v1alpha1.ClusterMigrationSpec,
	plan *v1alpha1.ClusterMigrationPlan,
) error {
	temporaryNamespace, sessionNamespace := spec.TemporaryNamespace, spec.SessionNamespace
	if temporaryNamespace == "" {
		temporaryNamespace = spec.SourceNamespace
	}

	if sessionNamespace == "" {
		sessionNamespace = spec.SourceNamespace
	}

	destinationNamespace := spec.DestinationNamespace
	if destinationNamespace == "" {
		destinationNamespace = spec.SourceNamespace
	}

	if plan.SourceNamespace == "" || plan.TemporaryNamespace == "" || plan.SessionNamespace == "" ||
		plan.SourceNamespace != spec.SourceNamespace || plan.TemporaryNamespace != temporaryNamespace ||
		plan.SessionNamespace != sessionNamespace || plan.DestinationNamespace != destinationNamespace {
		return domain.NewError(
			domain.ErrorValidation,
			"migration",
			"migration plan namespace roles must match its spec and preserve source identities",
		)
	}

	return nil
}

func validateMigrationRequestedVolumes(
	requests []v1alpha1.VolumeRequest,
	volumes map[string]v1alpha1.VolumeSpec,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "migration", message) }
	if len(requests) != len(volumes) {
		return invalid("migration plan must cover exactly the requested volumes")
	}

	requested := make(map[string]bool, len(requests))
	for _, request := range requests {
		volume, exists := volumes[request.SourcePVC.Name]
		if !exists || requested[request.SourcePVC.Name] ||
			!reservationReferenceMatches(request.SourcePVC, volume.SourcePVC) ||
			(request.SourcePV != nil && !reservationReferenceMatches(*request.SourcePV, volume.SourcePV)) ||
			(request.DestinationPVC != nil && !reservationReferenceMatches(*request.DestinationPVC, volume.DestinationPVC)) {
			return invalid("migration plan does not satisfy the requested identities")
		}

		requested[request.SourcePVC.Name] = true
	}

	return nil
}

func validateMigrationCheckpoints(
	volumes map[string]v1alpha1.VolumeSpec,
	checkpoints []v1alpha1.ClusterMigrationVolumeStatus,
	sourceNamespace, temporaryNamespace string,
	phase v1alpha1.WorkflowPhase,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "migration", message) }

	if len(checkpoints) == 0 && (phase == domain.PhasePlanned || phase == domain.PhaseReserving ||
		phase == domain.PhaseAborting || phase == domain.PhaseAborted) {
		return nil
	}

	if len(checkpoints) != len(volumes) {
		return invalid("migration checkpoints must cover every planned volume")
	}

	seen := make(map[string]bool, len(checkpoints))
	for _, checkpoint := range checkpoints {
		volume, exists := volumes[checkpoint.SourcePVCName]
		if !exists || seen[checkpoint.SourcePVCName] {
			return invalid("migration checkpoint contains an unknown or duplicate source PVC")
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
	}

	return nil
}

func validateMigrationVolumeCheckpoint(
	volume v1alpha1.VolumeSpec,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
	activation v1alpha1.ClusterVolumeActivationStatus,
	copyAttempts int,
	sourceNamespace, temporaryNamespace string,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "migration", message) }
	if ref := checkpoint.DestinationPVC; ref != nil &&
		(ref.Name != volume.DestinationPVC.Name || ref.Namespace != temporaryNamespace || ref.UID == "") {
		return invalid("migration destination PVC checkpoint is invalid")
	}

	if ref := checkpoint.DestinationPV; ref != nil &&
		(ref.Name == "" || ref.UID == "" || ref.Namespace != "") {
		return invalid("migration destination PV checkpoint is invalid")
	}

	if checkpoint.Reserved &&
		(checkpoint.DestinationPVC == nil || checkpoint.DestinationPV == nil) {
		return invalid("reserved migration volume requires destination PVC and PV identities")
	}

	if ref := activation.ActivePVC; ref != nil &&
		(ref.Name != volume.SourcePVC.Name || ref.Namespace != sourceNamespace || ref.UID == "") {
		return invalid("migration active PVC must preserve the source identity name and namespace")
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

func validateMigrationVolumePhase(
	reserved bool,
	finalCompletedAt, activatedAt, rolledBackAt *metav1.Time,
	phase v1alpha1.WorkflowPhase,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "migration", message) }
	if phase != domain.PhasePlanned && phase != domain.PhaseReserving &&
		phase != domain.PhaseAborting &&
		phase != domain.PhaseAborted &&
		!reserved {
		return invalid("migration cannot advance before every volume is reserved")
	}

	switch phase {
	case domain.PhaseFinalSynced,
		domain.PhaseActivating,
		domain.PhaseActivated,
		domain.PhaseResuming,
		domain.PhaseCompleted:
		if finalCompletedAt == nil {
			return invalid("migration cutover requires completed final sync for every volume")
		}
	}

	switch phase {
	case domain.PhaseActivated, domain.PhaseResuming, domain.PhaseCompleted:
		if activatedAt == nil {
			return invalid("migration completion requires activation for every volume")
		}
	case domain.PhaseRolledBack:
		if rolledBackAt == nil {
			return invalid("migration rollback requires a restored checkpoint for every volume")
		}
	}

	return nil
}

func validateMigrationCopyRetry(reason string) error {
	if reason == domain.FailureDestinationCapacityExhausted {
		return domain.NewError(
			domain.ErrorConflict,
			"migration final sync",
			"destination capacity was exhausted; abort and clean up this migration before creating one with larger storage",
		)
	}

	return nil
}

func clusterMigrationVolumeIndexes(volumes []v1alpha1.ClusterMigrationVolumeStatus) map[string]int {
	indexes := make(map[string]int, len(volumes))
	for index, volume := range volumes {
		indexes[volume.SourcePVCName] = index
	}

	return indexes
}

func validateMigrationTransition(current, next v1alpha1.WorkflowPhase) error {
	edges := map[v1alpha1.WorkflowPhase][]v1alpha1.WorkflowPhase{
		"": {domain.PhaseAborted},
		domain.PhasePlanned: {
			domain.PhaseReserving,
			domain.PhaseAborting,
			domain.PhaseAborted,
		},
		domain.PhaseReserving:    {domain.PhaseReserved, domain.PhaseAborting},
		domain.PhaseReserved:     {domain.PhaseFinalSyncing, domain.PhaseAborting},
		domain.PhaseFinalSyncing: {domain.PhaseFinalSynced, domain.PhaseAborting},
		domain.PhaseFinalSynced: {
			domain.PhaseFinalSyncing,
			domain.PhaseActivating,
			domain.PhaseRollingBack,
			domain.PhaseAborting,
		},
		domain.PhaseActivating: {domain.PhaseActivated, domain.PhaseRollingBack},
		domain.PhaseActivated: {
			domain.PhaseResuming,
			domain.PhaseCompleted,
			domain.PhaseRollingBack,
		},
		domain.PhaseResuming:    {domain.PhaseCompleted, domain.PhaseRollingBack},
		domain.PhaseCompleted:   {domain.PhaseRollingBack},
		domain.PhaseRollingBack: {domain.PhaseRolledBack},
		domain.PhaseAborting:    {domain.PhaseAborted},
	}

	allowed := (current == next || next == domain.PhaseFailed) && migrationPhase(current) ||
		slices.Contains(edges[current], next)
	if !allowed {
		return domain.NewError(
			domain.ErrorPrecondition,
			"migration",
			fmt.Sprintf("cannot transition from %s to %s", current, next),
		)
	}

	return nil
}
