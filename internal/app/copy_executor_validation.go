package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func validateClusterCopyObject(object *v1alpha1.ClusterCopy) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "copy", message) }
	if object == nil || object.Name == "" || object.Namespace != "" {
		return invalid("a named cluster-scoped copy is required")
	}

	if err := validateCopyLifecycle(object.Status.WorkflowStatus); err != nil {
		return err
	}

	plan := object.Status.Plan

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if plan == nil {
		if len(object.Status.Volumes) != 0 || object.Status.SourceNode != "" ||
			(phase != "" && phase != domain.PhasePlanned && phase != domain.PhaseAborted) {
			return invalid("copy execution progress requires a plan")
		}

		return nil
	}

	if object.Status.Phase == "" || len(plan.Volumes) == 0 || plan.SourceNamespace == "" ||
		plan.DestinationNamespace == "" ||
		plan.SourceNamespace != object.Spec.SourceNamespace ||
		plan.DestinationNamespace != object.Spec.DestinationNamespace ||
		plan.Online != object.Spec.Online {
		return invalid("copy plan must match its spec and contain resolved volumes")
	}

	if err := domain.ValidateUnusedStoragePolicy(plan.UnusedStoragePolicy); err != nil {
		return err
	}

	if err := domain.ValidateUnusedStoragePolicy(object.Spec.UnusedStoragePolicy); err != nil {
		return err
	}

	volumes, err := validateReservationVolumes(
		plan.Volumes,
		plan.SourceNamespace,
		plan.DestinationNamespace,
	)
	if err != nil {
		return err
	}

	if err := validateCopyRequestedVolumes(object.Spec.Volumes, volumes); err != nil {
		return err
	}

	if object.Status.SourceNode != "" && plan.SourceNode != "" &&
		object.Status.SourceNode != plan.SourceNode {
		return invalid("copy source placement differs from the planned node")
	}

	return validateClusterCopyCheckpoints(
		plan.Volumes,
		string(plan.DestinationNamespace),
		object.Status.Volumes,
		phase,
	)
}

func validateCopyRequestedVolumes(
	requests []v1alpha1.VolumeRequest,
	volumes map[string]v1alpha1.VolumeSpec,
) error {
	for _, request := range requests {
		volume, exists := volumes[request.SourcePVC.Name]
		if !exists || !reservationReferenceMatches(request.SourcePVC, volume.SourcePVC) ||
			(request.SourcePV != nil && !reservationReferenceMatches(*request.SourcePV, volume.SourcePV)) ||
			(request.DestinationPVC != nil && !reservationReferenceMatches(*request.DestinationPVC, volume.DestinationPVC)) {
			return domain.NewError(
				domain.ErrorValidation,
				"copy",
				"planned volume does not satisfy the requested identity",
			)
		}
	}

	return nil
}

func validateClusterCopyCheckpoints(
	volumes []v1alpha1.VolumeSpec,
	destinationNamespace string,
	checkpoints []v1alpha1.ClusterCopyVolumeStatus,
	phase v1alpha1.WorkflowPhase,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "copy", message) }

	requiresReserved := phase == domain.PhaseReserved || phase == domain.PhaseWarmCopying ||
		phase == domain.PhaseWarmCopied
	if len(checkpoints) == 0 && !requiresReserved {
		return nil
	}

	if len(checkpoints) != len(volumes) {
		return invalid("copy checkpoint must cover every planned volume")
	}

	indexes := clusterCopyVolumeIndexes(checkpoints)
	if len(indexes) != len(checkpoints) {
		return invalid("copy checkpoint has duplicate source identities")
	}

	for _, volume := range volumes {
		index, found := indexes[volume.SourcePVC.Name]
		if !found {
			return invalid("copy checkpoint is missing a planned source")
		}

		checkpoint := checkpoints[index]
		if ref := checkpoint.DestinationPVC; ref != nil &&
			(ref.Name != volume.DestinationPVC.Name || ref.Namespace != destinationNamespace || ref.UID == "") {
			return invalid("copy destination PVC checkpoint is invalid")
		}

		if ref := checkpoint.DestinationPV; ref != nil &&
			(ref.Name == "" || ref.Namespace != "" || ref.UID == "") {
			return invalid("copy destination PV checkpoint is invalid")
		}

		if requiresReserved && !checkpoint.Reserved {
			return invalid("copy phase requires reserved volumes")
		}

		if checkpoint.Reserved &&
			(checkpoint.DestinationPVC == nil || checkpoint.DestinationPV == nil) {
			return invalid("reserved copy volume requires destination identities")
		}

		if checkpoint.Sync.Attempts < 0 || (checkpoint.Sync.Attempts > 0 && !checkpoint.Reserved) ||
			(checkpoint.Sync.WarmCompletedAt != nil && !checkpoint.Reserved) ||
			(phase == domain.PhaseWarmCopied && checkpoint.Sync.WarmCompletedAt == nil) {
			return invalid("copy synchronization checkpoint is invalid")
		}
	}

	return nil
}

func clusterCopyVolumeIndexes(volumes []v1alpha1.ClusterCopyVolumeStatus) map[string]int {
	indexes := make(map[string]int, len(volumes))
	for i, volume := range volumes {
		indexes[volume.SourcePVCName] = i
	}

	return indexes
}

func (c *ClusterCopyExecutor) Validate(ctx context.Context, object *v1alpha1.ClusterCopy) error {
	if err := validateClusterCopyObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil ||
		workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseAborted {
		return nil
	}

	if err := validateCopyRetry(object.Status.FailureReason); err != nil {
		return err
	}

	_, err := c.validateResources(ctx, object)

	return err
}

func validateCopyRetry(reason string) error {
	if reason == domain.FailureDestinationCapacityExhausted {
		return domain.NewError(
			domain.ErrorConflict,
			"copy",
			"destination capacity was exhausted; abort and clean up this copy before creating one with larger storage",
		)
	}

	return nil
}

func (c *ClusterCopyExecutor) validateResources(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
) (string, error) {
	checkpoints := make(
		map[string]v1alpha1.ClusterVolumeReservationStatus,
		len(object.Status.Volumes),
	)
	for _, volume := range object.Status.Volumes {
		checkpoints[volume.SourcePVCName] = *volume.ClusterVolumeReservationStatus.DeepCopy()
	}

	return c.validateStorage(
		ctx,
		object.Name,
		string(object.Status.Plan.SourceNamespace),
		string(object.Status.Plan.DestinationNamespace),
		&object.Status.Plan.CopyPlan,
		object.Status.SourceNode,
		checkpoints,
	)
}

func validateCopyLifecycle(status v1alpha1.WorkflowStatus) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "copy", message) }
	for _, phase := range []v1alpha1.WorkflowPhase{status.Phase, status.ResumeFrom} {
		switch phase {
		case "",
			domain.PhasePlanned,
			domain.PhaseReserving,
			domain.PhaseReserved,
			domain.PhaseWarmCopying,
			domain.PhaseWarmCopied,
			domain.PhaseAborting,
			domain.PhaseAborted,
			domain.PhaseFailed:
		default:
			return invalid("phase does not belong to copy")
		}
	}

	if status.Phase == domain.PhaseFailed &&
		(status.ResumeFrom == "" || status.ResumeFrom == domain.PhaseFailed) {
		return invalid("failed copy requires a valid resume checkpoint")
	}

	if status.FailureReason != "" &&
		(status.Phase != domain.PhaseFailed || status.FailureReason != domain.FailureDestinationCapacityExhausted) {
		return invalid("copy failure reason is invalid")
	}

	return nil
}

func validateCopyTransition(current, next v1alpha1.WorkflowPhase, planned bool) error {
	allowed := current == next || next == domain.PhaseFailed ||
		(next == domain.PhaseReserving && current == domain.PhasePlanned) ||
		(next == domain.PhaseReserved && current == domain.PhaseReserving) ||
		(next == domain.PhaseWarmCopying && (current == domain.PhaseReserved || current == domain.PhaseWarmCopied)) ||
		(next == domain.PhaseWarmCopied && current == domain.PhaseWarmCopying) ||
		(next == domain.PhaseAborting && current != domain.PhaseAborted) ||
		(next == domain.PhaseAborted && (current == domain.PhaseAborting || !planned))
	if !allowed {
		return domain.NewError(domain.ErrorConflict, "copy", "invalid copy phase transition")
	}

	return nil
}
