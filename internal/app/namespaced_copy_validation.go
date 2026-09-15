package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func validateCopyObject(object *v1alpha1.Copy) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "copy", message) }
	if object == nil || object.Name == "" || object.Namespace == "" {
		return invalid("a named namespaced copy is required")
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

	if object.Status.Phase == "" || len(plan.Volumes) == 0 ||
		plan.Online != object.Spec.Online {
		return invalid("copy plan must match its spec and contain resolved volumes")
	}

	if err := domain.ValidateReclaimPolicies("", plan.DestinationPVCReclaimPolicy); err != nil {
		return err
	}

	if err := domain.ValidateReclaimPolicies(
		"",
		object.Spec.DestinationPVCReclaimPolicy,
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

	if err := validateCopyRequestedVolumes(object.Spec.Volumes, volumes); err != nil {
		return err
	}

	if object.Status.SourceNode != "" && plan.SourceNode != "" &&
		object.Status.SourceNode != plan.SourceNode {
		return invalid("copy source placement differs from the planned node")
	}

	return validateCopyCheckpoints(plan.Volumes, object.Status.Volumes, phase)
}

func validateCopyCheckpoints(
	volumes []v1alpha1.VolumeSpec,
	checkpoints []v1alpha1.CopyVolumeStatus,
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

	indexes := copyVolumeIndexes(checkpoints)
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
			(ref.Name != volume.DestinationPVC.Name || ref.UID == "") {
			return invalid("copy destination PVC checkpoint is invalid")
		}

		if ref := checkpoint.DestinationPV; ref != nil &&
			(ref.Name == "" || ref.UID == "") {
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

func copyVolumeIndexes(volumes []v1alpha1.CopyVolumeStatus) map[string]int {
	indexes := make(map[string]int, len(volumes))
	for i, volume := range volumes {
		indexes[volume.SourcePVCName] = i
	}

	return indexes
}

func (c *CopyExecutor) Validate(ctx context.Context, object *v1alpha1.Copy) error {
	if err := validateCopyObject(object); err != nil {
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

func (c *CopyExecutor) validateResources(
	ctx context.Context,
	object *v1alpha1.Copy,
) (string, error) {
	checkpoints := make(
		map[string]v1alpha1.ClusterVolumeReservationStatus,
		len(object.Status.Volumes),
	)
	for _, volume := range object.Status.Volumes {
		checkpoints[volume.SourcePVCName] = qualifiedReservationCheckpoint(
			volume.VolumeReservationStatus,
			object.Namespace,
		)
	}

	return c.validateStorage(
		ctx,
		object.Name,
		object.Namespace,
		object.Namespace,
		object.Status.Plan,
		object.Status.SourceNode,
		checkpoints,
	)
}
