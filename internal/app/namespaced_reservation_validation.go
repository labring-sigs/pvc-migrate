package app

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func validateReservationObject(object *v1alpha1.Reservation) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "reservation", message) }
	if object == nil || object.Name == "" || object.Namespace == "" {
		return invalid("a named namespaced reservation is required")
	}

	if err := validateReservationLifecycle(object.Status.WorkflowStatus); err != nil {
		return err
	}

	plan := object.Status.Plan

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if plan == nil {
		if len(object.Status.Volumes) != 0 ||
			(phase != "" && phase != domain.PhasePlanned && phase != domain.PhaseAborted) {
			return invalid("reservation progress requires an execution plan")
		}

		return nil
	}

	if object.Status.Phase == "" || len(plan.Volumes) == 0 {
		return invalid("planned reservation requires a lifecycle phase and volumes")
	}

	if err := domain.ValidateReclaimPolicies(
		"",
		object.Spec.DestinationPVCReclaimPolicy,
	); err != nil {
		return err
	}

	if err := domain.ValidateReclaimPolicies("", plan.DestinationPVCReclaimPolicy); err != nil {
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

	for _, request := range object.Spec.Volumes {
		volume, exists := volumes[request.SourcePVC.Name]
		if !exists || !reservationReferenceMatches(request.SourcePVC, volume.SourcePVC) ||
			(request.SourcePV != nil && !reservationReferenceMatches(*request.SourcePV, volume.SourcePV)) ||
			(request.DestinationPVC != nil && !reservationReferenceMatches(*request.DestinationPVC, volume.DestinationPVC)) {
			return invalid("planned volume does not satisfy the requested identity")
		}
	}

	return validateReservationCheckpoints(volumes, object.Status.Volumes, phase)
}

func validateReservationCheckpoints(
	volumes map[string]v1alpha1.VolumeSpec,
	checkpoints []v1alpha1.ReservationVolumeStatus,
	phase v1alpha1.WorkflowPhase,
) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "reservation", message) }

	if len(checkpoints) == 0 && phase != domain.PhaseReserved {
		return nil
	}

	if len(checkpoints) != len(volumes) {
		return invalid("reservation checkpoint must cover every planned volume")
	}

	seen := make(map[string]bool, len(volumes))
	for _, checkpoint := range checkpoints {
		volume, exists := volumes[checkpoint.SourcePVCName]
		if !exists || seen[checkpoint.SourcePVCName] {
			return invalid("reservation checkpoint contains an unknown or duplicate source PVC")
		}

		seen[checkpoint.SourcePVCName] = true
		if ref := checkpoint.DestinationPVC; ref != nil &&
			(ref.Name != volume.DestinationPVC.Name || ref.UID == "") {
			return invalid("reservation checkpoint destination PVC identity is invalid")
		}

		if ref := checkpoint.DestinationPV; ref != nil && (ref.Name == "" || ref.UID == "") {
			return invalid("reservation checkpoint destination PV identity is invalid")
		}

		if checkpoint.Reserved &&
			(checkpoint.DestinationPVC == nil || checkpoint.DestinationPV == nil) {
			return invalid("reserved volume requires destination PVC and PV identities")
		}

		if phase == domain.PhaseReserved && !checkpoint.Reserved {
			return invalid("reservation has an incomplete volume checkpoint")
		}
	}

	return nil
}

func reservationVolumeIndexes(volumes []v1alpha1.ReservationVolumeStatus) map[string]int {
	indexes := make(map[string]int, len(volumes))
	for i, volume := range volumes {
		indexes[volume.SourcePVCName] = i
	}

	return indexes
}

// Resource provisioning uses explicit namespace-qualified references. These
// helpers translate only that storage checkpoint, never a workflow or its plan.
func qualifiedReservationCheckpoint(
	local v1alpha1.VolumeReservationStatus,
	namespace string,
) v1alpha1.ClusterVolumeReservationStatus {
	checkpoint := v1alpha1.ClusterVolumeReservationStatus{
		SourcePVCName:     local.SourcePVCName,
		Reserved:          local.Reserved,
		DestinationPolicy: local.DestinationPolicy,
	}
	if local.DestinationPVC != nil {
		ref := qualifiedResourceReference(*local.DestinationPVC, namespace)
		checkpoint.DestinationPVC = &ref
	}

	if local.DestinationPV != nil {
		ref := qualifiedResourceReference(*local.DestinationPV, "")
		checkpoint.DestinationPV = &ref
	}

	return checkpoint
}

func localReservationCheckpoint(
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
	namespace string,
) (v1alpha1.VolumeReservationStatus, error) {
	local := v1alpha1.VolumeReservationStatus{
		SourcePVCName:     checkpoint.SourcePVCName,
		Reserved:          checkpoint.Reserved,
		DestinationPolicy: checkpoint.DestinationPolicy,
	}
	if checkpoint.DestinationPVC != nil {
		if checkpoint.DestinationPVC.Namespace != namespace {
			return local, domain.NewError(
				domain.ErrorConflict,
				"reservation checkpoint",
				"destination PVC is outside the workflow namespace",
			)
		}

		local.DestinationPVC = localResourceReference(*checkpoint.DestinationPVC)
	}

	if checkpoint.DestinationPV != nil {
		if checkpoint.DestinationPV.Namespace != "" {
			return local, domain.NewError(
				domain.ErrorConflict,
				"reservation checkpoint",
				"destination PV must be cluster-scoped",
			)
		}

		local.DestinationPV = localResourceReference(*checkpoint.DestinationPV)
	}

	return local, nil
}

func validateReservationTransition(current, next v1alpha1.WorkflowPhase, planned bool) error {
	allowed := current == next || next == domain.PhaseFailed ||
		(next == domain.PhaseReserving && (current == domain.PhasePlanned || current == domain.PhaseFailed)) ||
		(next == domain.PhaseReserved && current == domain.PhaseReserving) ||
		(next == domain.PhaseAborting && (current == domain.PhasePlanned || current == domain.PhaseReserving || current == domain.PhaseReserved || current == domain.PhaseFailed)) ||
		(next == domain.PhaseAborted && (current == domain.PhaseAborting || !planned))
	if !allowed {
		return domain.NewError(
			domain.ErrorConflict,
			"reservation",
			"invalid reservation phase transition",
		)
	}

	return nil
}
