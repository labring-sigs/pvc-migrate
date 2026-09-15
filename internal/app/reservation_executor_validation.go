package app

import (
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/apimachinery/pkg/api/resource"
)

func validateClusterReservationObject(object *v1alpha1.ClusterReservation) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "reservation", message)
	}
	if object == nil || object.Name == "" || object.Namespace != "" {
		return invalid("a named cluster-scoped reservation is required")
	}

	if err := validateReservationLifecycle(object.Status.WorkflowStatus); err != nil {
		return err
	}

	plan := object.Status.Plan
	if plan == nil {
		phase := workflowResumePhase(object.Status.WorkflowStatus)
		if len(object.Status.Volumes) != 0 ||
			(phase != "" && phase != domain.PhasePlanned && phase != domain.PhaseAborted) {
			return invalid("reservation progress requires an execution plan")
		}

		return nil
	}

	if len(plan.Volumes) == 0 {
		return invalid("reservation plan requires at least one volume")
	}

	if object.Status.Phase == "" {
		return invalid("planned reservation requires a lifecycle phase")
	}

	if plan.SourceNamespace == "" || plan.DestinationNamespace == "" ||
		plan.SourceNamespace != object.Spec.SourceNamespace || plan.DestinationNamespace != object.Spec.DestinationNamespace {
		return invalid("reservation plan namespaces must match its spec")
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

	phase := workflowResumePhase(object.Status.WorkflowStatus)

	volumes, err := validateReservationVolumes(
		plan.Volumes,
		plan.SourceNamespace,
		plan.DestinationNamespace,
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

	return validateClusterReservationCheckpoints(
		volumes,
		object.Status.Volumes,
		string(plan.DestinationNamespace),
		phase,
	)
}

func validateReservationLifecycle(status v1alpha1.WorkflowStatus) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "reservation", message)
	}

	switch status.Phase {
	case "", domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved,
		domain.PhaseAborting, domain.PhaseAborted, domain.PhaseFailed:
	default:
		return invalid("phase does not belong to reservation")
	}

	switch status.ResumeFrom {
	case "", domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved, domain.PhaseAborting:
	default:
		return invalid("resume checkpoint does not belong to reservation")
	}

	if status.Phase == domain.PhaseFailed && status.ResumeFrom == "" {
		return invalid("failed reservation has no resume checkpoint")
	}

	if status.FailureReason != "" {
		return invalid("reservation has an unsupported failure reason")
	}

	return nil
}

func validateReservationVolumes(
	planned []v1alpha1.VolumeSpec,
	sourceNamespace, destinationNamespace v1alpha1.NamespaceName,
) (map[string]v1alpha1.VolumeSpec, error) {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "reservation", message)
	}
	volumes := make(map[string]v1alpha1.VolumeSpec, len(planned))

	destinations := make(map[string]bool, len(planned))
	for _, volume := range planned {
		if volume.SourcePVC.Name == "" || volume.SourcePVC.UID == "" ||
			volume.SourcePV.Name == "" ||
			volume.SourcePV.UID == "" ||
			volume.DestinationPVC.Name == "" {
			return nil, invalid(
				"planned volume requires source PVC/PV identities and a destination name",
			)
		}

		if _, exists := volumes[volume.SourcePVC.Name]; exists ||
			destinations[volume.DestinationPVC.Name] {
			return nil, invalid("planned source and destination PVC names must be unique")
		}

		if sourceNamespace == destinationNamespace &&
			volume.SourcePVC.Name == volume.DestinationPVC.Name {
			return nil, invalid("destination PVC must differ from the source")
		}

		for _, capacity := range []string{volume.SourceCapacity, volume.Capacity} {
			quantity, err := resource.ParseQuantity(capacity)
			if err != nil || quantity.Sign() <= 0 {
				return nil, invalid(
					"planned source and destination capacities must be positive quantities",
				)
			}
		}

		volumes[volume.SourcePVC.Name] = volume
		destinations[volume.DestinationPVC.Name] = true
	}

	return volumes, nil
}

func validateClusterReservationCheckpoints(
	volumes map[string]v1alpha1.VolumeSpec,
	checkpoints []v1alpha1.ClusterReservationVolumeStatus,
	destinationNamespace string,
	phase v1alpha1.WorkflowPhase,
) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "reservation", message)
	}

	if len(checkpoints) == 0 && phase != domain.PhaseReserved {
		return nil
	}

	if len(checkpoints) != len(volumes) {
		return invalid("reservation checkpoint must cover every planned volume")
	}

	seen := make(map[string]bool, len(checkpoints))
	for _, checkpoint := range checkpoints {
		volume, exists := volumes[checkpoint.SourcePVCName]
		if !exists || seen[checkpoint.SourcePVCName] {
			return invalid("reservation checkpoint contains an unknown or duplicate source PVC")
		}

		seen[checkpoint.SourcePVCName] = true
		if ref := checkpoint.DestinationPVC; ref != nil &&
			(ref.Name != volume.DestinationPVC.Name || ref.Namespace != destinationNamespace || ref.UID == "") {
			return invalid("reservation checkpoint destination PVC identity is invalid")
		}

		if ref := checkpoint.DestinationPV; ref != nil &&
			(ref.Name == "" || ref.Namespace != "" || ref.UID == "") {
			return invalid("reservation checkpoint destination PV identity is invalid")
		}

		if checkpoint.Reserved &&
			(checkpoint.DestinationPVC == nil || checkpoint.DestinationPV == nil) {
			return invalid("reserved volume requires destination PVC and PV identities")
		}

		if phase == domain.PhaseReserved && !checkpoint.Reserved {
			return invalid(
				fmt.Sprintf("PVC %s has not completed reservation", checkpoint.SourcePVCName),
			)
		}
	}

	return nil
}

func reservationReferenceMatches(request, resolved v1alpha1.LocalResourceReference) bool {
	return request.Name == resolved.Name &&
		(request.UID == "" || request.UID == resolved.UID) &&
		(request.ResourceVersion == "" || request.ResourceVersion == resolved.ResourceVersion) &&
		(request.Kind == "" || request.Kind == resolved.Kind) &&
		(request.APIVersion == "" || request.APIVersion == resolved.APIVersion)
}

func clusterReservationVolumeIndexes(
	volumes []v1alpha1.ClusterReservationVolumeStatus,
) map[string]int {
	indexes := make(map[string]int, len(volumes))
	for index, volume := range volumes {
		indexes[volume.SourcePVCName] = index
	}

	return indexes
}
