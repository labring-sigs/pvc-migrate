package app

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
)

func validateMoveObject(object *v1alpha1.Move, storageNamespace string) error {
	if object == nil || object.Name == "" || object.Namespace != "" || storageNamespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"cluster-scoped workflow name and storage namespace are required",
		)
	}

	if err := validateMoveStatus(&object.Status, storageNamespace); err != nil {
		return err
	}

	if object.DeletionTimestamp != nil {
		return nil
	}

	if object.Status.ObservedGeneration != 0 &&
		object.Status.ObservedGeneration != object.Generation {
		return domain.NewError(domain.ErrorConflict, "move", "workflow spec changed after planning")
	}

	return validateMoveSpec(object.Spec, object.Status.Plan, storageNamespace)
}

func validateMoveSpec(
	spec v1alpha1.MoveSpec,
	plan *v1alpha1.MovePlan,
	storageNamespace string,
) error {
	destination := spec.SourcePVC.Name
	if spec.DestinationPVC != nil && spec.DestinationPVC.Name != "" {
		destination = spec.DestinationPVC.Name
	}

	if spec.SourceNamespace == "" || spec.DestinationNamespace == "" || spec.SourcePVC.Name == "" ||
		(spec.SourceNamespace == spec.DestinationNamespace && spec.SourcePVC.Name == destination) {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"distinct source and destination PVC identities are required",
		)
	}

	if spec.SessionNamespace != "" && string(spec.SessionNamespace) != storageNamespace {
		return domain.NewError(
			domain.ErrorConflict,
			"move",
			"storage namespace differs from spec.sessionNamespace",
		)
	}

	if plan == nil {
		return nil
	}

	if plan.SourceNamespace != spec.SourceNamespace ||
		plan.DestinationNamespace != spec.DestinationNamespace {
		return domain.NewError(
			domain.ErrorConflict,
			"move",
			"plan namespaces differ from the requested namespaces",
		)
	}

	identity := plan.Identity
	if identity.SourcePVC.Name != spec.SourcePVC.Name ||
		identity.DestinationPVC.Name != destination {
		return domain.NewError(
			domain.ErrorConflict,
			"move",
			"plan endpoints differ from the requested PVC identities",
		)
	}

	if spec.SourcePVC.UID != "" && spec.SourcePVC.UID != identity.SourcePVC.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"move",
			"planned source PVC differs from the requested identity",
		)
	}

	if expected := spec.SourcePV; expected != nil &&
		(expected.Name != identity.SourcePV.Name || (expected.UID != "" && expected.UID != identity.SourcePV.UID)) {
		return domain.NewError(
			domain.ErrorConflict,
			"move",
			"planned PV differs from the requested identity",
		)
	}

	if expected := spec.DestinationPVC; expected != nil && expected.UID != "" &&
		expected.UID != identity.DestinationPVC.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"move",
			"planned destination PVC differs from the requested identity",
		)
	}

	return nil
}

func validateMoveStatus(status *v1alpha1.MoveStatus, storageNamespace string) error {
	if status.Phase == "" && status.Plan == nil &&
		status.Activation == (v1alpha1.MoveActivationStatus{}) {
		return nil
	}

	if err := domain.ValidateIdentityLifecycle(
		status.WorkflowStatus,
		domain.PhaseMoving,
	); err != nil {
		return err
	}

	phase := workflowResumePhase(status.WorkflowStatus)

	plan := status.Plan
	if plan == nil {
		if status.Activation != (v1alpha1.MoveActivationStatus{}) ||
			(phase != domain.PhasePlanned && phase != domain.PhaseAborted) {
			return domain.NewError(
				domain.ErrorValidation,
				"move",
				"execution requires a persisted plan",
			)
		}

		return nil
	}

	if plan.SourceNamespace == "" || plan.DestinationNamespace == "" ||
		string(plan.SessionNamespace) != storageNamespace {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"plan must retain source, destination and storage namespaces",
		)
	}

	identity := plan.Identity
	if identity.SourcePVC.Name == "" || identity.DestinationPVC.Name == "" ||
		(plan.SourceNamespace == plan.DestinationNamespace && identity.SourcePVC.Name == identity.DestinationPVC.Name) {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"planned PVC identities must be distinct",
		)
	}

	if identity.SourcePVC.UID == "" || identity.SourcePV.Name == "" ||
		identity.SourcePV.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"plan and checkpoint do not match the source identity",
		)
	}

	if identity.SourceTemplate.ReclaimPolicy != corev1.PersistentVolumeReclaimRetain &&
		identity.SourceTemplate.ReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"source reclaim policy must be recorded",
		)
	}

	return validateMoveActivation(phase, plan, status.Activation.ActivePVC)
}

func validateMoveActivation(
	phase v1alpha1.WorkflowPhase,
	plan *v1alpha1.MovePlan,
	active *v1alpha1.ObjectReference,
) error {
	if active == nil {
		if phase == domain.PhaseCompleted || phase == domain.PhaseRollingBack ||
			phase == domain.PhaseRolledBack {
			return domain.NewError(
				domain.ErrorValidation,
				"move",
				"completed move requires an active PVC checkpoint",
			)
		}

		return nil
	}

	if active.UID == "" || active.Name == "" || active.Namespace == "" {
		return domain.NewError(domain.ErrorValidation, "move", "active PVC identity is incomplete")
	}

	source := active.Name == plan.Identity.SourcePVC.Name &&
		active.Namespace == string(plan.SourceNamespace)

	destination := active.Name == plan.Identity.DestinationPVC.Name &&
		active.Namespace == string(plan.DestinationNamespace)
	if !source && !destination {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"active PVC is outside the planned endpoints",
		)
	}

	if (phase == domain.PhaseCompleted || phase == domain.PhaseRollingBack) && !destination {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"completed move requires the destination PVC checkpoint",
		)
	}

	if phase == domain.PhaseRolledBack && !source {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"completed rollback requires the source PVC checkpoint",
		)
	}

	return nil
}
