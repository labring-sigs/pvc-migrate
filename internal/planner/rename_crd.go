package planner

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// PlanRename resolves a Rename spec into its controller-owned execution plan.
// Storage location affects permission and quota checks, not the API object.
func (p *Planner) PlanRename(
	ctx context.Context,
	object *v1alpha1.Rename,
	storageNamespace string,
) (*domain.PVCIdentityReport, error) {
	if object == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan rename",
			"Rename object is required",
		)
	}

	if object.DeletionTimestamp != nil {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan rename",
			"a deleting workflow cannot be planned",
		)
	}
	// A resolved plan records the identities used by destructive operations.
	// Replanning must never replace those identities after execution starts.
	status := object.Status.WorkflowStatus
	if object.Status.Plan != nil ||
		object.Status.Activation != (v1alpha1.RenameActivationStatus{}) ||
		(status.Phase != "" && status.Phase != domain.PhasePlanned &&
			(status.Phase != domain.PhaseFailed || status.ResumeFrom != domain.PhasePlanned)) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan rename",
			"only an unplanned workflow can be planned",
		)
	}

	report := newPVCIdentityReport(
		"RenamePlan",
		object.Name,
		storageNamespace,
		object.Namespace,
		object.Namespace,
	)

	resolved := p.planPVCRebind(
		ctx,
		report,
		object.Spec.SourcePVC.Name,
		object.Spec.DestinationPVC.Name,
	)
	if !resolved.Ready {
		return &resolved.PVCIdentityReport, nil
	}

	identity := resolved.identity
	if err := checkReference(
		&object.Spec.SourcePVC,
		identity.SourcePVC,
	); err != nil {
		return nil, err
	}

	if err := checkReference(
		object.Spec.SourcePV,
		identity.SourcePV,
	); err != nil {
		return nil, err
	}

	if err := checkReference(
		&object.Spec.DestinationPVC,
		identity.DestinationPVC,
	); err != nil {
		return nil, err
	}

	object.Status.Plan = &v1alpha1.RenamePlan{PVCIdentityFields: *identity.DeepCopy()}

	return &resolved.PVCIdentityReport, nil
}

func localPlanningReference(ref v1alpha1.ObjectReference) v1alpha1.LocalResourceReference {
	return v1alpha1.LocalResourceReference{
		APIVersion:      ref.APIVersion,
		Kind:            ref.Kind,
		Name:            ref.Name,
		UID:             ref.UID,
		ResourceVersion: ref.ResourceVersion,
	}
}
