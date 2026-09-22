package planner

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlanMove resolves only the resource identities owned by a concrete Move.
func (p *Planner) PlanMove(
	ctx context.Context,
	object *v1alpha1.Move,
	storageNamespace string,
) (*domain.PVCIdentityReport, error) {
	if object == nil || object.Namespace != "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan move",
			"a cluster-scoped Move object is required",
		)
	}

	if object.DeletionTimestamp != nil {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan move",
			"a deleting workflow cannot be planned",
		)
	}

	status := object.Status.WorkflowStatus
	if object.Status.Plan != nil || object.Status.Activation != (v1alpha1.MoveActivationStatus{}) ||
		(status.Phase != "" && status.Phase != domain.PhasePlanned &&
			(status.Phase != domain.PhaseFailed || status.ResumeFrom != domain.PhasePlanned)) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan move",
			"only an unplanned workflow can be planned",
		)
	}

	if configured := string(
		object.Spec.SessionNamespace,
	); configured != "" &&
		configured != storageNamespace {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan move",
			"storage namespace differs from spec.sessionNamespace",
		)
	}

	destination := object.Spec.SourcePVC.Name
	if object.Spec.DestinationPVC != nil && object.Spec.DestinationPVC.Name != "" {
		destination = object.Spec.DestinationPVC.Name
	}

	report := newPVCIdentityReport("MovePlan", object.Name, storageNamespace,
		string(object.Spec.SourceNamespace), string(object.Spec.DestinationNamespace))
	if object.Spec.DestinationNamespace == "" {
		report.AddCheck(failed(domain.CheckNameMove, "destination namespace is required"))
		return &report, nil
	}

	if _, err := p.client.CoreV1().
		Namespaces().
		Get(ctx, string(object.Spec.DestinationNamespace), metav1.GetOptions{}); err != nil {
		report.AddCheck(
			failed(
				domain.CheckNameDestinationNamespace,
				fmt.Sprintf(
					"read destination namespace %s: %v",
					object.Spec.DestinationNamespace,
					err,
				),
			),
		)

		return &report, nil
	}

	report.AddCheck(
		passed(
			domain.CheckNameDestinationNamespace,
			fmt.Sprintf("destination namespace %s exists", object.Spec.DestinationNamespace),
		),
	)

	resolved := p.planPVCRebind(ctx, report, object.Spec.SourcePVC.Name, destination)
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
		object.Spec.DestinationPVC,
		identity.DestinationPVC,
	); err != nil {
		return nil, err
	}

	object.Status.Plan = &v1alpha1.MovePlan{
		SourceNamespace:      object.Spec.SourceNamespace,
		DestinationNamespace: object.Spec.DestinationNamespace,
		SessionNamespace:     v1alpha1.NamespaceName(storageNamespace),
		Identity: v1alpha1.MoveIdentity{
			SourcePVC:      identity.SourcePVC,
			SourcePV:       identity.SourcePV,
			DestinationPVC: identity.DestinationPVC,
			SourceTemplate: *identity.SourceTemplate.DeepCopy(),
		},
	}

	return &resolved.PVCIdentityReport, nil
}
