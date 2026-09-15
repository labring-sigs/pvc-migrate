package planner

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

func (p *Planner) PlanReserve(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
	image string,
) (*domain.TransferPlan, error) {
	if object == nil || object.Name == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan reservation",
			"reservation name is required",
		)
	}

	status := object.Status.WorkflowStatus
	if object.DeletionTimestamp != nil || object.Status.Plan != nil ||
		len(object.Status.Volumes) != 0 ||
		(status.Phase != "" && status.Phase != domain.PhasePlanned &&
			(status.Phase != domain.PhaseFailed || status.ResumeFrom != domain.PhasePlanned)) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan reservation",
			"only an unplanned reservation can be planned",
		)
	}

	spec := object.Spec.DeepCopy()
	if spec.SessionNamespace == "" {
		spec.SessionNamespace = spec.SourceNamespace
	}

	plan, resolved, err := p.resolveReservation(
		ctx,
		object.Name,
		spec.ReservationSpec,
		string(
			spec.SourceNamespace,
		),
		string(spec.DestinationNamespace),
		string(spec.SessionNamespace),
		image,
	)
	if err != nil {
		return nil, err
	}

	clusterPlan := v1alpha1.ClusterReservationPlan{
		ReservationPlan:      *resolved.DeepCopy(),
		SourceNamespace:      spec.SourceNamespace,
		DestinationNamespace: spec.DestinationNamespace,
		SessionNamespace:     spec.SessionNamespace,
	}
	if plan.Ready {
		object.Status.Plan = &clusterPlan
	}

	return plan, nil
}

// PlanReservation plans the namespaced API directly. Its namespace comes from
// metadata and is never reconstructed from a cluster workflow or legacy session.
func (p *Planner) PlanReservation(
	ctx context.Context,
	object *v1alpha1.Reservation,
	image string,
) (*domain.TransferPlan, error) {
	if object == nil || object.Name == "" || object.Namespace == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan reservation",
			"reservation name and namespace are required",
		)
	}

	status := object.Status.WorkflowStatus
	if object.DeletionTimestamp != nil || object.Status.Plan != nil ||
		len(object.Status.Volumes) != 0 ||
		(status.Phase != "" && status.Phase != domain.PhasePlanned &&
			(status.Phase != domain.PhaseFailed || status.ResumeFrom != domain.PhasePlanned)) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan reservation",
			"only an unplanned reservation can be planned",
		)
	}

	plan, resolved, err := p.resolveReservation(ctx, object.Name, *object.Spec.DeepCopy(),
		object.Namespace, object.Namespace, object.Namespace, image)
	if err != nil {
		return nil, err
	}

	if plan.Ready {
		object.Status.Plan = resolved.DeepCopy()
	}

	return plan, nil
}

func (p *Planner) resolveReservation(
	ctx context.Context,
	name string,
	spec v1alpha1.ReservationSpec,
	sourceNamespace, destinationNamespace, sessionNamespace, image string,
) (*domain.TransferPlan, *v1alpha1.ReservationPlan, error) {
	options := planOptions{
		SessionID:            name,
		SourceNamespace:      sourceNamespace,
		DestinationNamespace: destinationNamespace,
		TemporaryNamespace:   destinationNamespace,
		StagingNamespace:     destinationNamespace,
		SessionNamespace:     sessionNamespace,
		ToolImage:            image,
		operationKind:        domain.OperationReserve,
	}

	if err := domain.ValidateReclaimPolicies("", spec.DestinationPVCReclaimPolicy); err != nil {
		return nil, nil, err
	}

	state := p.newTransferPlanState(options, spec.TransferOptions, spec.Volumes)
	if _, err := p.selectPlanVolumes(ctx, &state, spec.Volumes, spec.Pod); err != nil {
		return nil, nil, err
	}

	if err := p.loadPlanContext(ctx, &state); err != nil {
		return nil, nil, err
	}

	inputs := p.planVolumes(ctx, &state, p.loadPlanVolumeInputs(ctx, &state))
	recordTransferScopeChecks(
		state.plan,
		state.options.SourceNamespace,
		state.volumeSpecs,
		domain.SeverityWarning,
	)
	checkReservationPlanConsumers(
		state.plan,
		inputs,
		state.inventory.namespacePods,
		state.inventory.namespacePodsErr,
	)

	p.selectPlanTarget(&state, v1alpha1.WorkloadNone, nil, "")

	plan, err := p.completeTransferPlan(ctx, &state, spec.Volumes)
	if err != nil {
		return nil, nil, err
	}

	p.finalizePlanResources(ctx, &state, nil, destinationToolProbePods(
		state.options.TargetNode, state.plannedVolumes,
	))

	resolved := v1alpha1.ReservationPlan{
		DestinationPVCReclaimPolicy: state.options.DestinationPVCReclaimPolicy,
		Volumes:                     state.volumeSpecs,
		SourceNode:                  state.options.SourceNode,
		TargetNode:                  state.options.TargetNode,
		ToolImage:                   state.options.ToolImage,
		Strategies:                  state.options.Strategies,
		VerifyChecksum:              state.options.VerifyChecksum,
		DeleteExtraneous:            state.options.DeleteExtraneous,
		SkipSourceUsageCheck:        state.options.SkipSourceUsageCheck,
	}
	if len(resolved.Volumes) > 0 {
		p.checkReservationPermissions(
			ctx,
			plan,
			name,
			sourceNamespace,
			destinationNamespace,
			sessionNamespace,
		)
	}

	return plan, &resolved, nil
}

func checkReservationPlanConsumers(
	plan checkRecorder,
	inputs []planVolumeInput,
	pods []corev1.Pod,
	listErr error,
) {
	for _, input := range inputs {
		consumers, listed := collectPVCConsumers(
			plan,
			input.pvc,
			pods,
			listErr,
			kube.ActivePodUsesPVC,
		)
		if listed {
			checkReservationConsumers(plan, input.pvc, consumers)
		}
	}
}
