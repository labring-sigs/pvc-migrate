package planner

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (p *Planner) PlanCopy(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
	image string,
) (*domain.TransferPlan, error) {
	if object == nil || object.Name == "" {
		return nil, domain.NewError(domain.ErrorValidation, "plan copy", "copy name is required")
	}

	status := object.Status.WorkflowStatus
	if object.DeletionTimestamp != nil || object.Status.Plan != nil ||
		len(object.Status.Volumes) != 0 || object.Status.SourceNode != "" ||
		(status.Phase != "" && status.Phase != domain.PhasePlanned &&
			(status.Phase != domain.PhaseFailed || status.ResumeFrom != domain.PhasePlanned)) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan copy",
			"only an unplanned copy can be planned",
		)
	}

	request := object.Spec.DeepCopy()
	if request.SessionNamespace == "" {
		request.SessionNamespace = request.SourceNamespace
	}

	plan, resolved, err := p.resolveCopy(
		ctx,
		object.Name,
		request.CopySpec,
		string(
			request.SourceNamespace,
		),
		string(request.DestinationNamespace),
		string(request.SessionNamespace),
		image,
	)
	if err != nil {
		return nil, err
	}

	clusterPlan := v1alpha1.ClusterCopyPlan{
		CopyPlan:             *resolved.DeepCopy(),
		SourceNamespace:      request.SourceNamespace,
		DestinationNamespace: request.DestinationNamespace,
		SessionNamespace:     request.SessionNamespace,
	}
	if plan.Ready {
		object.Status.Plan = &clusterPlan
	}

	return plan, nil
}

// PlanNamespacedCopy resolves a Copy directly in its metadata namespace.
func (p *Planner) PlanNamespacedCopy(
	ctx context.Context,
	object *v1alpha1.Copy,
	image string,
) (*domain.TransferPlan, error) {
	if object == nil || object.Name == "" || object.Namespace == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan copy",
			"copy name and namespace are required",
		)
	}

	status := object.Status.WorkflowStatus
	if object.DeletionTimestamp != nil || object.Status.Plan != nil ||
		len(object.Status.Volumes) != 0 || object.Status.SourceNode != "" ||
		(status.Phase != "" && status.Phase != domain.PhasePlanned &&
			(status.Phase != domain.PhaseFailed || status.ResumeFrom != domain.PhasePlanned)) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan copy",
			"only an unplanned copy can be planned",
		)
	}

	plan, resolved, err := p.resolveCopy(ctx, object.Name, *object.Spec.DeepCopy(),
		object.Namespace, object.Namespace, object.Namespace, image)
	if err != nil {
		return nil, err
	}

	if plan.Ready {
		object.Status.Plan = resolved.DeepCopy()
	}

	return plan, nil
}

func (p *Planner) resolveCopy(ctx context.Context, name string, spec v1alpha1.CopySpec,
	sourceNamespace, destinationNamespace, sessionNamespace, image string,
) (*domain.TransferPlan, *v1alpha1.CopyPlan, error) {
	options := transferInput{
		SessionID:            name,
		SourceNamespace:      sourceNamespace,
		DestinationNamespace: destinationNamespace,
		TemporaryNamespace:   destinationNamespace,
		SessionNamespace:     sessionNamespace,
		ToolImage:            image,
	}

	if err := domain.ValidateUnusedStoragePolicy(spec.UnusedStoragePolicy); err != nil {
		return nil, nil, err
	}

	state := p.newTransferPlanState(
		options,
		domain.OperationCopy,
		spec.TransferOptions,
		spec.Volumes,
	)
	if _, err := p.selectPlanVolumes(ctx, &state, spec.Volumes, spec.Pod); err != nil {
		return nil, nil, err
	}

	if err := p.loadPlanContext(ctx, &state); err != nil {
		return nil, nil, err
	}

	if state.options.SourceNamespace != state.options.StagingNamespace {
		for index, name := range state.pvcNames {
			if state.destinationPVCs[index] == "" {
				state.destinationPVCs[index] = name
			}
		}
	}

	inputs := p.planVolumes(ctx, &state, p.loadPlanVolumeInputs(ctx, &state))
	recordTransferScopeChecks(
		state.plan,
		state.options.SourceNamespace,
		state.volumeSpecs,
		domain.SeverityInfo,
	)
	inspectShared := p.checkCopyPlanConsumers(ctx, &state, inputs, spec.Online)

	p.selectPlanTarget(&state, v1alpha1.WorkloadNone, nil, "")

	plan, err := p.completeTransferPlan(ctx, &state, spec.Volumes)
	if err != nil {
		return nil, nil, err
	}

	p.finalizePlanResources(ctx, &state, transferChartResourceEstimates(
		state.options.SourceNamespace, state.options.StagingNamespace,
		state.options.Strategies, len(state.plannedVolumes),
	), transferProbePods(
		state.options.TargetNode, state.options.Strategies, state.plannedVolumes, true,
	))

	resolved := v1alpha1.CopyPlan{
		UnusedStoragePolicy:  state.options.UnusedStoragePolicy,
		Volumes:              state.volumeSpecs,
		SourceNode:           state.options.SourceNode,
		TargetNode:           state.options.TargetNode,
		ToolImage:            state.options.ToolImage,
		Strategies:           state.options.Strategies,
		VerifyChecksum:       state.options.VerifyChecksum,
		DeleteExtraneous:     state.options.DeleteExtraneousValue(),
		SkipSourceUsageCheck: state.options.SkipSourceUsageCheck,
		Online:               spec.Online,
	}
	if len(resolved.Volumes) > 0 {
		p.checkCopyPermissions(
			ctx,
			plan,
			name,
			sourceNamespace,
			destinationNamespace,
			sessionNamespace,
			resolved.Strategies,
			inspectShared,
		)
	}

	if spec.Online {
		plan.AddCheck(warned(
			domain.CheckNameCopyMode,
			"online copy performs one finite warm pass with file-level consistency while source Pods may keep writing",
		))
	} else {
		plan.AddCheck(passed(domain.CheckNameCopyMode,
			"offline copy requires every source PVC to have zero active Pod consumers"))
	}

	return plan, &resolved, nil
}

func (p *Planner) checkCopyPlanConsumers(
	ctx context.Context,
	state *planState,
	inputs []planVolumeInput,
	online bool,
) bool {
	nodes := map[string]struct{}{}

	inspectShared := false
	for _, input := range inputs {
		consumers, listed := collectPVCConsumers(
			state.plan, input.pvc, state.inventory.namespacePods, state.inventory.namespacePodsErr,
			kube.ActivePodUsesPVC,
		)
		if !listed {
			continue
		}

		checkCopyConsumers(state.plan, input.pvc, online, consumers, p.guidanceAudience())

		if !online {
			continue
		}

		inspect, _ := p.checkWarmCopyMountCompatibility(
			ctx,
			state.plan,
			domain.OperationCopy,
			false,
			input.pvc,
			input.pv,
			input.sourceClass,
			state.storageClasses[input.sourceClass],
			state.storageClassErrors[input.sourceClass],
			consumers,
		)
		inspectShared = inspectShared || inspect

		for _, consumer := range consumers {
			if consumer.Spec.NodeName != "" {
				nodes[consumer.Spec.NodeName] = struct{}{}
			}
		}
	}

	if !online {
		return inspectShared
	}

	state.options.SourceNode = inferOnlineCopySourceNode(
		state.plan,
		state.options.SourceNode,
		nodes,
	)
	if state.options.SourceNode != "" && state.inventory.sourceNode == nil &&
		state.inventory.sourceNodeErr == nil {
		state.inventory.sourceNode, state.inventory.sourceNodeErr = p.client.CoreV1().Nodes().Get(
			ctx, state.options.SourceNode, metav1.GetOptions{},
		)
	}

	return inspectShared
}
