package planner

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

func (p *Planner) PlanOfflineMigration(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
	image string,
) (*domain.TransferPlan, error) {
	if object == nil || object.Name == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan migration",
			"migration name is required",
		)
	}

	spec := object.Spec.DeepCopy()
	if spec.TemporaryNamespace == "" {
		spec.TemporaryNamespace = spec.SourceNamespace
	}

	if spec.SessionNamespace == "" {
		spec.SessionNamespace = spec.SourceNamespace
	}

	if spec.DestinationNamespace == "" {
		spec.DestinationNamespace = spec.SourceNamespace
	}

	if !migrationCanPlan(
		object.Status.WorkflowStatus,
		object.Status.Plan != nil,
		len(object.Status.Volumes),
		object.DeletionTimestamp != nil,
	) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan migration",
			"only an unplanned migration can be planned",
		)
	}

	plan, resolved, err := p.resolveMigration(
		ctx,
		object.Name,
		spec.MigrationSpec,
		string(
			spec.SourceNamespace,
		),
		string(spec.DestinationNamespace),
		string(spec.TemporaryNamespace),
		string(spec.SessionNamespace),
		image,
	)
	if err != nil {
		return nil, err
	}

	clusterPlan := v1alpha1.ClusterMigrationPlan{
		SourceNamespace:      spec.SourceNamespace,
		DestinationNamespace: spec.DestinationNamespace,
		TemporaryNamespace:   spec.TemporaryNamespace,
		SessionNamespace:     spec.SessionNamespace,
		UnusedStoragePolicy:  resolved.UnusedStoragePolicy,
		Volumes:              resolved.Volumes,
		SourceNode:           resolved.SourceNode,
		TargetNode:           resolved.TargetNode,
		ToolImage:            resolved.ToolImage,
		Strategies:           resolved.Strategies,
		VerifyChecksum:       resolved.VerifyChecksum,
		DeleteExtraneous:     resolved.DeleteExtraneous,
		SkipSourceUsageCheck: resolved.SkipSourceUsageCheck,
	}
	if plan.Ready {
		object.Status.Plan = clusterPlan.DeepCopy()
	}

	return plan, nil
}

// PlanNamespacedMigration discovers storage directly into the Migration CRD status.
func (p *Planner) PlanNamespacedMigration(
	ctx context.Context,
	object *v1alpha1.Migration,
	image string,
) (*domain.TransferPlan, error) {
	if object == nil || object.Name == "" || object.Namespace == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan migration",
			"migration name and namespace are required",
		)
	}

	if !migrationCanPlan(
		object.Status.WorkflowStatus,
		object.Status.Plan != nil,
		len(object.Status.Volumes),
		object.DeletionTimestamp != nil,
	) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan migration",
			"only an unplanned migration can be planned",
		)
	}

	plan, resolved, err := p.resolveMigration(
		ctx,
		object.Name,
		*object.Spec.DeepCopy(),
		object.Namespace,
		object.Namespace,
		object.Namespace,
		object.Namespace,
		image,
	)
	if err != nil {
		return nil, err
	}

	if plan.Ready {
		object.Status.Plan = resolved.DeepCopy()
	}

	return plan, nil
}

func migrationCanPlan(
	status v1alpha1.WorkflowStatus,
	planned bool,
	checkpoints int,
	deleting bool,
) bool {
	return !planned && checkpoints == 0 && !deleting &&
		(status.Phase == "" || status.Phase == domain.PhasePlanned ||
			(status.Phase == domain.PhaseFailed && status.ResumeFrom == domain.PhasePlanned))
}

func (p *Planner) resolveMigration(ctx context.Context, name string, spec v1alpha1.MigrationSpec,
	sourceNamespace, destinationNamespace, temporaryNamespace, sessionNamespace, image string,
) (*domain.TransferPlan, *v1alpha1.MigrationPlan, error) {
	options := transferInput{
		SessionID:            name,
		SourceNamespace:      sourceNamespace,
		DestinationNamespace: destinationNamespace,
		TemporaryNamespace:   temporaryNamespace,
		StagingNamespace:     temporaryNamespace,
		SessionNamespace:     sessionNamespace,
		ToolImage:            image,
	}

	if err := domain.ValidateUnusedStoragePolicy(spec.UnusedStoragePolicy); err != nil {
		return nil, nil, err
	}

	state := p.newTransferPlanState(
		options,
		domain.OperationMigrate,
		spec.TransferOptions,
		spec.Volumes,
	)
	if _, err := p.selectPlanVolumes(ctx, &state, spec.Volumes, nil); err != nil {
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

	for _, input := range inputs {
		p.checkPVCFinalizers(state.plan, input.pvc)
	}

	checkOfflineMigrationPlanConsumers(
		state.plan,
		inputs,
		state.inventory.namespacePods,
		state.inventory.namespacePodsErr,
		p.presentation(),
	)

	p.selectPlanTarget(ctx, &state, v1alpha1.WorkloadNone, nil, "")

	plan, err := p.completeTransferPlan(ctx, &state, spec.Volumes)
	if err != nil {
		return nil, nil, err
	}

	p.checkActivationPVCPolicies(ctx, plan, state.options.DestinationNamespace, state.volumeSpecs)
	p.finalizePlanResources(ctx, &state, transferChartResourceEstimates(
		state.options.SourceNamespace, state.options.StagingNamespace,
		state.options.Strategies, len(state.plannedVolumes),
	), migrationProbePodPeaks(
		state.options.TargetNode, state.options.Strategies, state.plannedVolumes,
	))

	resolved := v1alpha1.MigrationPlan{
		UnusedStoragePolicy:  state.options.UnusedStoragePolicy,
		Volumes:              state.volumeSpecs,
		SourceNode:           state.options.SourceNode,
		TargetNode:           state.options.TargetNode,
		ToolImage:            state.options.ToolImage,
		Strategies:           state.options.Strategies,
		VerifyChecksum:       state.options.VerifyChecksum,
		DeleteExtraneous:     state.options.DeleteExtraneousValue(),
		SkipSourceUsageCheck: state.options.SkipSourceUsageCheck,
	}
	if len(resolved.Volumes) > 0 {
		p.checkMigrationPermissions(
			ctx,
			plan,
			name,
			sourceNamespace,
			destinationNamespace,
			temporaryNamespace,
			sessionNamespace,
			resolved.Strategies,
		)
	}

	return plan, &resolved, nil
}

func migrationProbePodPeaks(
	targetNode string,
	strategies []string,
	volumes []domain.PlannedVolume,
) map[string]int {
	return mergeProbePodPeaks(
		transferProbePods(targetNode, strategies, volumes, false),
		sourcePathProbePods(volumes),
	)
}

func checkOfflineMigrationPlanConsumers(
	plan checkRecorder,
	inputs []planVolumeInput,
	pods []corev1.Pod,
	listErr error,
	presentation domain.Presentation,
) {
	activeNames := []string{}

	terminalNames := []string{}
	for _, input := range inputs {
		consumers, listed := collectPVCConsumers(
			plan,
			input.pvc,
			pods,
			listErr,
			kube.PodPreventsSafePVCDeletion,
		)
		if !listed {
			continue
		}

		if len(consumers) == 0 {
			checkOfflinePVC(plan, input.pvc)
		}

		for _, consumer := range consumers {
			if consumer.Status.Phase == corev1.PodSucceeded ||
				consumer.Status.Phase == corev1.PodFailed {
				terminalNames = append(
					terminalNames,
					fmt.Sprintf("%s (phase %s)", consumer.Name, consumer.Status.Phase),
				)

				continue
			}

			activeNames = append(activeNames, consumer.Name)
		}
	}

	if len(activeNames) == 0 && len(terminalNames) == 0 {
		return
	}

	clauses := []string{}
	if len(activeNames) > 0 {
		sort.Strings(activeNames)
		activeNames = slices.Compact(activeNames)

		alternative := "or use a PodMigration workflow to select a workload that can be paused before final sync"
		if presentation == domain.PresentationCLI {
			alternative = "or use the separate migrate-pod command to select a workload that pvc-migrate can pause before final sync"
		}

		clauses = append(clauses, fmt.Sprintf(
			"active Pod consumer(s) %s; stop them before offline migration, %s",
			strings.Join(activeNames, ","),
			alternative,
		))
	}

	if len(terminalNames) > 0 {
		sort.Strings(terminalNames)
		terminalNames = slices.Compact(terminalNames)

		// Terminal Pods no longer write, but a scheduled one still holds the
		// PVC protection boundary, so only deleting the Pod object releases
		// the claim.
		clauses = append(clauses, fmt.Sprintf(
			"terminal Pod(s) %s still hold the PVC protection boundary; delete the finished Pod objects before offline migration",
			strings.Join(terminalNames, ","),
		))
	}

	plan.AddCheck(failed(domain.CheckNamePVCConsumers,
		"offline migrate found "+strings.Join(clauses, "; "),
	))
}
