package planner

import (
	"context"
	"slices"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (p *Planner) finalizePlanStrategies(state *planState) {
	filtered := filterStrategies(
		state.plan,
		state.options,
		state.mountTopologyConflict,
	)
	state.options.Strategies = filtered

	if len(filtered) == 0 {
		state.plan.AddCheck(failed(
			"strategy",
			"no selected pv-migrate strategy can handle the requested source and destination",
		))
	} else if state.autoStrategyRequested {
		state.plan.AddCheck(passed(
			"strategy-selection",
			"auto selected strategy order: "+strings.Join(filtered, ","),
		))
	}
}

func (p *Planner) finalizePlanResources(ctx context.Context, state *planState) {
	state.plan.Volumes = state.plannedVolumes
	estimates := migrationNamespaceResourceEstimates(state)
	state.plan.TemporaryUsage = estimates[state.options.StagingNamespace]

	state.plan.RollbackRetention.StorageRequests = state.rollbackStorage.String()
	state.plan.RollbackRetention.PVCs = len(state.plannedVolumes)

	for class, quantity := range state.rollbackByClass {
		state.plan.RollbackRetention.ByStorageClass[class] = quantity.String()
		state.plan.RollbackRetention.PVCsByStorageClass[class] = state.rollbackPVCsByClass[class]
	}

	if state.options.Operation == domain.OperationMigrate ||
		state.options.Operation == domain.OperationMigratePod {
		p.checkActivationPVCPolicies(ctx, state.plan, state.volumeSpecs)
	}

	if len(state.plannedVolumes) == 0 {
		return
	}

	p.runPlanPolicyChecks(ctx, state, estimates)
}

func migrationNamespaceResourceEstimates(
	state *planState,
) map[string]domain.ResourceEstimate {
	options := state.options
	volumeCount := len(state.plannedVolumes)
	estimates := map[string]domain.ResourceEstimate{}

	staging := domain.ResourceEstimate{}
	if volumeCount > 0 {
		staging = migrationChartResourceEstimate(
			options.Operation,
			options.Strategies,
			options.SourceNamespace == options.StagingNamespace,
			true,
		)
	}

	staging.StorageRequests = state.totalStorage.String()
	staging.PVCs = volumeCount
	staging.ByStorageClass = map[string]string{}
	staging.PVCsByStorageClass = map[string]int{}

	for class, quantity := range state.storageByClass {
		staging.ByStorageClass[class] = quantity.String()
		staging.PVCsByStorageClass[class] = state.pvcsByClass[class]
	}

	estimates[options.StagingNamespace] = staging

	if options.SourceNamespace != options.StagingNamespace {
		source := domain.ResourceEstimate{}
		if volumeCount > 0 {
			source = migrationChartResourceEstimate(
				options.Operation,
				options.Strategies,
				false,
				false,
			)
		}

		initializeResourceEstimateMaps(&source)
		estimates[options.SourceNamespace] = source
	}

	for namespace, terminatingPods := range migrationProbePodPeaks(
		options,
		state.plannedVolumes,
	) {
		estimate := estimates[namespace]
		initializeResourceEstimateMaps(&estimate)
		estimate.TerminatingPods = terminatingPods
		estimate.Pods = max(estimate.Pods, terminatingPods)
		estimates[namespace] = estimate
	}

	// Reservation consumers are created one volume at a time and do not set an
	// active deadline, so one destination Pod is the NotTerminating peak.
	if volumeCount > 0 {
		estimate := estimates[options.StagingNamespace]
		estimate.NotTerminatingPods = max(estimate.NotTerminatingPods, 1)
		estimate.Pods = max(estimate.Pods, estimate.NotTerminatingPods)
		estimates[options.StagingNamespace] = estimate
	}

	session := estimates[options.SessionNamespace]
	initializeResourceEstimateMaps(&session)
	session.ConfigMaps++
	session.Leases++
	estimates[options.SessionNamespace] = session

	return estimates
}

func migrationChartResourceEstimate(
	operation domain.Operation,
	strategies []string,
	sameNamespace bool,
	destinationSide bool,
) domain.ResourceEstimate {
	switch operation {
	case domain.OperationCopy, domain.OperationMigrate, domain.OperationMigratePod:
		return kube.PVMigrateResourceEstimate(
			strategies,
			sameNamespace,
			destinationSide,
		)
	default:
		return domain.ResourceEstimate{}
	}
}

func migrationProbePodPeaks(
	options planOptions,
	volumes []domain.PlannedVolume,
) map[string]int {
	peaks := map[string]int{}
	addStage := func(stage map[string]int) {
		for namespace, pods := range stage {
			peaks[namespace] = max(peaks[namespace], pods)
		}
	}
	addDestinationBase := func(stage map[string]int) {
		if options.TargetNode == "" {
			return
		}

		seen := map[string]struct{}{}
		for _, volume := range volumes {
			namespace := volume.DestinationPVC.Namespace
			if _, exists := seen[namespace]; exists {
				continue
			}

			seen[namespace] = struct{}{}
			stage[namespace]++
		}
	}
	addCopyStage := func(mountSourcePVC bool) {
		stage := map[string]int{}
		addDestinationBase(stage)

		needsSource := mountSourcePVC || slices.ContainsFunc(
			options.Strategies,
			func(strategy string) bool { return strategy != domain.StrategyMount },
		)
		for _, volume := range volumes {
			if needsSource {
				stage[volume.SourcePVC.Namespace]++
			}

			if domain.DestinationTransferPath(volume.TransferScope) != domain.VolumeRootPath {
				stage[volume.DestinationPVC.Namespace]++
			}
		}

		addStage(stage)
	}

	reservation := map[string]int{}
	addDestinationBase(reservation)
	addStage(reservation)

	switch options.Operation {
	case domain.OperationCopy:
		addCopyStage(true)
	case domain.OperationMigrate:
		addCopyStage(false)
	case domain.OperationMigratePod:
		if options.PrecopyPasses > 0 {
			addCopyStage(true)
		}

		addCopyStage(false)
	}

	if options.Operation == domain.OperationMigrate || options.Operation == domain.OperationMigratePod {
		sourcePathStage := map[string]int{}
		for _, volume := range volumes {
			if domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
				sourcePathStage[volume.SourcePVC.Namespace]++
			}
		}

		addStage(sourcePathStage)
	}

	return peaks
}

func initializeResourceEstimateMaps(estimate *domain.ResourceEstimate) {
	if estimate.StorageRequests == "" {
		estimate.StorageRequests = "0"
	}

	if estimate.ByStorageClass == nil {
		estimate.ByStorageClass = map[string]string{}
	}

	if estimate.PVCsByStorageClass == nil {
		estimate.PVCsByStorageClass = map[string]int{}
	}
}

func (p *Planner) runPlanPolicyChecks(
	ctx context.Context,
	state *planState,
	estimates map[string]domain.ResourceEstimate,
) {
	options := state.options
	p.logInfo(
		"validating migration cluster policies",
		"session", options.SessionID,
		"sourceNamespace", options.SourceNamespace,
		"stagingNamespace", options.StagingNamespace,
		"sessionNamespace", options.SessionNamespace,
		"volumes", len(state.plannedVolumes),
	)

	staging := estimates[options.StagingNamespace]

	tasks := []planCheckTask{
		func(result *domain.MigrationPlan) {
			p.checkNamespaceResourcePolicies(
				ctx,
				result,
				options.StagingNamespace,
				state.plannedVolumes,
				staging,
			)
		},
	}
	if options.SourceNamespace != options.StagingNamespace {
		source := estimates[options.SourceNamespace]

		tasks = append(tasks, func(result *domain.MigrationPlan) {
			p.checkNamespaceResourcePolicies(
				ctx,
				result,
				options.SourceNamespace,
				nil,
				source,
			)
		})
	}

	if options.SessionNamespace != options.StagingNamespace &&
		options.SessionNamespace != options.SourceNamespace {
		session := estimates[options.SessionNamespace]

		tasks = append(tasks, func(result *domain.MigrationPlan) {
			p.checkNamespaceResourcePolicies(
				ctx,
				result,
				options.SessionNamespace,
				nil,
				session,
			)
		})
	}

	tasks = append(tasks,
		func(result *domain.MigrationPlan) {
			p.checkNetworkPolicies(ctx, result, options.SourceNamespace, options.StagingNamespace)
		},
		func(result *domain.MigrationPlan) {
			p.checkRBAC(
				ctx,
				result,
				options.SourceNamespace,
				options.StagingNamespace,
				options.SessionNamespace,
				state.workload,
				state.inspectOpenEBSShared,
				state.patchOpenEBSShared,
			)
		},
	)
	if state.sourcePod != nil {
		tasks = append(tasks, func(result *domain.MigrationPlan) {
			p.checkPodDependencies(ctx, result, state.sourcePod)
		})
	}

	runPlanCheckTasks(state.plan, tasks)

	if state.sourcePod != nil {
		for _, issue := range podMigrationIssues(
			state.sourcePod.Spec,
			options.SourceNode,
			options.TargetNode,
		) {
			state.plan.AddCheck(failed("pod-scheduling", issue))
		}
	}
}
