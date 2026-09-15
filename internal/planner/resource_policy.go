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
		state.options.Strategies,
		state.options.SourceNamespace, state.options.StagingNamespace,
		state.mountTopologyConflict,
	)
	state.options.Strategies = filtered

	if len(filtered) == 0 {
		state.plan.AddCheck(failed(
			domain.CheckNameStrategy,
			"no selected pv-migrate strategy can handle the requested source and destination",
		))
	} else if state.autoStrategyRequested {
		state.plan.AddCheck(passed(
			domain.CheckNameStrategySelection,
			"auto selected strategy order: "+strings.Join(filtered, ","),
		))
	}
}

func (p *Planner) finalizePlanResources(
	ctx context.Context,
	state *planState,
	chartEstimates map[string]domain.ResourceEstimate,
	probePodPeaks map[string]int,
) {
	state.plan.Volumes = state.plannedVolumes

	estimates := migrationNamespaceResourceEstimates(state, chartEstimates, probePodPeaks)
	if p.controllerSubmission {
		session := estimates[state.options.SessionNamespace]
		session.ConfigMaps--
		estimates[state.options.SessionNamespace] = session
	}

	state.plan.TemporaryUsage = estimates[state.options.StagingNamespace]

	state.plan.RollbackRetention.StorageRequests = state.rollbackStorage.String()
	state.plan.RollbackRetention.PVCs = len(state.plannedVolumes)

	for class, quantity := range state.rollbackByClass {
		state.plan.RollbackRetention.ByStorageClass[class] = quantity.String()
		state.plan.RollbackRetention.PVCsByStorageClass[class] = state.rollbackPVCsByClass[class]
	}

	if len(state.plannedVolumes) == 0 {
		return
	}

	p.runPlanPolicyChecks(ctx, state, estimates)
}

func migrationNamespaceResourceEstimates(
	state *planState,
	chartEstimates map[string]domain.ResourceEstimate,
	probePodPeaks map[string]int,
) map[string]domain.ResourceEstimate {
	options := state.options
	volumeCount := len(state.plannedVolumes)
	estimates := map[string]domain.ResourceEstimate{}

	staging := chartEstimates[options.StagingNamespace]

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
		source := chartEstimates[options.SourceNamespace]

		initializeResourceEstimateMaps(&source)
		estimates[options.SourceNamespace] = source
	}

	for namespace, terminatingPods := range probePodPeaks {
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

func transferChartResourceEstimates(
	sourceNamespace, destinationNamespace string,
	strategies []string,
	volumeCount int,
) map[string]domain.ResourceEstimate {
	if volumeCount == 0 {
		return nil
	}

	sameNamespace := sourceNamespace == destinationNamespace

	estimates := map[string]domain.ResourceEstimate{
		destinationNamespace: kube.PVMigrateResourceEstimate(strategies, sameNamespace, true),
	}
	if !sameNamespace {
		estimates[sourceNamespace] = kube.PVMigrateResourceEstimate(strategies, false, false)
	}

	return estimates
}

func destinationToolProbePods(targetNode string, volumes []domain.PlannedVolume) map[string]int {
	stage := map[string]int{}
	if targetNode == "" {
		return stage
	}

	for _, volume := range volumes {
		stage[volume.DestinationPVC.Namespace] = 1
	}

	return stage
}

func transferProbePods(
	targetNode string,
	strategies []string,
	volumes []domain.PlannedVolume,
	probeSourceMount bool,
) map[string]int {
	stage := destinationToolProbePods(targetNode, volumes)

	needsSource := probeSourceMount || slices.ContainsFunc(
		strategies,
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

	return stage
}

func sourcePathProbePods(volumes []domain.PlannedVolume) map[string]int {
	stage := map[string]int{}
	for _, volume := range volumes {
		if domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
			stage[volume.SourcePVC.Namespace]++
		}
	}

	return stage
}

func mergeProbePodPeaks(stages ...map[string]int) map[string]int {
	peaks := map[string]int{}
	for _, stage := range stages {
		for namespace, pods := range stage {
			peaks[namespace] = max(peaks[namespace], pods)
		}
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
		func(result checkRecorder) {
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

		tasks = append(tasks, func(result checkRecorder) {
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

		tasks = append(tasks, func(result checkRecorder) {
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
		func(result checkRecorder) {
			p.checkNetworkPolicies(ctx, result, options.SourceNamespace, options.StagingNamespace)
		},
	)
	runPlanCheckTasks(state.plan, tasks)
}
