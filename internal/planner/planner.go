package planner

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"
)

type Options struct {
	SessionID              string
	Operation              domain.Operation
	SourceNamespace        string
	TemporaryNamespace     string
	DestinationNamespace   string
	SessionNamespace       string
	StagingNamespace       string
	ToolImage              string
	CapacityAwareness      domain.CapacityAwareness
	SourcePVCs             []string
	DestinationPVCs        []string
	DestinationCapacities  []string
	SourcePaths            []string
	DestinationPaths       []string
	AllowVolumeShrink      bool
	SkipSourceUsageCheck   bool
	PodName                string
	SourceNode             string
	TargetNode             string
	DestinationClass       string
	Strategies             []string
	Online                 bool
	VerifyChecksum         bool
	DeleteExtraneous       bool
	SwitchoverCandidate    string
	AllowLeaderDowntime    bool
	ForceReprovision       bool
	PrecopyPasses          int
	OpenEBSLVMEnableShared bool
}

type Planner struct {
	client                        kubernetes.Interface
	controllers                   *controller.Manager
	openEBSLVMSharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
	volumeUsageReader             kube.VolumeUsageReader
	logger                        *slog.Logger
}

type planState struct {
	options               Options
	autoStrategyRequested bool
	autoTargetNode        bool
	plan                  *domain.MigrationPlan
	workload              domain.WorkloadSpec
	pvcNames              []string
	sourcePod             *corev1.Pod
	inventory             planInventory
	targetNode            *corev1.Node
	destinationPVCs       []string
	transferScopes        []*domain.TransferScope
	volumeSpecs           []domain.VolumeSpec
	plannedVolumes        []domain.PlannedVolume
	destinationSources    map[string]string
	copyConsumerNodes     map[string]struct{}
	unmanagedConsumers    map[string]struct{}
	sourcePVs             map[string]*corev1.PersistentVolume
	mountTopologyConflict string
	storageClasses        map[string]*storagev1.StorageClass
	storageClassErrors    map[string]error
	requestedCapacities   []string
	totalStorage          resource.Quantity
	rollbackStorage       resource.Quantity
	storageByClass        map[string]resource.Quantity
	rollbackByClass       map[string]resource.Quantity
	pvcsByClass           map[string]int
	rollbackPVCsByClass   map[string]int
	storageClassChanged   bool
	inspectOpenEBSShared  bool
	patchOpenEBSShared    bool
}

func New(client kubernetes.Interface, controllers *controller.Manager) *Planner {
	return &Planner{client: client, controllers: controllers}
}

func (p *Planner) WithOpenEBSLVMSharedVolumeManager(
	manager kube.OpenEBSLVMSharedVolumeManager,
) *Planner {
	p.openEBSLVMSharedVolumeManager = manager
	return p
}

func (p *Planner) WithVolumeUsageReader(reader kube.VolumeUsageReader) *Planner {
	p.volumeUsageReader = reader
	return p
}

// WithLogger enables progress logs for cluster inventory and policy checks.
func (p *Planner) WithLogger(logger *slog.Logger) *Planner {
	p.logger = logger
	return p
}

func (p *Planner) logInfo(message string, args ...any) {
	if p != nil && p.logger != nil {
		p.logger.Info(message, args...)
	}
}

func (p *Planner) Plan(ctx context.Context, options Options) (*domain.MigrationPlan, error) {
	state := newPlanState(p, options)
	p.validatePlanInputs(state.plan, state.options)

	if err := p.discoverPlanWorkload(ctx, &state); err != nil {
		return nil, err
	}

	p.loadPlanContext(ctx, &state)

	p.planVolumes(ctx, &state)
	p.finalizePlan(ctx, &state)

	return state.plan, nil
}

func newPlanState(p *Planner, input Options) planState {
	autoStrategyRequested := len(input.Strategies) == 0 ||
		(len(input.Strategies) == 1 && containsStrategy(input.Strategies, domain.StrategyAuto))
	autoTargetNode := isAutoNode(input.TargetNode)
	options := applyDefaults(input)
	p.logInfo(
		"migration planning started",
		"operation", options.Operation,
		"session", options.SessionID,
		"namespace", options.SourceNamespace,
		"pod", options.PodName,
		"pvcs", len(options.SourcePVCs),
	)

	if autoTargetNode {
		options.TargetNode = ""
	}

	plan := &domain.MigrationPlan{
		APIVersion:           domain.SessionAPIVersion,
		Kind:                 domain.MigrationPlanKind,
		SessionID:            options.SessionID,
		SourceNamespace:      options.SourceNamespace,
		TemporaryNamespace:   options.TemporaryNamespace,
		DestinationNamespace: options.DestinationNamespace,
		SessionNamespace:     options.SessionNamespace,
		ToolImage:            options.ToolImage,
		CapacityAwareness:    options.CapacityAwareness,
		TargetNode:           options.TargetNode,
		Ready:                true,
		TemporaryUsage: domain.ResourceEstimate{
			ByStorageClass:     map[string]string{},
			PVCsByStorageClass: map[string]int{},
		},
		RollbackRetention: domain.ResourceEstimate{
			ByStorageClass:     map[string]string{},
			PVCsByStorageClass: map[string]int{},
		},
	}

	return planState{
		options:               options,
		autoStrategyRequested: autoStrategyRequested,
		autoTargetNode:        autoTargetNode,
		plan:                  plan,
		destinationSources:    make(map[string]string),
		copyConsumerNodes:     make(map[string]struct{}),
		unmanagedConsumers:    make(map[string]struct{}),
		sourcePVs:             make(map[string]*corev1.PersistentVolume),
		storageByClass:        make(map[string]resource.Quantity),
		rollbackByClass:       make(map[string]resource.Quantity),
		pvcsByClass:           make(map[string]int),
		rollbackPVCsByClass:   make(map[string]int),
		totalStorage:          resource.MustParse("0"),
		rollbackStorage:       resource.MustParse("0"),
	}
}

func (p *Planner) validatePlanInputs(plan *domain.MigrationPlan, options Options) {
	if _, err := kube.NormalizeToolImage(options.ToolImage); err != nil {
		plan.AddCheck(failed("tool-image", err.Error()))
	}

	if problems := validation.IsDNS1123Label(options.SessionID); len(problems) > 0 {
		plan.AddCheck(failed("session-id", strings.Join(problems, "; ")))
	}

	for _, strategy := range options.Strategies {
		if supportedStrategy(strategy) {
			continue
		}

		message := fmt.Sprintf("unsupported pv-migrate strategy %q", strategy)
		if strategy == domain.StrategyAuto {
			message = "strategy auto selects the full fallback order and cannot be combined with explicit strategies"
		}

		plan.AddCheck(failed("strategy", message))
	}

	if !validCapacityAwareness(options.CapacityAwareness) {
		plan.AddCheck(failed(
			"capacity-awareness",
			fmt.Sprintf(
				"unsupported capacity awareness mode %q; use auto, require, or off",
				options.CapacityAwareness,
			),
		))
	}

	if options.PrecopyPasses < 0 {
		plan.AddCheck(failed("precopy-passes", "warm-copy passes cannot be negative"))
	}

	if len(options.DestinationCapacities) > 0 && !supportsDestinationCapacity(options.Operation) {
		plan.AddCheck(failed(
			"destination-capacity",
			fmt.Sprintf(
				"--destination-capacity is not supported for %s; this operation does not create a destination PVC",
				options.Operation,
			),
		))
	}

	if options.AllowVolumeShrink && len(options.DestinationCapacities) == 0 {
		plan.AddCheck(
			failed("destination-capacity", "--allow-volume-shrink requires --destination-capacity"),
		)
	}

	if options.SkipSourceUsageCheck && !options.AllowVolumeShrink {
		plan.AddCheck(
			failed(
				"destination-capacity",
				"--skip-source-usage-check requires --allow-volume-shrink",
			),
		)
	}

	for _, value := range options.DestinationCapacities {
		if err := validateDestinationCapacityValue(value); err != nil {
			plan.AddCheck(failed(
				"destination-capacity",
				fmt.Sprintf("destination capacity %q is invalid: %v", value, err),
			))
		}
	}

	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "source namespace", value: options.SourceNamespace},
		{name: "staging namespace", value: options.StagingNamespace},
		{name: "session namespace", value: options.SessionNamespace},
	} {
		if problems := validation.IsDNS1123Label(field.value); len(problems) == 0 {
			continue
		}

		problems := validation.IsDNS1123Label(field.value)
		plan.AddCheck(failed(
			"namespace",
			fmt.Sprintf(
				"%s %q is invalid: %s",
				field.name,
				field.value,
				strings.Join(problems, "; "),
			),
		))
	}

	if options.Operation != domain.OperationCopy {
		return
	}

	if options.Online {
		plan.AddCheck(warned(
			"copy-mode",
			"online copy performs one finite warm pass with file-level consistency while source Pods may keep writing",
		))

		return
	}

	plan.AddCheck(passed(
		"copy-mode",
		"offline copy requires every source PVC to have zero active Pod consumers",
	))
}

func (p *Planner) discoverPlanWorkload(ctx context.Context, state *planState) error {
	options := state.options
	state.workload = domain.WorkloadSpec{Adapter: domain.WorkloadNone}

	state.pvcNames = uniqueInOrder(options.SourcePVCs)
	if options.PodName == "" {
		if len(state.pvcNames) == 0 {
			state.plan.AddCheck(failed("source-pvc", "at least one source PVC is required"))
		}

		state.plan.Workload = state.workload

		return nil
	}

	if len(options.SourcePVCs) > 0 {
		state.plan.AddCheck(failed(
			"source-pvc",
			"--source-pvc cannot be combined with --pod; the Pod PVC set is migrated as one unit",
		))
	}

	if options.Operation != domain.OperationCopy && p.controllers == nil {
		return domain.NewError(domain.ErrorInternal, "plan", "controller manager is unavailable")
	}

	pod, err := p.client.CoreV1().Pods(options.SourceNamespace).Get(
		ctx,
		options.PodName,
		metav1.GetOptions{},
	)
	if err == nil && (pod == nil || pod.Name == "") {
		err = fmt.Errorf(
			"read Pod %s/%s returned an empty object",
			options.SourceNamespace,
			options.PodName,
		)
	}

	readMessage := sourcePodReadMessage(options.SourceNamespace, options.PodName, err)
	switch {
	case options.Operation == domain.OperationCopy:
		state.plan.AddCheck(
			passed("controller-adapter", "copy does not mutate or pause the selected workload"),
		)
	case err != nil:
		wrapped := domain.WrapError(domain.ErrorKubernetes, "discover workload", readMessage, err)
		state.plan.AddCheck(failed("controller-adapter", wrapped.Error()))
	default:
		discovered, discoverErr := p.controllers.DiscoverPod(ctx, pod, controller.DiscoverOptions{
			Namespace:           options.SourceNamespace,
			PodName:             options.PodName,
			SwitchoverCandidate: options.SwitchoverCandidate,
			AllowLeaderDowntime: options.AllowLeaderDowntime,
		})
		if discoverErr != nil {
			state.plan.AddCheck(failed("controller-adapter", discoverErr.Error()))
		} else {
			state.workload = discovered
			state.plan.AddCheck(passed(
				"controller-adapter",
				fmt.Sprintf("%s provides pause and resume semantics", discovered.Adapter),
			))

			if discovered.KubeBlocks != nil && isRiskRole(discovered.KubeBlocks.Role) {
				state.plan.AddCheck(
					warned("database-role", kubeBlocksRoleWarning(discovered.KubeBlocks)),
				)
			}

			if discovered.KubeBlocks != nil {
				message := "KubeBlocks migration stops the selected Cluster component through componentSpecs[].stop; that component shares the downtime window and source PVCs remain retained"
				if discovered.Controller.Kind == domain.KindInstanceSet {
					message = "KubeBlocks migration pauses InstanceSet reconciliation and stops only the selected Pod; sibling instances remain running while InstanceSet self-healing is suspended"
				}

				state.plan.AddCheck(warned("database-pause-scope", message))
			}
		}
	}

	if err != nil {
		state.plan.AddCheck(failed("source-pod", readMessage))
		state.plan.Workload = state.workload
		return nil
	}

	state.sourcePod = pod
	if options.SourceNode == "" {
		state.options.SourceNode = pod.Spec.NodeName
	}

	if state.options.SourceNode != pod.Spec.NodeName {
		state.plan.AddCheck(failed(
			"source-node",
			fmt.Sprintf(
				"Pod %s/%s runs on %s, requested source node is %s",
				pod.Namespace, pod.Name, pod.Spec.NodeName, state.options.SourceNode,
			),
		))
	}

	state.pvcNames = podPVCNames(pod)
	if len(state.pvcNames) == 0 {
		state.plan.AddCheck(failed("source-pod", "Pod has no PVC volumes"))
	} else {
		state.plan.AddCheck(
			passed("source-pod", fmt.Sprintf("Pod references %d PVC(s)", len(state.pvcNames))),
		)
	}

	state.plan.Workload = state.workload

	return nil
}

func (p *Planner) loadPlanContext(ctx context.Context, state *planState) {
	options := state.options
	p.logInfo(
		"loading migration cluster inventory",
		"session", options.SessionID,
		"namespace", options.SourceNamespace,
		"pvcs", len(state.pvcNames),
		"autoTargetNode", state.autoTargetNode,
	)
	state.inventory = p.loadPlanInventory(ctx, options, state.pvcNames, state.autoTargetNode)
	state.storageClasses = state.inventory.storageClasses

	state.storageClassErrors = state.inventory.storageClassError
	if options.TargetNode != "" {
		node, err := state.inventory.targetNode, state.inventory.targetNodeErr
		switch {
		case err != nil:
			state.plan.AddCheck(failed("target-node", fmt.Sprintf("read target node: %v", err)))
		case node == nil || node.Name == "":
			state.plan.AddCheck(failed("target-node", "read target node returned an empty object"))
		default:
			state.targetNode = node
			if !nodeReady(node) || node.Spec.Unschedulable {
				state.plan.AddCheck(
					failed(
						"target-node",
						fmt.Sprintf("node %s must be Ready and schedulable", node.Name),
					),
				)
			} else {
				state.plan.AddCheck(
					passed(
						"target-node",
						fmt.Sprintf("node %s is Ready and schedulable", node.Name),
					),
				)
			}

			p.checkPodTargetScheduling(state.plan, state.sourcePod, state.workload, node)
		}
	}

	var err error
	if state.destinationPVCs, err = resolveDestinationPVCs(
		options.DestinationPVCs,
		state.pvcNames,
	); err != nil {
		state.plan.AddCheck(failed("destination-pvc", err.Error()))
	}

	if state.transferScopes, err = resolveTransferScopes(
		options.SourcePaths,
		options.DestinationPaths,
		state.pvcNames,
	); err != nil {
		state.plan.AddCheck(failed("transfer-path", err.Error()))
		state.transferScopes = make([]*domain.TransferScope, len(state.pvcNames))
	}

	if state.requestedCapacities, err = resolveDestinationCapacities(
		options.DestinationCapacities,
		state.pvcNames,
	); err != nil {
		state.plan.AddCheck(failed("destination-capacity", err.Error()))
		state.requestedCapacities = make([]string, len(state.pvcNames))
	}
}

type planVolumeInput struct {
	pvc               *corev1.PersistentVolumeClaim
	pv                *corev1.PersistentVolume
	mode              corev1.PersistentVolumeMode
	capacity          resource.Quantity
	sourceClass       string
	sourceProvisioner string
}

func (p *Planner) planVolumes(ctx context.Context, state *planState) {
	for index, name := range state.pvcNames {
		input, ok := p.loadPlanVolumeInput(ctx, state, index, name)
		if !ok {
			continue
		}

		destinationCapacity, sourceUsed, usageKnown := p.planVolumeCapacity(
			ctx,
			state,
			index,
			input,
		)
		p.updatePlanStorageTotals(state, input, destinationCapacity)

		storageClass, bindingMode, ok := p.resolvePlanStorageClass(
			state,
			input,
			destinationCapacity,
		)
		if !ok {
			continue
		}

		if !p.appendPlanVolume(
			state,
			index,
			input,
			destinationCapacity,
			sourceUsed,
			usageKnown,
			storageClass,
			bindingMode,
		) {
			continue
		}

		p.checkPlanVolumeConsumers(ctx, state, input)
	}

	if len(state.unmanagedConsumers) > 0 {
		names := slices.Collect(maps.Keys(state.unmanagedConsumers))
		sort.Strings(names)
		state.plan.AddCheck(failed(
			"controller-adapter",
			fmt.Sprintf(
				"PVC-only migration has active Pod consumer(s) %s; use --pod to select a workload that pvc-migrate can pause before final sync",
				strings.Join(names, ","),
			),
		))
	}

	if state.options.Operation == domain.OperationCopy && state.options.Online &&
		state.sourcePod == nil {
		state.options.SourceNode = inferOnlineCopySourceNode(
			state.plan,
			state.options.SourceNode,
			state.copyConsumerNodes,
		)
		if state.options.SourceNode != "" && state.inventory.sourceNode == nil &&
			state.inventory.sourceNodeErr == nil {
			state.inventory.sourceNode, state.inventory.sourceNodeErr = p.client.CoreV1().
				Nodes().
				Get(
					ctx,
					state.options.SourceNode,
					metav1.GetOptions{},
				)
		}
	}
}

func (p *Planner) finalizePlan(ctx context.Context, state *planState) {
	capacityInventory := state.inventory.capacity
	if capacityInventory == nil {
		capacityInventory = &storageCapacityInventory{mode: state.options.CapacityAwareness}
	}

	p.finalizePlanTarget(ctx, state, capacityInventory)
	p.finalizePlanStrategies(state)
	p.finalizePlanResources(ctx, state)
	p.finalizePlanSession(state)
}

func (p *Planner) finalizePlanTarget(
	ctx context.Context,
	state *planState,
	capacityInventory *storageCapacityInventory,
) {
	if state.autoTargetNode {
		state.targetNode = p.selectTargetNodeFromNodes(
			state.plan,
			state.workload,
			state.sourcePod,
			state.options.SourceNode,
			state.plannedVolumes,
			state.sourcePVs,
			state.storageClasses,
			capacityInventory,
			state.inventory.nodes,
			state.inventory.nodesErr,
		)
		if state.targetNode != nil {
			state.options.TargetNode = state.targetNode.Name
			state.plan.TargetNode = state.targetNode.Name
			p.checkPodTargetScheduling(
				state.plan,
				state.sourcePod,
				state.workload,
				state.targetNode,
			)
		}
	}

	if state.options.Operation == domain.OperationMigratePod &&
		state.sourcePod != nil &&
		state.sourcePod.Spec.NodeName != "" &&
		state.sourcePod.Spec.NodeName == state.options.TargetNode &&
		!state.storageClassChanged &&
		len(state.plannedVolumes) > 0 &&
		len(state.plannedVolumes) == len(state.pvcNames) {
		message := fmt.Sprintf(
			"Pod %s/%s already uses target node %s and every PVC already uses the requested StorageClass",
			state.options.SourceNamespace,
			state.options.PodName,
			state.options.TargetNode,
		)
		if state.options.ForceReprovision {
			state.plan.AddCheck(
				warned(
					"force-reprovision",
					message+"; --force-reprovision will replace the backing PVs",
				),
			)
		} else {
			state.plan.AddCheck(
				failed(
					"migration-needed",
					message+"; use --force-reprovision to intentionally replace the backing PVs",
				),
			)
		}
	}

	var (
		csiNode    *storagev1.CSINode
		csiNodeErr error
	)
	if state.targetNode != nil && len(state.plannedVolumes) > 0 {
		if state.autoTargetNode {
			csiNode, csiNodeErr = p.client.StorageV1().CSINodes().Get(
				ctx,
				state.targetNode.Name,
				metav1.GetOptions{},
			)
		} else {
			csiNode, csiNodeErr = state.inventory.csiNode, state.inventory.csiNodeErr
		}
	}

	p.checkPlanTargetTopology(state, csiNode, csiNodeErr)

	if state.targetNode != nil && len(state.plannedVolumes) > 0 &&
		validCapacityAwareness(state.options.CapacityAwareness) &&
		state.options.CapacityAwareness != domain.CapacityAwarenessOff {
		p.checkStorageCapacity(
			state.plan,
			state.targetNode,
			state.plannedVolumes,
			capacityInventory,
			state.options.CapacityAwareness,
		)
	}

	p.checkPlanSourceNode(state)
}

func (p *Planner) checkPlanTargetTopology(
	state *planState,
	csiNode *storagev1.CSINode,
	csiNodeErr error,
) {
	if state.targetNode == nil {
		return
	}

	for _, volume := range state.plannedVolumes {
		pv := state.sourcePVs[volume.SourcePV.Name]
		if pv != nil && !kube.PVSupportsNode(pv, state.targetNode) &&
			state.mountTopologyConflict == "" {
			state.mountTopologyConflict = fmt.Sprintf(
				"source PV %s node affinity excludes target node %s",
				pv.Name, state.targetNode.Name,
			)
		}

		sc := state.storageClasses[volume.StorageClass]
		if sc == nil {
			continue
		}

		if !matchesAllowedTopologies(sc, state.targetNode) {
			state.plan.AddCheck(failed(
				"storage-topology",
				fmt.Sprintf(
					"node %s does not satisfy StorageClass %s allowedTopologies",
					state.targetNode.Name, sc.Name,
				),
			))
		} else {
			state.plan.AddCheck(passed(
				"storage-topology",
				fmt.Sprintf(
					"StorageClass %s bindingMode=%s topology is compatible",
					sc.Name, volume.BindingMode,
				),
			))
		}

		p.checkCSINodeFromObject(state.plan, sc, state.targetNode, csiNode, csiNodeErr)
	}
}

func (p *Planner) checkPlanSourceNode(state *planState) {
	if state.options.SourceNode == "" {
		return
	}

	node, err := state.inventory.sourceNode, state.inventory.sourceNodeErr
	switch {
	case err != nil:
		state.plan.AddCheck(failed("source-node", fmt.Sprintf("read source node: %v", err)))
	case node == nil || node.Name == "":
		state.plan.AddCheck(failed("source-node", "read source node returned an empty object"))
	case !nodeReady(node) || node.Spec.Unschedulable:
		state.plan.AddCheck(failed(
			"source-node",
			fmt.Sprintf("node %s must be Ready and schedulable for the source tool", node.Name),
		))
	default:
		state.plan.AddCheck(
			passed("source-node", fmt.Sprintf("node %s is Ready and schedulable", node.Name)),
		)
	}
}

func (p *Planner) finalizePlanSession(state *planState) {
	options := state.options

	state.plan.SourceNode = options.SourceNode
	state.plan.Strategies = slices.Clone(options.Strategies)

	state.plan.SessionSpec = domain.NewSessionSpec(
		options.Operation,
		domain.SessionCommon{
			SourceNamespace:      options.SourceNamespace,
			TemporaryNamespace:   options.TemporaryNamespace,
			DestinationNamespace: options.DestinationNamespace,
			SessionNamespace:     options.SessionNamespace,
			Volumes:              state.volumeSpecs,
		},
		state.workload,
		options.Online,
		domain.SessionWorkflowOptions{
			SourceNode:             options.SourceNode,
			TargetNode:             options.TargetNode,
			Strategies:             append([]string(nil), options.Strategies...),
			VerifyChecksum:         options.VerifyChecksum,
			DeleteExtraneous:       options.DeleteExtraneous,
			PrecopyPasses:          options.PrecopyPasses,
			OpenEBSLVMEnableShared: options.OpenEBSLVMEnableShared,
			SkipSourceUsageCheck:   options.SkipSourceUsageCheck,
			ToolImage:              options.ToolImage,
		},
	)
	if len(state.volumeSpecs) != len(state.pvcNames) {
		state.plan.Ready = false
	}
}

func (p *Planner) loadPlanVolumeInput(
	ctx context.Context,
	state *planState,
	index int,
	name string,
) (planVolumeInput, bool) {
	options := state.options

	pvc, err := state.inventory.pvcs[index].pvc, state.inventory.pvcs[index].err
	if err != nil || pvc == nil || pvc.Name == "" {
		if err == nil {
			err = fmt.Errorf(
				"read PVC %s/%s returned an empty object",
				options.SourceNamespace,
				name,
			)
		}

		state.plan.AddCheck(
			failed(
				"source-pvc",
				fmt.Sprintf("read PVC %s/%s: %v", options.SourceNamespace, name, err),
			),
		)

		return planVolumeInput{}, false
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		state.plan.AddCheck(
			failed("source-pvc", fmt.Sprintf("PVC %s/%s must be Bound", pvc.Namespace, pvc.Name)),
		)
		return planVolumeInput{}, false
	}

	p.checkPVCFinalizers(state.plan, pvc, options.Operation)

	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}

	if mode != corev1.PersistentVolumeFilesystem {
		state.plan.AddCheck(failed(
			"volume-mode",
			fmt.Sprintf(
				"PVC %s/%s uses %s; the embedded pv-migrate engine supports Filesystem",
				pvc.Namespace,
				pvc.Name,
				mode,
			),
		))
	}

	if !kube.HasWritableAccessMode(pvc.Spec.AccessModes) {
		state.plan.AddCheck(failed(
			"access-mode",
			fmt.Sprintf(
				"PVC %s/%s has no writable access mode for the destination copy",
				pvc.Namespace,
				pvc.Name,
			),
		))
	}

	pv, err := state.inventory.pvs[index].pv, state.inventory.pvs[index].err
	if err != nil || pv == nil || pv.Name == "" {
		if err == nil {
			err = fmt.Errorf("read PV %s returned an empty object", pvc.Spec.VolumeName)
		}

		state.plan.AddCheck(
			failed("source-pv", fmt.Sprintf("read PV %s: %v", pvc.Spec.VolumeName, err)),
		)

		return planVolumeInput{}, false
	}

	if !sourceBindingMatches(pvc, pv) {
		state.plan.AddCheck(failed(
			"source-binding",
			fmt.Sprintf(
				"PV %s claimRef does not match PVC %s/%s UID %s",
				pv.Name,
				pvc.Namespace,
				pvc.Name,
				pvc.UID,
			),
		))
	}

	p.checkSessionOwnership(ctx, state.plan, options.SessionNamespace, pvc, pv)
	state.sourcePVs[pv.Name] = pv

	capacity, ok := pv.Spec.Capacity[corev1.ResourceStorage]
	if !ok || capacity.Sign() <= 0 {
		state.plan.AddCheck(
			failed("capacity", fmt.Sprintf("PV %s has no positive storage capacity", pv.Name)),
		)
		return planVolumeInput{}, false
	}

	sourceClass := ""
	if pvc.Spec.StorageClassName != nil {
		sourceClass = *pvc.Spec.StorageClassName
	}

	sourceProvisioner := ""
	if sourceStorageClass := state.storageClasses[sourceClass]; sourceStorageClass != nil {
		sourceProvisioner = sourceStorageClass.Provisioner
	}

	return planVolumeInput{
		pvc: pvc, pv: pv, mode: mode, capacity: capacity,
		sourceClass: sourceClass, sourceProvisioner: sourceProvisioner,
	}, true
}

func (p *Planner) planVolumeCapacity(
	ctx context.Context,
	state *planState,
	index int,
	input planVolumeInput,
) (resource.Quantity, int64, bool) {
	destinationCapacity := input.capacity

	var (
		sourceUsed int64
		usageKnown bool
	)
	if index >= len(state.requestedCapacities) || state.requestedCapacities[index] == "" {
		return destinationCapacity, sourceUsed, usageKnown
	}

	parsed, err := resource.ParseQuantity(state.requestedCapacities[index])
	if err != nil || parsed.Sign() <= 0 {
		return destinationCapacity, sourceUsed, usageKnown
	}

	destinationCapacity = parsed

	comparison := destinationCapacity.Cmp(input.capacity)
	switch {
	case comparison > 0:
		state.plan.AddCheck(passed(
			"destination-capacity",
			fmt.Sprintf(
				"PVC %s/%s destination capacity expands from %s to %s",
				input.pvc.Namespace,
				input.pvc.Name,
				input.capacity.String(),
				destinationCapacity.String(),
			),
		))
	case comparison < 0:
		partialSource := index < len(state.transferScopes) &&
			domain.SourceTransferPath(state.transferScopes[index]) != domain.VolumeRootPath
		p.checkPlanVolumeShrink(
			ctx,
			state,
			index,
			input,
			destinationCapacity,
			partialSource,
			&sourceUsed,
			&usageKnown,
		)
	}

	return destinationCapacity, sourceUsed, usageKnown
}

func (p *Planner) updatePlanStorageTotals(
	state *planState,
	input planVolumeInput,
	destinationCapacity resource.Quantity,
) {
	state.totalStorage.Add(destinationCapacity)
	state.rollbackStorage.Add(input.capacity)
	rollbackQuantity := state.rollbackByClass[input.sourceClass]
	rollbackQuantity.Add(input.capacity)
	state.rollbackByClass[input.sourceClass] = rollbackQuantity
	state.rollbackPVCsByClass[input.sourceClass]++
}

func (p *Planner) resolvePlanStorageClass(
	state *planState,
	input planVolumeInput,
	destinationCapacity resource.Quantity,
) (*storagev1.StorageClass, storagev1.VolumeBindingMode, bool) {
	destinationClass := state.options.DestinationClass
	if destinationClass == "" {
		destinationClass = input.sourceClass
	}

	if destinationClass != input.sourceClass {
		state.storageClassChanged = true
	}

	if destinationClass == "" {
		state.plan.AddCheck(failed(
			"storage-class",
			fmt.Sprintf(
				"PVC %s/%s has no storageClassName and no destination class was supplied",
				input.pvc.Namespace, input.pvc.Name,
			),
		))

		return nil, "", false
	}

	sc, cached := state.storageClasses[destinationClass]

	storageClassErr := state.storageClassErrors[destinationClass]
	if !cached {
		storageClassErr = fmt.Errorf("storage class %s was not loaded", destinationClass)
	}

	if storageClassErr == nil && (sc == nil || sc.Name == "") {
		storageClassErr = fmt.Errorf(
			"read StorageClass %s returned an empty object",
			destinationClass,
		)
	}

	if storageClassErr != nil {
		state.plan.AddCheck(failed(
			"storage-class",
			fmt.Sprintf("read StorageClass %s: %v", destinationClass, storageClassErr),
		))

		return nil, "", false
	}

	bindingMode := storagev1.VolumeBindingImmediate
	if sc.VolumeBindingMode != nil {
		bindingMode = *sc.VolumeBindingMode
	}

	classQuantity := state.storageByClass[destinationClass]
	classQuantity.Add(destinationCapacity)
	state.storageByClass[destinationClass] = classQuantity
	state.pvcsByClass[destinationClass]++

	return sc, bindingMode, true
}

func (p *Planner) appendPlanVolume(
	state *planState,
	index int,
	input planVolumeInput,
	destinationCapacity resource.Quantity,
	sourceUsed int64,
	usageKnown bool,
	storageClass *storagev1.StorageClass,
	bindingMode storagev1.VolumeBindingMode,
) bool {
	destinationName := destinationPVCNameFor(
		state.options,
		state.destinationPVCs,
		input.pvc.Name,
		index,
	)
	if problems := validation.IsDNS1123Subdomain(destinationName); len(problems) > 0 {
		state.plan.AddCheck(failed(
			"destination-pvc",
			fmt.Sprintf(
				"generated PVC name %q is invalid: %s",
				destinationName,
				strings.Join(problems, "; "),
			),
		))

		return false
	}

	destinationKey := state.options.StagingNamespace + "/" + destinationName
	if previousSource, exists := state.destinationSources[destinationKey]; exists {
		state.plan.AddCheck(failed(
			"destination-pvc",
			fmt.Sprintf(
				"source PVCs %s/%s and %s/%s map to the same destination PVC %s",
				state.options.SourceNamespace,
				previousSource,
				input.pvc.Namespace,
				input.pvc.Name,
				destinationKey,
			),
		))
	} else {
		state.destinationSources[destinationKey] = input.pvc.Name
	}

	transferScope := (*domain.TransferScope)(nil)
	if index < len(state.transferScopes) {
		transferScope = state.transferScopes[index]
	}

	if transferScope != nil {
		message := transferScopePlanMessage(input.pvc.Namespace, input.pvc.Name, transferScope)
		if state.options.Operation == domain.OperationCopy {
			state.plan.AddCheck(passed("transfer-scope", message))
		} else {
			state.plan.AddCheck(warned("transfer-scope", message))
		}
	}

	destinationRef := domain.ObjectReference{
		APIVersion: domain.CoreAPIVersion,
		Kind:       domain.KindPersistentVolumeClaim,
		Namespace:  state.options.StagingNamespace,
		Name:       destinationName,
	}

	accessModes := append([]corev1.PersistentVolumeAccessMode(nil), input.pvc.Spec.AccessModes...)

	state.plannedVolumes = append(state.plannedVolumes, domain.PlannedVolume{
		SourcePVC:        pvcReference(input.pvc),
		SourcePV:         pvReference(input.pv),
		DestinationPVC:   destinationRef,
		Capacity:         destinationCapacity.String(),
		SourceCapacity:   input.capacity.String(),
		SourceUsedBytes:  sourceUsed,
		SourceUsageKnown: usageKnown,
		AccessModes:      accessModes,
		VolumeMode:       input.mode,
		StorageClass:     state.options.DestinationClass,
		BindingMode:      bindingMode,
		CSIProvisioner:   storageClass.Provisioner,
		TransferScope:    domain.CloneTransferScope(transferScope),
	})
	if state.options.DestinationClass == "" {
		state.plannedVolumes[len(state.plannedVolumes)-1].StorageClass = input.sourceClass
	}

	state.volumeSpecs = append(state.volumeSpecs, domain.VolumeSpec{
		SourcePVC:           pvcReference(input.pvc),
		SourcePV:            pvReference(input.pv),
		SourceReclaimPolicy: input.pv.Spec.PersistentVolumeReclaimPolicy,
		SourcePVCSpec:       *input.pvc.Spec.DeepCopy(),
		SourcePVCMetadata: domain.PVCMetadata{
			Labels:          maps.Clone(input.pvc.Labels),
			Annotations:     filteredPVCAnnotations(input.pvc.Annotations),
			OwnerReferences: append([]metav1.OwnerReference(nil), input.pvc.OwnerReferences...),
		},
		DestinationPVC:   destinationRef,
		Capacity:         destinationCapacity.String(),
		SourceCapacity:   input.capacity.String(),
		SourceUsedBytes:  sourceUsed,
		SourceUsageKnown: usageKnown,
		StorageClass:     state.plannedVolumes[len(state.plannedVolumes)-1].StorageClass,
		AccessModes:      accessModes,
		VolumeMode:       input.mode,
		TransferScope:    domain.CloneTransferScope(transferScope),
	})

	return true
}

func (p *Planner) checkPlanVolumeConsumers(
	ctx context.Context,
	state *planState,
	input planVolumeInput,
) {
	consumers := p.checkPVCReferencesFromPods(
		state.plan,
		input.pvc,
		state.sourcePod,
		state.options.Operation,
		state.options.Online,
		state.inventory.namespacePods,
		state.inventory.namespacePodsErr,
	)
	if warmCopyRequested(state.options) {
		inspect, patch := p.checkWarmCopyMountCompatibility(
			ctx,
			state.plan,
			state.options.Operation,
			state.options.OpenEBSLVMEnableShared,
			input.pvc,
			input.pv,
			input.sourceClass,
			state.storageClasses[input.sourceClass],
			state.storageClassErrors[input.sourceClass],
			consumers,
		)
		state.inspectOpenEBSShared = state.inspectOpenEBSShared || inspect
		state.patchOpenEBSShared = state.patchOpenEBSShared || patch
	}

	if state.options.Operation == domain.OperationMigrate && state.sourcePod == nil {
		for _, consumer := range consumers {
			state.unmanagedConsumers[consumer.Name] = struct{}{}
		}
	}

	if state.options.Operation == domain.OperationCopy && state.options.Online &&
		state.sourcePod == nil {
		for _, consumer := range consumers {
			if consumer.Spec.NodeName != "" {
				state.copyConsumerNodes[consumer.Spec.NodeName] = struct{}{}
			}
		}
	}

	if state.workload.Adapter == domain.WorkloadStandalone {
		for _, owner := range input.pvc.OwnerReferences {
			if owner.Kind == domain.KindPod {
				state.plan.AddCheck(failed(
					"pvc-ownership",
					fmt.Sprintf(
						"standalone Pod migration cannot preserve Pod-owned PVC %s/%s",
						input.pvc.Namespace, input.pvc.Name,
					),
				))
			}
		}
	}
}

func (p *Planner) checkPlanVolumeShrink(
	ctx context.Context,
	state *planState,
	index int,
	input planVolumeInput,
	destinationCapacity resource.Quantity,
	partialSource bool,
	sourceUsed *int64,
	usageKnown *bool,
) {
	message := fmt.Sprintf(
		"PVC %s/%s destination capacity %s is below source PV capacity %s; pass --allow-volume-shrink only when the copied data is known to fit",
		input.pvc.Namespace,
		input.pvc.Name,
		destinationCapacity.String(),
		input.capacity.String(),
	)
	if state.options.AllowVolumeShrink {
		state.plan.AddCheck(warned("destination-capacity", message))
	} else {
		state.plan.AddCheck(failed("destination-capacity", message))
		return
	}

	if state.options.SkipSourceUsageCheck {
		state.plan.AddCheck(warned(
			"source-usage",
			fmt.Sprintf(
				"PVC %s/%s source usage check was explicitly skipped; independently verify that its data fits destination capacity %s",
				input.pvc.Namespace,
				input.pvc.Name,
				destinationCapacity.String(),
			),
		))

		return
	}

	if p.volumeUsageReader == nil {
		backend := input.sourceClass
		if input.sourceProvisioner != "" {
			backend += " (" + input.sourceProvisioner + ")"
		}

		if backend == "" {
			backend = "<unknown>"
		}

		state.plan.AddCheck(failed(
			"source-usage",
			fmt.Sprintf(
				"PVC %s/%s uses StorageClass backend %s, which has no trusted CRD usage reader; pass --skip-source-usage-check only after independently verifying that the data fits",
				input.pvc.Namespace,
				input.pvc.Name,
				backend,
			),
		))

		return
	}

	usage, err := p.volumeUsageReader.Read(ctx, kube.VolumeUsageReadOptions{
		SourcePVC: pvcReference(input.pvc),
		SourcePV:  pvReference(input.pv),
	})
	if err != nil {
		state.plan.AddCheck(failed(
			"source-usage",
			fmt.Sprintf(
				"PVC %s/%s usage could not be read from its storage backend CRD: %v; pass --skip-source-usage-check only after independently verifying that the data fits",
				input.pvc.Namespace,
				input.pvc.Name,
				err,
			),
		))

		return
	}

	if usage.UsedBytes < 0 {
		state.plan.AddCheck(failed(
			"source-usage",
			fmt.Sprintf(
				"PVC %s/%s storage backend returned invalid used bytes %d",
				input.pvc.Namespace,
				input.pvc.Name,
				usage.UsedBytes,
			),
		))

		return
	}

	*sourceUsed = usage.UsedBytes
	*usageKnown = true

	usageSource := strings.TrimSpace(usage.Source)
	if usageSource == "" {
		usageSource = "the storage backend CRD"
	}

	if usage.UsedBytes > destinationCapacity.Value() {
		if partialSource {
			state.plan.AddCheck(failed(
				"source-usage",
				fmt.Sprintf(
					"PVC %s/%s whole-volume usage is %d bytes according to %s, above destination capacity %s; this cannot prove that selected source directory %q fits; pass --skip-source-usage-check only after independently measuring the selected data",
					input.pvc.Namespace,
					input.pvc.Name,
					usage.UsedBytes,
					usageSource,
					destinationCapacity.String(),
					domain.SourceTransferPath(state.transferScopes[index]),
				),
			))
		} else {
			state.plan.AddCheck(failed(
				"source-usage",
				fmt.Sprintf(
					"PVC %s/%s uses %d bytes according to %s, above destination capacity %s; shrink is unsafe",
					input.pvc.Namespace,
					input.pvc.Name,
					usage.UsedBytes,
					usageSource,
					destinationCapacity.String(),
				),
			))
		}

		return
	}

	state.plan.AddCheck(passed(
		"source-usage",
		fmt.Sprintf(
			"PVC %s/%s usage is %d bytes according to %s and fits destination capacity %s",
			input.pvc.Namespace,
			input.pvc.Name,
			usage.UsedBytes,
			usageSource,
			destinationCapacity.String(),
		),
	))
}

func sourceBindingMatches(pvc *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) bool {
	if pvc == nil || pv == nil || pvc.UID == "" || pv.UID == "" || pvc.Spec.VolumeName != pv.Name ||
		pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.UID == "" {
		return false
	}

	return pv.Spec.ClaimRef.Namespace == pvc.Namespace &&
		pv.Spec.ClaimRef.Name == pvc.Name &&
		pv.Spec.ClaimRef.UID == pvc.UID
}

func applyDefaults(options Options) Options {
	if options.Operation == "" {
		options.Operation = domain.OperationMigrate
	}

	if options.SourceNamespace == "" {
		options.SourceNamespace = "default"
	}

	if options.DestinationNamespace == "" {
		options.DestinationNamespace = options.SourceNamespace
	}

	if options.TemporaryNamespace == "" {
		options.TemporaryNamespace = "pvc-migrate-system"
	}

	if options.SessionNamespace == "" {
		options.SessionNamespace = options.TemporaryNamespace
	}

	if options.StagingNamespace == "" {
		options.StagingNamespace = options.DestinationNamespace
	}

	if options.ToolImage == "" {
		options.ToolImage = kube.DefaultToolImageRepository + ":main"
	}

	if options.CapacityAwareness == "" {
		options.CapacityAwareness = domain.CapacityAwarenessAuto
	}

	if len(options.Strategies) == 0 ||
		(len(options.Strategies) == 1 && containsStrategy(options.Strategies, domain.StrategyAuto)) {
		options.Strategies = autoStrategies(options.SourceNamespace, options.StagingNamespace)
	}

	return options
}

func isAutoNode(value string) bool {
	return value == "" || strings.EqualFold(value, domain.AutoValue)
}

func (p *Planner) checkPodTargetScheduling(
	plan *domain.MigrationPlan,
	sourcePod *corev1.Pod,
	workload domain.WorkloadSpec,
	node *corev1.Node,
) {
	if sourcePod == nil {
		return
	}

	schedulingSpec := sourcePod.Spec
	if workload.Adapter == domain.WorkloadStandalone {
		schedulingSpec = *sourcePod.Spec.DeepCopy()
		// The standalone adapter clears both direct and hostname placement from
		// the recreated Pod before applying the selected target node.
		schedulingSpec.NodeName = ""
		if hostname := schedulingSpec.NodeSelector[corev1.LabelHostname]; hostname != "" &&
			hostname != node.Labels[corev1.LabelHostname] {
			delete(schedulingSpec.NodeSelector, corev1.LabelHostname)
			plan.AddCheck(
				warned(
					"pod-scheduling",
					fmt.Sprintf(
						"standalone Pod hostname selector %s will be replaced with target hostname %s",
						hostname,
						node.Labels[corev1.LabelHostname],
					),
				),
			)
		}
	}

	issues := schedulingIssues(schedulingSpec, node)
	if len(issues) > 0 {
		plan.AddCheck(failed("pod-scheduling", strings.Join(issues, "; ")))
	} else {
		plan.AddCheck(
			passed(
				"pod-scheduling",
				"target node satisfies nodeSelector, required nodeAffinity, and taints",
			),
		)
	}

	if workload.Adapter == domain.WorkloadStandalone {
		resourceIssues, known := resourceFitIssues(schedulingSpec, node)
		if len(resourceIssues) > 0 {
			plan.AddCheck(failed("pod-resources", strings.Join(resourceIssues, "; ")))
		} else if !known {
			plan.AddCheck(
				warned(
					"pod-resources",
					fmt.Sprintf(
						"target node %s does not publish all allocatable resources needed to verify standalone Pod placement",
						node.Name,
					),
				),
			)
		}
	}
}

type targetNodeCandidate struct {
	node            *corev1.Node
	distinctFromSrc bool
	taintPenalty    int
	sourcePVMatches int
	capacityKnown   int
	capacityUnknown int
	capacitySurplus resource.Quantity
	resourceUnknown int
}

type planCheckTask func(*domain.MigrationPlan)

func runPlanCheckTasks(plan *domain.MigrationPlan, tasks []planCheckTask) {
	checks := make([][]domain.Check, len(tasks))
	parallel.For(len(tasks), func(index int) {
		result := &domain.MigrationPlan{Ready: true}
		tasks[index](result)
		checks[index] = result.Checks
	})

	for _, taskChecks := range checks {
		for _, check := range taskChecks {
			plan.AddCheck(check)
		}
	}
}

func (p *Planner) selectTargetNode(
	ctx context.Context,
	plan *domain.MigrationPlan,
	workload domain.WorkloadSpec,
	sourcePod *corev1.Pod,
	sourceNode string,
	volumes []domain.PlannedVolume,
	capacityInventory *storageCapacityInventory,
) *corev1.Node {
	nodes, err := p.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if nodes == nil {
		return p.selectTargetNodeFromNodes(
			plan,
			workload,
			sourcePod,
			sourceNode,
			volumes,
			nil,
			nil,
			capacityInventory,
			nil,
			err,
		)
	}

	return p.selectTargetNodeFromNodes(
		plan,
		workload,
		sourcePod,
		sourceNode,
		volumes,
		nil,
		nil,
		capacityInventory,
		nodes.Items,
		err,
	)
}

func (p *Planner) selectTargetNodeFromNodes(
	plan *domain.MigrationPlan,
	workload domain.WorkloadSpec,
	sourcePod *corev1.Pod,
	sourceNode string,
	volumes []domain.PlannedVolume,
	sourcePVs map[string]*corev1.PersistentVolume,
	storageClasses map[string]*storagev1.StorageClass,
	capacityInventory *storageCapacityInventory,
	nodes []corev1.Node,
	err error,
) *corev1.Node {
	if len(volumes) == 0 {
		plan.AddCheck(
			failed(
				"target-node",
				"target node auto-selection requires at least one valid source PVC",
			),
		)

		return nil
	}

	if err != nil {
		plan.AddCheck(failed("target-node", fmt.Sprintf("list nodes for auto-selection: %v", err)))
		return nil
	}

	candidates := make([]targetNodeCandidate, 0, len(nodes))
	for i := range nodes {
		node := &nodes[i]
		if !nodeReady(node) || node.Spec.Unschedulable {
			continue
		}

		if sourcePod != nil && len(schedulingIssuesForTarget(sourcePod, workload, node)) > 0 {
			continue
		}

		resourceUnknown := 0
		if sourcePod != nil && workload.Adapter == domain.WorkloadStandalone {
			spec := *sourcePod.Spec.DeepCopy()
			spec.NodeName = ""

			resourceIssues, known := resourceFitIssues(spec, node)
			if len(resourceIssues) > 0 {
				continue
			}

			if !known {
				resourceUnknown = 1
			}
		}

		if sourcePod != nil && len(podMigrationIssues(sourcePod.Spec, sourceNode, node.Name)) > 0 {
			continue
		}

		compatible := true

		sourcePVMatches := 0
		for _, volume := range volumes {
			if sc := storageClasses[volume.StorageClass]; sc != nil &&
				!matchesAllowedTopologies(sc, node) {
				compatible = false
				break
			}

			if pv := sourcePVs[volume.SourcePV.Name]; pv != nil && kube.PVSupportsNode(pv, node) {
				sourcePVMatches++
			}
		}

		if !compatible {
			continue
		}

		capacityKnown, capacityUnknown, capacitySurplus, capacityCompatible := capacityScore(
			capacityInventory,
			node,
			volumes,
		)
		if !capacityCompatible {
			continue
		}

		candidates = append(
			candidates,
			targetNodeCandidate{
				node:            node,
				distinctFromSrc: sourceNode == "" || node.Name != sourceNode,
				taintPenalty:    hardTaintCount(node),
				sourcePVMatches: sourcePVMatches,
				capacityKnown:   capacityKnown,
				capacityUnknown: capacityUnknown,
				capacitySurplus: capacitySurplus,
				resourceUnknown: resourceUnknown,
			},
		)
	}

	if len(candidates) == 0 {
		message := "no Ready and schedulable node satisfies Pod scheduling and destination StorageClass topology"
		if capacityInventory != nil && capacityInventory.loaded &&
			len(capacityInventory.items) > 0 {
			message += " with sufficient CSI-reported capacity"
		}

		plan.AddCheck(failed("target-node", message))

		return nil
	}

	slices.SortStableFunc(candidates, compareTargetNodeCandidates)
	selected := candidates[0]

	reasons := []string{"topology-compatible Ready node"}
	if selected.capacityKnown > 0 {
		reasons = append(
			reasons,
			fmt.Sprintf("CSI capacity verified for %d StorageClass(es)", selected.capacityKnown),
		)
	}

	if sourceNode != "" {
		switch {
		case selected.node.Name != sourceNode:
			reasons = append(reasons, "distinct from source "+sourceNode)
		case len(candidates) == 1:
			reasons = append(
				reasons,
				fmt.Sprintf("only compatible node; source node %s is retained", sourceNode),
			)
		}
	}

	plan.AddCheck(
		passed(
			"target-node-selection",
			fmt.Sprintf(
				"auto selected target node %s (%s)",
				selected.node.Name,
				strings.Join(reasons, ", "),
			),
		),
	)

	return selected.node
}

func compareTargetNodeCandidates(a, b targetNodeCandidate) int {
	if a.capacityKnown != b.capacityKnown {
		if a.capacityKnown > b.capacityKnown {
			return -1
		}
		return 1
	}

	if a.capacityUnknown != b.capacityUnknown {
		if a.capacityUnknown < b.capacityUnknown {
			return -1
		}
		return 1
	}

	if a.resourceUnknown != b.resourceUnknown {
		if a.resourceUnknown < b.resourceUnknown {
			return -1
		}
		return 1
	}

	if comparison := a.capacitySurplus.Cmp(b.capacitySurplus); comparison != 0 {
		return -comparison
	}

	if a.distinctFromSrc != b.distinctFromSrc {
		if a.distinctFromSrc {
			return -1
		}
		return 1
	}

	if a.taintPenalty != b.taintPenalty {
		if a.taintPenalty < b.taintPenalty {
			return -1
		}
		return 1
	}

	if a.sourcePVMatches != b.sourcePVMatches {
		if a.sourcePVMatches > b.sourcePVMatches {
			return -1
		}
		return 1
	}

	return strings.Compare(a.node.Name, b.node.Name)
}

func hardTaintCount(node *corev1.Node) int {
	count := 0
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule ||
			taint.Effect == corev1.TaintEffectNoExecute {
			count++
		}
	}

	return count
}

func schedulingIssuesForTarget(
	sourcePod *corev1.Pod,
	workload domain.WorkloadSpec,
	node *corev1.Node,
) []string {
	spec := sourcePod.Spec
	if workload.Adapter == domain.WorkloadStandalone {
		spec = *sourcePod.Spec.DeepCopy()

		spec.NodeName = ""
		if hostname := spec.NodeSelector[corev1.LabelHostname]; hostname != "" &&
			hostname != node.Labels[corev1.LabelHostname] {
			delete(spec.NodeSelector, corev1.LabelHostname)
		}
	}

	issues := schedulingIssues(spec, node)
	if workload.Adapter == domain.WorkloadStandalone {
		resourceIssues, _ := resourceFitIssues(spec, node)
		issues = append(issues, resourceIssues...)
	}

	return issues
}

func containsStrategy(strategies []string, wanted string) bool {
	return slices.Contains(strategies, wanted)
}

func autoStrategies(sourceNamespace, destinationNamespace string) []string {
	if sourceNamespace == destinationNamespace {
		return []string{domain.StrategyMount, domain.StrategyClusterIP}
	}
	return []string{domain.StrategyClusterIP, domain.StrategyLocal}
}

func warmCopyRequested(options Options) bool {
	switch options.Operation {
	case domain.OperationCopy:
		return options.Online
	case domain.OperationMigrate, domain.OperationMigratePod:
		return options.PrecopyPasses > 0
	default:
		return false
	}
}

func (p *Planner) checkWarmCopyMountCompatibility(
	ctx context.Context,
	plan *domain.MigrationPlan,
	operation domain.Operation,
	enableOpenEBSLVMShared bool,
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
	storageClassName string,
	storageClass *storagev1.StorageClass,
	storageClassErr error,
	consumers []*corev1.Pod,
) (inspectShared, patchShared bool) {
	inspectOpenEBSLVMShared := pv != nil && pv.Spec.CSI != nil &&
		pv.Spec.CSI.Driver == kube.OpenEBSLVMCSIDriver

	active := make([]string, 0, len(consumers))
	for _, pod := range consumers {
		if pod != nil && kube.ActivePodUsesPVC(pod, pvc.Name) {
			name := pod.Name
			if pod.Spec.NodeName != "" {
				name += "@" + pod.Spec.NodeName
			}

			active = append(active, name)
		}
	}

	if len(active) == 0 {
		return inspectOpenEBSLVMShared, false
	}

	sort.Strings(active)

	consumerList := strings.Join(active, ",")
	if inspectOpenEBSLVMShared {
		if p == nil || p.openEBSLVMSharedVolumeManager == nil {
			plan.AddCheck(
				failed(
					"warm-copy-mount",
					fmt.Sprintf(
						"PVC %s/%s uses OpenEBS LVM source PV %s, but the planner cannot inspect the current LVMVolume.spec.shared value",
						pvc.Namespace,
						pvc.Name,
						pv.Name,
					),
				),
			)

			return true, false
		}

		shared, err := p.openEBSLVMSharedVolumeManager.Shared(
			ctx,
			domain.ObjectReference{Name: pv.Name, UID: pv.UID},
			domain.ObjectReference{},
			"",
		)
		if err != nil {
			plan.AddCheck(
				failed(
					"warm-copy-mount",
					fmt.Sprintf(
						"read current OpenEBS LVMVolume.spec.shared for source PV %s used by PVC %s/%s: %v",
						pv.Name,
						pvc.Namespace,
						pvc.Name,
						err,
					),
				),
			)

			return true, false
		}

		if !shared {
			if enableOpenEBSLVMShared &&
				(operation == domain.OperationMigrate || operation == domain.OperationMigratePod) {
				plan.AddCheck(
					passed(
						"warm-copy-mount",
						fmt.Sprintf(
							"OpenEBS LVMVolume for source PV %s does not currently have spec.shared=yes; execution will temporarily set it to yes, then restore its original value after the warm-copy pass. It verifies a second-Pod read-write mount without writing data for active PVC %s/%s on %s. This enables same-node concurrent mounts only; coordinate application writes during warm copy",
							pv.Name,
							pvc.Namespace,
							pvc.Name,
							consumerList,
						),
					),
				)

				return true, true
			}

			message := fmt.Sprintf(
				"OpenEBS LVMVolume for source PV %s does not currently have spec.shared=yes; PVC %s/%s is already mounted by %s, so a second Pod cannot mount it for warm copy; %s",
				pv.Name,
				pvc.Namespace,
				pvc.Name,
				consumerList,
				warmCopyMountFallback(operation),
			)
			if operation == domain.OperationMigrate || operation == domain.OperationMigratePod {
				message += "; alternatively, rerun with --openebs-lvm-enable-shared to patch the existing matching OpenEBS LVMVolume spec.shared to \"yes\" before the read-write mount probe"
			}

			plan.AddCheck(failed("warm-copy-mount", message))

			return true, false
		}

		plan.AddCheck(
			passed(
				"warm-copy-mount",
				fmt.Sprintf(
					"OpenEBS LVMVolume for source PV %s currently has spec.shared=yes for PVC %s/%s; execution will verify a second-Pod read-write mount without writing data before warm copy",
					pv.Name,
					pvc.Namespace,
					pvc.Name,
				),
			),
		)

		return true, false
	}

	if storageClassName == "" {
		plan.AddCheck(
			warned(
				"warm-copy-mount",
				fmt.Sprintf(
					"PVC %s/%s is active on %s and has no StorageClass; execution will verify a read-only second-Pod mount before warm copy",
					pvc.Namespace,
					pvc.Name,
					consumerList,
				),
			),
		)

		return false, false
	}

	if storageClassErr != nil || storageClass == nil || storageClass.Name == "" {
		plan.AddCheck(
			warned(
				"warm-copy-mount",
				fmt.Sprintf(
					"PVC %s/%s is active on %s; concurrent mount support for StorageClass %s is unknown because it could not be read, so execution will run a read-only mount probe before warm copy",
					pvc.Namespace,
					pvc.Name,
					consumerList,
					storageClassName,
				),
			),
		)

		return false, false
	}

	if storageClass.Provisioner == "openebs.io/local" {
		storageType := openEBSLocalStorageType(storageClass)
		if strings.EqualFold(storageType, "hostpath") {
			plan.AddCheck(
				passed(
					"warm-copy-mount",
					fmt.Sprintf(
						"StorageClass %s uses OpenEBS Local PV Hostpath; same-node second-Pod mounts are supported for PVC %s/%s and execution will verify the read-only mount",
						storageClass.Name,
						pvc.Namespace,
						pvc.Name,
					),
				),
			)

			return false, false
		}

		detail := "has no recognizable StorageType metadata"
		if storageType != "" {
			detail = "declares StorageType=" + storageType
		}

		plan.AddCheck(
			warned(
				"warm-copy-mount",
				fmt.Sprintf(
					"StorageClass %s uses the OpenEBS local provisioner and %s; only StorageType=hostpath has built-in concurrent same-node mount support, so execution will run a read-only mount probe for PVC %s/%s",
					storageClass.Name,
					detail,
					pvc.Namespace,
					pvc.Name,
				),
			),
		)

		return false, false
	}

	plan.AddCheck(
		warned(
			"warm-copy-mount",
			fmt.Sprintf(
				"StorageClass %s uses %s and PVC %s/%s is active on %s; concurrent mount support is driver-specific, so execution will run a read-only mount probe before warm copy; if the mount is rejected, %s",
				storageClass.Name,
				storageClass.Provisioner,
				pvc.Namespace,
				pvc.Name,
				consumerList,
				warmCopyMountFallback(operation),
			),
		),
	)

	return false, false
}

func warmCopyMountFallback(operation domain.Operation) string {
	if operation == domain.OperationCopy {
		return "stop all active PVC consumers and rerun without --online"
	}
	return "use --precopy-passes 0 for offline final sync"
}

func sourcePodReadMessage(namespace, name string, err error) string {
	if err == nil {
		return ""
	}

	if apierrors.IsNotFound(err) {
		return fmt.Sprintf(
			"source Pod %s/%s does not exist; verify --namespace and --pod",
			namespace,
			name,
		)
	}

	return fmt.Sprintf("read source Pod %s/%s: %v", namespace, name, err)
}

func openEBSLocalStorageType(storageClass *storagev1.StorageClass) string {
	if storageClass == nil || storageClass.Provisioner != "openebs.io/local" {
		return ""
	}

	for key, value := range storageClass.Parameters {
		if strings.EqualFold(key, "storageType") {
			return strings.TrimSpace(value)
		}
	}

	config := storageClass.Annotations["cas.openebs.io/config"]
	if strings.TrimSpace(config) == "" {
		return ""
	}

	var entries []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := yaml.Unmarshal([]byte(config), &entries); err != nil {
		return ""
	}

	for _, entry := range entries {
		if strings.EqualFold(strings.TrimSpace(entry.Name), "StorageType") {
			return strings.TrimSpace(entry.Value)
		}
	}

	return ""
}

// filterStrategies removes constraints that are deterministically known from
// the Kubernetes inventory before a tool resource is created. A fallback
// strategy remains valid when one earlier strategy cannot handle the topology.
func filterStrategies(
	plan *domain.MigrationPlan,
	options Options,
	mountTopologyConflict string,
) []string {
	filtered := make([]string, 0, len(options.Strategies))
	for _, strategy := range options.Strategies {
		if strategy == domain.StrategyMount {
			switch {
			case options.SourceNamespace != options.StagingNamespace:
				plan.AddCheck(
					warned(
						"strategy",
						"mount skipped: source and destination PVCs are in different namespaces; use clusterip or local",
					),
				)

				continue
			case mountTopologyConflict != "":
				plan.AddCheck(
					warned(
						"strategy",
						"mount skipped: "+mountTopologyConflict+"; use clusterip, nodeport, loadbalancer, or local",
					),
				)

				continue
			}
		}

		filtered = append(filtered, strategy)
	}

	return filtered
}

func supportedStrategy(strategy string) bool {
	switch strategy {
	case domain.StrategyMount,
		domain.StrategyClusterIP,
		domain.StrategyLoadBalancer,
		domain.StrategyNodePort,
		domain.StrategyLocal:
		return true
	default:
		return false
	}
}

func destinationPVCName(options Options, source string, index int) string {
	return destinationPVCNameFor(options, nil, source, index)
}

func destinationPVCNameFor(options Options, mapped []string, source string, index int) string {
	if index < len(mapped) && mapped[index] != "" {
		return mapped[index]
	}

	if options.Operation == domain.OperationCopy &&
		options.SourceNamespace != options.StagingNamespace {
		return source
	}

	suffix := options.SessionID
	if len(suffix) > 12 {
		suffix = suffix[:12]
	}

	suffix = strings.TrimRight(suffix, "-.")
	if suffix == "" {
		suffix = "session"
	}

	maxSource := 253 - len(suffix) - len("-migrated-")
	if len(source) > maxSource {
		source = strings.TrimRight(source[:maxSource], "-.")
	}

	return source + "-migrated-" + suffix
}

func passed(name, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityInfo, Passed: true, Message: message}
}

func warned(name, message string) domain.Check {
	return domain.Check{
		Name:     name,
		Severity: domain.SeverityWarning,
		Passed:   true,
		Message:  message,
	}
}

func failed(name, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityError, Passed: false, Message: message}
}

func pvcReference(pvc *corev1.PersistentVolumeClaim) domain.ObjectReference {
	return domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
}

func pvReference(pv *corev1.PersistentVolume) domain.ObjectReference {
	return domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolume,
		Name:            pv.Name,
		UID:             pv.UID,
		ResourceVersion: pv.ResourceVersion,
	}
}

func nodeReady(node *corev1.Node) bool {
	if node == nil {
		return false
	}

	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

func matchesAllowedTopologies(sc *storagev1.StorageClass, node *corev1.Node) bool {
	if len(sc.AllowedTopologies) == 0 {
		return true
	}

	for _, term := range sc.AllowedTopologies {
		matches := true
		for _, expression := range term.MatchLabelExpressions {
			actual, exists := node.Labels[expression.Key]
			if !exists || !contains(expression.Values, actual) {
				matches = false
				break
			}
		}

		if matches {
			return true
		}
	}

	return false
}

func (p *Planner) checkCSINode(
	ctx context.Context,
	plan *domain.MigrationPlan,
	sc *storagev1.StorageClass,
	node *corev1.Node,
) {
	csiNode, err := p.client.StorageV1().CSINodes().Get(ctx, node.Name, metav1.GetOptions{})
	p.checkCSINodeFromObject(plan, sc, node, csiNode, err)
}

func (p *Planner) checkCSINodeFromObject(
	plan *domain.MigrationPlan,
	sc *storagev1.StorageClass,
	node *corev1.Node,
	csiNode *storagev1.CSINode,
	err error,
) {
	if node == nil || sc == nil {
		plan.AddCheck(failed("csi-node", "node or StorageClass inventory returned an empty object"))
		return
	}

	if apierrors.IsNotFound(err) {
		plan.AddCheck(
			warned(
				"csi-node",
				fmt.Sprintf(
					"node %s has no CSINode object; provisioner %s must validate node support during reservation",
					node.Name,
					sc.Provisioner,
				),
			),
		)

		return
	}

	if err != nil {
		plan.AddCheck(failed("csi-node", fmt.Sprintf("read CSINode %s: %v", node.Name, err)))
		return
	}

	if csiNode == nil || csiNode.Name == "" {
		plan.AddCheck(
			failed("csi-node", fmt.Sprintf("read CSINode %s returned an empty object", node.Name)),
		)
		return
	}

	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == sc.Provisioner {
			plan.AddCheck(
				passed(
					"csi-node",
					fmt.Sprintf("CSI driver %s is registered on %s", sc.Provisioner, node.Name),
				),
			)

			return
		}
	}

	plan.AddCheck(
		warned(
			"csi-node",
			fmt.Sprintf(
				"provisioner %s is absent from CSINode %s; an in-tree or external provisioner may still support it",
				sc.Provisioner,
				node.Name,
			),
		),
	)
}

func (p *Planner) checkPVCReferences(
	ctx context.Context,
	plan *domain.MigrationPlan,
	pvc *corev1.PersistentVolumeClaim,
	sourcePod *corev1.Pod,
	operation domain.Operation,
	online bool,
) {
	pods, err := p.client.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		p.checkPVCReferencesFromPods(plan, pvc, sourcePod, operation, online, nil, err)

		return
	}

	p.checkPVCReferencesFromPods(plan, pvc, sourcePod, operation, online, pods.Items, nil)
}

func (p *Planner) checkPVCReferencesFromPods(
	plan *domain.MigrationPlan,
	pvc *corev1.PersistentVolumeClaim,
	sourcePod *corev1.Pod,
	operation domain.Operation,
	online bool,
	pods []corev1.Pod,
	listErr error,
) []*corev1.Pod {
	if listErr != nil {
		plan.AddCheck(
			failed(
				"pvc-consumers",
				fmt.Sprintf("list Pods for PVC %s/%s: %v", pvc.Namespace, pvc.Name, listErr),
			),
		)

		return nil
	}

	consumers := make([]*corev1.Pod, 0)
	for i := range pods {
		pod := &pods[i]
		if operation.RecreatesPVC() {
			if kube.PodPreventsSafePVCDeletion(pod, pvc.Name) {
				consumers = append(consumers, pod)
			}
			continue
		}

		if kube.ActivePodUsesPVC(pod, pvc.Name) {
			consumers = append(consumers, pod)
		}
	}

	consumerNames := make([]string, 0, len(consumers))
	for _, consumer := range consumers {
		consumerNames = append(consumerNames, consumer.Name)
	}

	sort.Strings(consumerNames)

	if operation == domain.OperationCopy && !online && len(consumers) > 0 {
		plan.AddCheck(
			failed(
				"pvc-consumers",
				fmt.Sprintf(
					"offline copy requires PVC %s/%s to have zero active Pod consumers; found %s; use --online for a finite warm copy",
					pvc.Namespace,
					pvc.Name,
					strings.Join(consumerNames, ","),
				),
			),
		)

		return consumers
	}

	if operation == domain.OperationCopy && online &&
		kube.HasAccessMode(pvc.Spec.AccessModes, corev1.ReadWriteOnce) {
		unscheduled := make([]string, 0)
		for _, consumer := range consumers {
			if consumer.Spec.NodeName == "" {
				unscheduled = append(unscheduled, consumer.Name)
			}
		}

		if len(unscheduled) > 0 {
			sort.Strings(unscheduled)
			plan.AddCheck(
				failed(
					"source-node",
					fmt.Sprintf(
						"RWO PVC %s/%s has unscheduled active consumer(s) %s; wait for every consumer to receive a node before online copy",
						pvc.Namespace,
						pvc.Name,
						strings.Join(unscheduled, ","),
					),
				),
			)

			return consumers
		}
	}

	if sourcePod != nil {
		others := make([]string, 0)
		for _, consumer := range consumers {
			if consumer.Name != sourcePod.Name {
				others = append(others, consumer.Name)
			}
		}

		sort.Strings(others)

		switch {
		case len(others) > 0:
			plan.AddCheck(
				failed(
					"pvc-consumers",
					fmt.Sprintf(
						"PVC %s/%s is shared with Pod(s): %s",
						pvc.Namespace,
						pvc.Name,
						strings.Join(others, ","),
					),
				),
			)
		case operation == domain.OperationCopy && online && kube.HasAccessMode(pvc.Spec.AccessModes, corev1.ReadWriteOncePod):
			plan.AddCheck(
				failed(
					"pvc-consumers",
					fmt.Sprintf(
						"active RWOP PVC %s/%s cannot be warm-copied",
						pvc.Namespace,
						pvc.Name,
					),
				),
			)
		case operation == domain.OperationCopy && online:
			plan.AddCheck(
				warned(
					"pvc-consumers",
					fmt.Sprintf(
						"PVC %s/%s is active on selected Pod %s; online copy has file-level consistency",
						pvc.Namespace,
						pvc.Name,
						sourcePod.Name,
					),
				),
			)
		default:
			plan.AddCheck(
				passed(
					"pvc-consumers",
					fmt.Sprintf(
						"PVC %s/%s belongs to the selected migration unit",
						pvc.Namespace,
						pvc.Name,
					),
				),
			)
		}

		return consumers
	}

	if len(consumers) == 0 {
		plan.AddCheck(
			passed("pvc-consumers", fmt.Sprintf("PVC %s/%s is offline", pvc.Namespace, pvc.Name)),
		)
		return consumers
	}

	if operation == domain.OperationMigrate {
		return consumers
	}

	if operation == domain.OperationReserve {
		plan.AddCheck(
			warned(
				"pvc-consumers",
				fmt.Sprintf(
					"PVC %s/%s is active on Pod(s) %s; reservation keeps the source PVC mounted and provisions destination storage",
					pvc.Namespace,
					pvc.Name,
					strings.Join(consumerNames, ","),
				),
			),
		)

		return consumers
	}

	if kube.HasAccessMode(pvc.Spec.AccessModes, corev1.ReadWriteOncePod) {
		plan.AddCheck(
			failed(
				"pvc-consumers",
				fmt.Sprintf("active RWOP PVC %s/%s cannot be warm-copied", pvc.Namespace, pvc.Name),
			),
		)

		return consumers
	}

	plan.AddCheck(
		warned(
			"pvc-consumers",
			fmt.Sprintf(
				"PVC %s/%s is active on Pod(s) %s; warm copy has file-level consistency until final sync",
				pvc.Namespace,
				pvc.Name,
				strings.Join(consumerNames, ","),
			),
		),
	)

	return consumers
}

func inferOnlineCopySourceNode(
	plan *domain.MigrationPlan,
	requested string,
	consumerNodes map[string]struct{},
) string {
	nodes := make([]string, 0, len(consumerNodes))
	for node := range consumerNodes {
		nodes = append(nodes, node)
	}

	sort.Strings(nodes)

	if len(nodes) == 0 {
		return requested
	}

	if len(nodes) > 1 {
		plan.AddCheck(
			failed(
				"source-node",
				fmt.Sprintf(
					"online copy consumers run on multiple nodes (%s); copy each node group in a separate session",
					strings.Join(nodes, ","),
				),
			),
		)

		return requested
	}

	if requested != "" && requested != nodes[0] {
		plan.AddCheck(
			failed(
				"source-node",
				fmt.Sprintf(
					"online copy consumer runs on %s, requested source node is %s",
					nodes[0],
					requested,
				),
			),
		)

		return requested
	}

	if requested == "" {
		plan.AddCheck(
			passed(
				"source-node-inference",
				fmt.Sprintf("inferred source tool node %s from active PVC consumers", nodes[0]),
			),
		)

		return nodes[0]
	}

	return requested
}

func podPVCNames(pod *corev1.Pod) []string {
	values := make([]string, 0)
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			values = append(values, volume.PersistentVolumeClaim.ClaimName)
		}
	}

	return uniqueSorted(values)
}

func uniqueInOrder(values []string) []string {
	seen := make(map[string]struct{}, len(values))

	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}

		if _, exists := seen[value]; exists {
			continue
		}

		seen[value] = struct{}{}
		result = append(result, value)
	}

	return result
}

func uniqueSorted(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}

	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}

	sort.Strings(result)

	return result
}

func filteredPVCAnnotations(input map[string]string) map[string]string {
	result := map[string]string{}
	for key, value := range input {
		switch key {
		case "pv.kubernetes.io/bind-completed",
			"pv.kubernetes.io/bound-by-controller",
			"volume.kubernetes.io/selected-node",
			"volume.kubernetes.io/storage-provisioner",
			"volume.beta.kubernetes.io/storage-provisioner",
			kube.PVCStorageResizerAnnotation,
			"kubectl.kubernetes.io/last-applied-configuration",
			kube.SessionKey:
			continue
		default:
			result[key] = value
		}
	}

	return result
}

func contains(values []string, expected string) bool {
	return slices.Contains(values, expected)
}

func isRiskRole(role string) bool {
	switch strings.ToLower(role) {
	case "leader", "primary", "master", "unknown":
		return true
	default:
		return false
	}
}

func kubeBlocksRoleWarning(spec *domain.KubeBlocksSpec) string {
	if spec.Role == "unknown" && spec.SwitchoverCandidate == "" {
		return "selected KubeBlocks instance role is unknown; possible leader downtime was explicitly acknowledged"
	}

	if spec.SwitchoverCandidate == "" {
		return fmt.Sprintf(
			"selected KubeBlocks instance role=%s; leader downtime was explicitly acknowledged",
			spec.Role,
		)
	}

	if spec.SwitchoverStrategy == domain.KubeBlocksSwitchoverMongoDBNative {
		return fmt.Sprintf(
			"selected KubeBlocks MongoDB instance role=%s; native candidate switchover targets=%s",
			spec.Role,
			spec.SwitchoverCandidate,
		)
	}

	return fmt.Sprintf(
		"selected KubeBlocks instance role=%s; switchover target=%s",
		spec.Role,
		spec.SwitchoverCandidate,
	)
}

func (p *Planner) checkPVCFinalizers(
	plan *domain.MigrationPlan,
	pvc *corev1.PersistentVolumeClaim,
	operation domain.Operation,
) {
	if pvc == nil || !operation.RecreatesPVC() {
		return
	}

	custom := kube.BlockingPVCFinalizers(pvc)
	if len(custom) == 0 {
		return
	}

	plan.AddCheck(
		failed(
			"pvc-finalizers",
			fmt.Sprintf(
				"PVC %s/%s has custom finalizer(s) %s; remove them or complete their controller cleanup before an operation that recreates the PVC",
				pvc.Namespace,
				pvc.Name,
				strings.Join(custom, ", "),
			),
		),
	)
}
