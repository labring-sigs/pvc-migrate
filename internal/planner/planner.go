package planner

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"
)

// transferInput contains only context shared by the four PVC transfer
// operations. The operation itself is supplied by each typed planner entry
// point, so an input cannot silently select a different workflow family.
type transferInput struct {
	v1alpha1.TransferOptions
	SessionID            string
	SourceNamespace      string
	TemporaryNamespace   string
	DestinationNamespace string
	SessionNamespace     string
	StagingNamespace     string
	ToolImage            string
	Volumes              []v1alpha1.VolumeRequest
}

// PodWorkloadDiscoverer resolves the workload adapter for a Pod. Defined here
// to invert the dependency: the planner layer must not import the controller
// package, which implements this on top of its k8s reconciliation machinery.
type PodWorkloadDiscoverer interface {
	DiscoverPod(
		ctx context.Context,
		pod *corev1.Pod,
		namespace string,
		expected v1alpha1.LocalResourceReference,
		switchoverCandidate string,
		allowLeaderDowntime bool,
	) (v1alpha1.WorkloadSpec, error)
}

type Planner struct {
	client                        kubernetes.Interface
	controllers                   PodWorkloadDiscoverer
	openEBSLVMSharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
	volumeUsageReader             kube.VolumeUsageReader
	logger                        *slog.Logger
	controllerSubmission          bool
	planningWorkflow              bool
	sessionRecordNamespace        string
	workflowOwners                workflowOwnerFinder
}

// workflowOwnerFinder is the narrow ownership capability required during
// planning. Planning never reads or mutates a workflow payload.
type workflowOwnerFinder interface {
	Find(ctx context.Context, sessionID string, namespaces ...string) (*kube.WorkflowOwner, error)
}

type planState struct {
	options               transferInput
	autoStrategyRequested bool
	autoTargetNode        bool
	plan                  *domain.TransferPlan
	pvcNames              []string
	inventory             planInventory
	targetNode            *corev1.Node
	destinationPVCs       []string
	transferScopes        []*v1alpha1.TransferScope
	volumeSpecs           []v1alpha1.VolumeSpec
	plannedVolumes        []domain.PlannedVolume
	destinationSources    map[string]string
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
}

func New(client kubernetes.Interface, controllers PodWorkloadDiscoverer) *Planner {
	return &Planner{
		client:         client,
		controllers:    controllers,
		workflowOwners: kube.NewCRDWorkflowOwnerFinder(nil),
	}
}

func (p *Planner) WithWorkflowOwnerFinder(finder workflowOwnerFinder) *Planner {
	p.workflowOwners = finder
	return p
}

// WithControllerSubmission checks the caller's workflow submission permissions;
// the elected controller owns execution and its data-plane permissions.
func (p *Planner) WithControllerSubmission(enabled bool) *Planner {
	p.controllerSubmission = enabled
	return p
}

// WithSessionRecordNamespace scopes session-record lookups to the namespace
// local CLI sessions persist in. Namespaced workflow planning derives every
// namespace role from the workload namespace, while local session records
// still live in the --session-namespace value; ownership checks must search
// the record location as well as the planned roles.
func (p *Planner) WithSessionRecordNamespace(namespace string) *Planner {
	p.sessionRecordNamespace = namespace
	return p
}

// ForController plans admitted CRDs using controller execution permissions and
// CR-backed resource estimates. The original CLI planner is unchanged.
func (p *Planner) ForController() *Planner {
	clone := *p
	clone.controllerSubmission = true
	clone.planningWorkflow = true

	return &clone
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

func newPlanState(p *Planner, input transferInput, operation domain.Operation) planState {
	autoStrategyRequested := len(input.Strategies) == 0 ||
		(len(input.Strategies) == 1 && containsStrategy(input.Strategies, domain.StrategyAuto))
	autoTargetNode := isAutoNode(input.TargetNode)
	options := applyDefaults(input)
	p.logInfo(
		"migration planning started",
		"operation", operation,
		"session", options.SessionID,
		"namespace", options.SourceNamespace,
		"volumeOverrides", len(options.Volumes),
	)

	if autoTargetNode {
		options.TargetNode = ""
	}

	plan := &domain.TransferPlan{
		PlanSummary: domain.PlanSummary{
			APIVersion: domain.SessionAPIVersion,
			Kind:       domain.TransferPlanKind,
			SessionID:  options.SessionID,

			SessionNamespace: options.SessionNamespace,

			Ready: true,
		}, SourceNamespace: options.SourceNamespace,
		TemporaryNamespace:   options.TemporaryNamespace,
		DestinationNamespace: options.DestinationNamespace,

		ToolImage:         options.ToolImage,
		CapacityAwareness: domain.CapacityAwareness(options.CapacityAwareness),
		TargetNode:        options.TargetNode,

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
		sourcePVs:             make(map[string]*corev1.PersistentVolume),
		storageByClass:        make(map[string]resource.Quantity),
		rollbackByClass:       make(map[string]resource.Quantity),
		pvcsByClass:           make(map[string]int),
		rollbackPVCsByClass:   make(map[string]int),
		totalStorage:          resource.MustParse("0"),
		rollbackStorage:       resource.MustParse("0"),
	}
}

func (p *Planner) validateStorageInputs(plan checkRecorder, options transferInput) {
	p.validateCommonPlanInputs(plan, options)
	validateDestinationCapacityInputs(
		plan, options.DestinationCapacity, options.Volumes,
		options.AllowVolumeShrink, options.SkipSourceUsageCheck,
	)
}

func (p *Planner) validateCommonPlanInputs(plan checkRecorder, options transferInput) {
	if _, err := kube.NormalizeToolImage(options.ToolImage); err != nil {
		plan.AddCheck(failed(domain.CheckNameToolImage, err.Error()))
	}

	if problems := validation.IsDNS1123Label(options.SessionID); len(problems) > 0 {
		plan.AddCheck(failed(domain.CheckNameSessionID, strings.Join(problems, "; ")))
	}

	for _, strategy := range options.Strategies {
		if supportedStrategy(strategy) {
			continue
		}

		message := fmt.Sprintf("unsupported pv-migrate strategy %q", strategy)
		if strategy == domain.StrategyAuto {
			message = "strategy auto selects the full fallback order and cannot be combined with explicit strategies"
		}

		plan.AddCheck(failed(domain.CheckNameStrategy, message))
	}

	if !validCapacityAwareness(domain.CapacityAwareness(options.CapacityAwareness)) {
		plan.AddCheck(failed(
			domain.CheckNameCapacityAwareness,
			fmt.Sprintf(
				"unsupported capacity awareness mode %q; use auto, require, or off",
				options.CapacityAwareness,
			),
		))
	}

	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "source namespace", value: options.SourceNamespace},
		{name: "staging namespace", value: options.StagingNamespace},
		{name: "session namespace", value: options.SessionNamespace},
	} {
		problems := validation.IsDNS1123Label(field.value)
		if len(problems) == 0 {
			continue
		}

		plan.AddCheck(failed(
			domain.CheckNameNamespace,
			fmt.Sprintf(
				"%s %q is invalid: %s",
				field.name,
				field.value,
				strings.Join(problems, "; "),
			),
		))
	}
}

func validateDestinationCapacityInputs(
	plan checkRecorder,
	destinationCapacity string,
	volumes []v1alpha1.VolumeRequest,
	allowVolumeShrink, skipSourceUsageCheck bool,
) {
	capacities := make([]string, 0, len(volumes)+1)
	if destinationCapacity != "" {
		capacities = append(capacities, destinationCapacity)
	}

	for _, volume := range volumes {
		if volume.Capacity != "" {
			capacities = append(capacities, volume.Capacity)
		}
	}

	if allowVolumeShrink && len(capacities) == 0 {
		plan.AddCheck(
			failed(
				domain.CheckNameDestinationCapacity,
				"--allow-volume-shrink requires --destination-capacity",
			),
		)
	}

	if skipSourceUsageCheck && !allowVolumeShrink {
		plan.AddCheck(
			failed(
				domain.CheckNameDestinationCapacity,
				"--skip-source-usage-check requires --allow-volume-shrink",
			),
		)
	}

	for _, value := range capacities {
		if err := validateDestinationCapacityValue(value); err != nil {
			plan.AddCheck(failed(
				domain.CheckNameDestinationCapacity,
				fmt.Sprintf("destination capacity %q is invalid: %v", value, err),
			))
		}
	}
}

func (p *Planner) selectPlanVolumes(
	ctx context.Context,
	state *planState,
	volumes []v1alpha1.VolumeRequest,
	selectedPod *v1alpha1.LocalResourceReference,
) (*corev1.Pod, error) {
	options := state.options

	if selectedPod == nil {
		state.pvcNames = requestedPVCNames(volumes)
		if len(state.pvcNames) == 0 {
			state.plan.AddCheck(
				failed(domain.CheckNameSourcePVC, "at least one source PVC is required"),
			)
		}

		state.plan.Workload = v1alpha1.WorkloadSpec{Adapter: v1alpha1.WorkloadNone}

		return nil, nil
	}

	if selectedPod.Name == "" {
		state.plan.AddCheck(failed(domain.CheckNameSourcePod, "Pod name is required"))
		return nil, nil
	}

	pod, err := p.client.CoreV1().Pods(options.SourceNamespace).Get(
		ctx,
		selectedPod.Name,
		metav1.GetOptions{},
	)
	if err == nil && (pod == nil || pod.Name == "") {
		err = fmt.Errorf(
			"read Pod %s/%s returned an empty object",
			options.SourceNamespace,
			selectedPod.Name,
		)
	}

	readMessage := sourcePodReadMessage(options.SourceNamespace, selectedPod.Name, err)

	if err != nil {
		state.plan.AddCheck(failed(domain.CheckNameSourcePod, readMessage))
		state.plan.Workload = v1alpha1.WorkloadSpec{Adapter: v1alpha1.WorkloadNone}
		return nil, nil
	}

	if err := checkReference(
		selectedPod, localPlanningReference(kube.PodReference(pod)),
	); err != nil {
		return nil, err
	}

	if options.SourceNode == "" {
		state.options.SourceNode = pod.Spec.NodeName
	}

	if state.options.SourceNode != pod.Spec.NodeName {
		state.plan.AddCheck(failed(
			domain.CheckNameSourceNode,
			fmt.Sprintf(
				"Pod %s/%s runs on %s, requested source node is %s",
				pod.Namespace, pod.Name, pod.Spec.NodeName, state.options.SourceNode,
			),
		))
	}

	state.pvcNames = podPVCNames(pod)
	if len(state.pvcNames) == 0 {
		state.plan.AddCheck(failed(domain.CheckNameSourcePod, "Pod has no PVC volumes"))
	} else {
		state.plan.AddCheck(
			passed(
				domain.CheckNameSourcePod,
				fmt.Sprintf("Pod references %d PVC(s)", len(state.pvcNames)),
			),
		)
	}

	state.plan.Workload = v1alpha1.WorkloadSpec{Adapter: v1alpha1.WorkloadNone}

	return pod, nil
}

func (p *Planner) loadPlanContext(ctx context.Context, state *planState) error {
	options := state.options
	p.logInfo(
		"loading migration cluster inventory",
		"session", options.SessionID,
		"namespace", options.SourceNamespace,
		"pvcs", len(state.pvcNames),
		"autoTargetNode", state.autoTargetNode,
	)
	state.inventory = p.loadPlanInventory(
		ctx, options.SourceNamespace, state.pvcNames,
		options.SourceNode, options.TargetNode, options.DestinationStorageClass,
		domain.CapacityAwareness(options.CapacityAwareness), state.autoTargetNode,
	)
	state.storageClasses = state.inventory.storageClasses

	state.storageClassErrors = state.inventory.storageClassError
	if options.TargetNode != "" {
		node, err := state.inventory.targetNode, state.inventory.targetNodeErr
		switch {
		case err != nil:
			state.plan.AddCheck(
				failed(domain.CheckNameTargetNode, fmt.Sprintf("read target node: %v", err)),
			)
		case node == nil || node.Name == "":
			state.plan.AddCheck(
				failed(domain.CheckNameTargetNode, "read target node returned an empty object"),
			)
		default:
			state.targetNode = node
			if !kube.NodeReadyAndSchedulable(node) {
				state.plan.AddCheck(
					failed(
						domain.CheckNameTargetNode,
						fmt.Sprintf("node %s must be Ready and schedulable", node.Name),
					),
				)
			} else {
				state.plan.AddCheck(
					passed(
						domain.CheckNameTargetNode,
						fmt.Sprintf("node %s is Ready and schedulable", node.Name),
					),
				)
			}
		}
	}

	state.destinationPVCs = make([]string, len(state.pvcNames))
	state.transferScopes = make([]*v1alpha1.TransferScope, len(state.pvcNames))
	state.requestedCapacities = make([]string, len(state.pvcNames))

	return applyWorkflowVolumes(state, options.Volumes, v1alpha1.TransferOptions{
		DestinationCapacity: options.DestinationCapacity,
		SourcePath:          options.SourcePath, DestinationPath: options.DestinationPath,
	})
}

type planVolumeInput struct {
	index             int
	pvc               *corev1.PersistentVolumeClaim
	pv                *corev1.PersistentVolume
	mode              corev1.PersistentVolumeMode
	capacity          resource.Quantity
	sourceClass       string
	sourceProvisioner string
}

func (p *Planner) loadPlanVolumeInputs(ctx context.Context, state *planState) []planVolumeInput {
	inputs := make([]planVolumeInput, 0, len(state.pvcNames))
	for index, name := range state.pvcNames {
		input, ok := p.loadPlanVolumeInput(ctx, state, index, name)
		if ok {
			input.index = index
			inputs = append(inputs, input)
		}
	}

	return inputs
}

func (p *Planner) planVolumes(
	ctx context.Context,
	state *planState,
	sources []planVolumeInput,
) []planVolumeInput {
	inputs := make([]planVolumeInput, 0, len(sources))
	for _, input := range sources {
		index := input.index

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

		inputs = append(inputs, input)
	}

	return inputs
}

func planCapacityInventory(state *planState) *storageCapacityInventory {
	capacityInventory := state.inventory.capacity
	if capacityInventory == nil {
		capacityInventory = &storageCapacityInventory{
			mode: domain.CapacityAwareness(state.options.CapacityAwareness),
		}
	}

	return capacityInventory
}

func (p *Planner) finalizePlan(ctx context.Context, state *planState) {
	p.finalizePlanTarget(ctx, state, planCapacityInventory(state))
	p.finalizePlanStrategies(state)
	finalizePlanReport(state)
}

func finalizePlanReport(state *planState) {
	state.plan.SourceNode = state.options.SourceNode
	state.plan.Strategies = slices.Clone(state.options.Strategies)

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
				domain.CheckNameSourcePVC,
				fmt.Sprintf("read PVC %s/%s: %v", options.SourceNamespace, name, err),
			),
		)

		return planVolumeInput{}, false
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		state.plan.AddCheck(
			failed(
				domain.CheckNameSourcePVC,
				fmt.Sprintf("PVC %s/%s must be Bound", pvc.Namespace, pvc.Name),
			),
		)

		return planVolumeInput{}, false
	}

	// A requested deletion only waits on protection finalizers while the
	// workload keeps mounting the claim; pausing the workload for a cutover
	// would let it fire mid-transfer. Terminating storage is never a source.
	if pvc.DeletionTimestamp != nil {
		state.plan.AddCheck(
			failed(
				domain.CheckNameSourcePVC,
				fmt.Sprintf(
					"PVC %s/%s is terminating (deletion requested at %s); wait for the deletion to settle or restore the claim before migrating",
					pvc.Namespace,
					pvc.Name,
					pvc.DeletionTimestamp.UTC().Format(time.RFC3339),
				),
			),
		)

		return planVolumeInput{}, false
	}

	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}

	if mode != corev1.PersistentVolumeFilesystem {
		state.plan.AddCheck(failed(
			domain.CheckNameVolumeMode,
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
			domain.CheckNameAccessMode,
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
			failed(
				domain.CheckNameSourcePV,
				fmt.Sprintf("read PV %s: %v", pvc.Spec.VolumeName, err),
			),
		)

		return planVolumeInput{}, false
	}

	if !sourceBindingMatches(pvc, pv) {
		state.plan.AddCheck(failed(
			domain.CheckNameSourceBinding,
			fmt.Sprintf(
				"PV %s claimRef does not match PVC %s/%s UID %s",
				pv.Name,
				pvc.Namespace,
				pvc.Name,
				pvc.UID,
			),
		))
	}

	if pv.DeletionTimestamp != nil {
		state.plan.AddCheck(
			failed(
				domain.CheckNameSourcePV,
				fmt.Sprintf(
					"PV %s is terminating (deletion requested at %s); wait for the deletion to settle before migrating",
					pv.Name,
					pv.DeletionTimestamp.UTC().Format(time.RFC3339),
				),
			),
		)

		return planVolumeInput{}, false
	}

	p.checkSessionOwnership(ctx, state.plan, options.SessionNamespace, pvc, pv)
	state.sourcePVs[pv.Name] = pv

	capacity, ok := pv.Spec.Capacity[corev1.ResourceStorage]
	if !ok || capacity.Sign() <= 0 {
		state.plan.AddCheck(
			failed(
				domain.CheckNameCapacity,
				fmt.Sprintf("PV %s has no positive storage capacity", pv.Name),
			),
		)

		return planVolumeInput{}, false
	}

	if err := kube.ValidateBoundVolumeCapacity(pvc, pv, nil); err != nil {
		state.plan.AddCheck(failed(domain.CheckNameCapacity, err.Error()))

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
			domain.CheckNameDestinationCapacity,
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

func isKubeBlocksPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	labels := pod.GetLabels()

	return strings.EqualFold(labels[kube.ManagedByLabel], "kubeblocks") ||
		labels["apps.kubeblocks.io/component-name"] != "" ||
		labels["kubeblocks.io/role"] != "" ||
		labels["apps.kubeblocks.io/role"] != ""
}

func isKubeBlocksPVC(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc == nil {
		return false
	}

	labels := pvc.GetLabels()

	return strings.EqualFold(labels[kube.ManagedByLabel], "kubeblocks") ||
		labels["apps.kubeblocks.io/component-name"] != ""
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
	destinationClass := state.options.DestinationStorageClass
	if destinationClass == "" {
		destinationClass = input.sourceClass
	}

	if destinationClass != input.sourceClass {
		state.storageClassChanged = true
	}

	if destinationClass == "" {
		state.plan.AddCheck(failed(
			domain.CheckNameStorageClass,
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
			domain.CheckNameStorageClass,
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
	destinationName := state.destinationPVCs[index]
	if destinationName == "" {
		destinationName = generatedDestinationPVCName(
			state.options.SessionID, state.options.SourceNamespace, input.pvc.Name,
		)
	}

	if problems := validation.IsDNS1123Subdomain(destinationName); len(problems) > 0 {
		state.plan.AddCheck(failed(
			domain.CheckNameDestinationPVC,
			fmt.Sprintf(
				"generated PVC name %q is invalid: %s",
				destinationName,
				strings.Join(problems, "; "),
			),
		))

		return false
	}

	destinationKey := state.options.StagingNamespace + "/" + destinationName
	if state.options.StagingNamespace == state.options.SourceNamespace &&
		slices.Contains(state.pvcNames, destinationName) {
		state.plan.AddCheck(failed(
			domain.CheckNameDestinationPVC,
			fmt.Sprintf(
				"destination PVC %s conflicts with a source PVC; choose a distinct destination name or temporary namespace",
				destinationKey,
			),
		))

		return false
	}

	if previousSource, exists := state.destinationSources[destinationKey]; exists {
		state.plan.AddCheck(failed(
			domain.CheckNameDestinationPVC,
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

	transferScope := (*v1alpha1.TransferScope)(nil)
	if index < len(state.transferScopes) {
		transferScope = state.transferScopes[index]
	}

	if err := kube.ValidateDestinationAccessModes(
		storageClass.Provisioner,
		input.pvc.Spec.AccessModes,
	); err != nil {
		state.plan.AddCheck(failed(
			domain.CheckNameDestinationAccessModes,
			fmt.Sprintf(
				"destination StorageClass %s cannot provide PVC %s/%s access modes: %v; choose a StorageClass with matching access-mode support",
				storageClass.Name,
				input.pvc.Namespace,
				input.pvc.Name,
				err,
			),
		))

		return false
	}

	destinationRef := v1alpha1.ObjectReference{
		APIVersion: domain.CoreAPIVersion,
		Kind:       domain.KindPersistentVolumeClaim,
		Namespace:  state.options.StagingNamespace,
		Name:       destinationName,
	}

	accessModes := append([]corev1.PersistentVolumeAccessMode(nil), input.pvc.Spec.AccessModes...)

	state.plannedVolumes = append(state.plannedVolumes, domain.PlannedVolume{
		SourcePVC:        kube.PVCReference(input.pvc),
		SourcePV:         kube.PVReference(input.pv),
		DestinationPVC:   destinationRef,
		Capacity:         destinationCapacity.String(),
		SourceCapacity:   input.capacity.String(),
		SourceUsedBytes:  sourceUsed,
		SourceUsageKnown: usageKnown,
		AccessModes:      accessModes,
		VolumeMode:       input.mode,
		StorageClass:     state.options.DestinationStorageClass,
		BindingMode:      bindingMode,
		CSIProvisioner:   storageClass.Provisioner,
		TransferScope:    transferScope.DeepCopy(),
	})
	if state.options.DestinationStorageClass == "" {
		state.plannedVolumes[len(state.plannedVolumes)-1].StorageClass = input.sourceClass
	}

	state.volumeSpecs = append(state.volumeSpecs, v1alpha1.VolumeSpec{
		SourcePVC:           localPlanningReference(kube.PVCReference(input.pvc)),
		SourcePV:            localPlanningReference(kube.PVReference(input.pv)),
		SourceReclaimPolicy: input.pv.Spec.PersistentVolumeReclaimPolicy,
		SourcePVCSpec:       *input.pvc.Spec.DeepCopy(),
		SourcePVCMetadata: v1alpha1.PVCMetadata{
			Labels:          maps.Clone(input.pvc.Labels),
			Annotations:     kube.PVCAnnotationsForRecreation(input.pvc.Annotations),
			OwnerReferences: append([]metav1.OwnerReference(nil), input.pvc.OwnerReferences...),
		},
		DestinationPVC:   localPlanningReference(destinationRef),
		Capacity:         destinationCapacity.String(),
		SourceCapacity:   input.capacity.String(),
		SourceUsedBytes:  sourceUsed,
		SourceUsageKnown: usageKnown,
		StorageClass:     state.plannedVolumes[len(state.plannedVolumes)-1].StorageClass,
		AccessModes:      accessModes,
		VolumeMode:       input.mode,
		TransferScope:    transferScope.DeepCopy(),
	})

	return true
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
		state.plan.AddCheck(warned(domain.CheckNameDestinationCapacity, message))
	} else {
		state.plan.AddCheck(failed(domain.CheckNameDestinationCapacity, message))
		return
	}

	if state.options.SkipSourceUsageCheck {
		state.plan.AddCheck(warned(
			domain.CheckNameSourceUsage,
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
			domain.CheckNameSourceUsage,
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
		SourcePVC: kube.PVCReference(input.pvc),
		SourcePV:  kube.PVReference(input.pv),
	})
	if err != nil {
		state.plan.AddCheck(failed(
			domain.CheckNameSourceUsage,
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
			domain.CheckNameSourceUsage,
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
				domain.CheckNameSourceUsage,
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
				domain.CheckNameSourceUsage,
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
		domain.CheckNameSourceUsage,
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

func applyDefaults(options transferInput) transferInput {
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
		options.CapacityAwareness = string(domain.CapacityAwarenessAuto)
	}

	options.Strategies = copyengine.ResolveStrategies(
		options.SourceNamespace,
		options.StagingNamespace,
		options.Strategies,
	)

	return options
}

func runPlanCheckTasks(plan checkRecorder, tasks []planCheckTask) {
	checks := make([][]domain.Check, len(tasks))
	parallel.For(len(tasks), func(index int) {
		result := &domain.PlanSummary{Ready: true}
		tasks[index](result)
		checks[index] = result.Checks
	})

	for _, taskChecks := range checks {
		for _, check := range taskChecks {
			plan.AddCheck(check)
		}
	}
}

func containsStrategy(strategies []string, wanted string) bool {
	return slices.Contains(strategies, wanted)
}

func (p *Planner) checkWarmCopyMountCompatibility(
	ctx context.Context,
	plan checkRecorder,
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
					domain.CheckNameWarmCopyMount,
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
			v1alpha1.ObjectReference{Name: pv.Name, UID: pv.UID},
			v1alpha1.ObjectReference{},
			"",
		)
		if err != nil {
			plan.AddCheck(
				failed(
					domain.CheckNameWarmCopyMount,
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
			if enableOpenEBSLVMShared && operation == domain.OperationMigratePod {
				plan.AddCheck(
					passed(
						domain.CheckNameWarmCopyMount,
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
			if operation == domain.OperationMigratePod {
				message += "; alternatively, rerun with --openebs-lvm-enable-shared to patch the existing matching OpenEBS LVMVolume spec.shared to \"yes\" before the read-write mount probe"
			}

			plan.AddCheck(failed(domain.CheckNameWarmCopyMount, message))

			return true, false
		}

		plan.AddCheck(
			passed(
				domain.CheckNameWarmCopyMount,
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
				domain.CheckNameWarmCopyMount,
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
				domain.CheckNameWarmCopyMount,
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

	if storageClass.Provisioner == kube.OpenEBSLocalPVProvisioner {
		storageType := openEBSLocalStorageType(storageClass)
		if strings.EqualFold(storageType, "hostpath") {
			plan.AddCheck(
				passed(
					domain.CheckNameWarmCopyMount,
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
				domain.CheckNameWarmCopyMount,
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
			domain.CheckNameWarmCopyMount,
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
	// Zero precopy passes do not avoid the co-mount: the final-sync tool
	// probe still mounts the source before the workload pauses.
	return "the final-sync tool probe co-mounts the source before the workload pauses, whatever the precopy pass count"
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
	if storageClass == nil || storageClass.Provisioner != kube.OpenEBSLocalPVProvisioner {
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
	plan checkRecorder,
	strategies []string,
	sourceNamespace, destinationNamespace string,
	mountTopologyConflict string,
) []string {
	filtered := make([]string, 0, len(strategies))
	for _, strategy := range strategies {
		if strategy == domain.StrategyMount {
			switch {
			case sourceNamespace != destinationNamespace:
				plan.AddCheck(
					warned(
						domain.CheckNameStrategy,
						"mount skipped: source and destination PVCs are in different namespaces; use clusterip or local",
					),
				)

				continue
			case mountTopologyConflict != "":
				plan.AddCheck(
					warned(
						domain.CheckNameStrategy,
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

func generatedDestinationPVCName(sessionID, sourceNamespace, source string) string {
	suffix := sessionID
	if len(suffix) > 12 {
		suffix = suffix[:12]
	}

	suffix = strings.TrimRight(suffix, "-.")
	if suffix == "" {
		suffix = "session"
	}

	// Keep truncated task and PVC names distinct while preserving stable retries.
	digest := sha256.Sum256(
		[]byte(sourceNamespace + "/" + source + "/" + sessionID),
	)
	suffix = fmt.Sprintf("%s-%x", suffix, digest[:5])

	maxSource := 253 - len(suffix) - len("-migrated-")
	if len(source) > maxSource {
		source = strings.TrimRight(source[:maxSource], "-.")
	}

	return source + "-migrated-" + suffix
}

func passed(name domain.CheckName, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityInfo, Passed: true, Message: message}
}

func warned(name domain.CheckName, message string) domain.Check {
	return domain.Check{
		Name:     name,
		Severity: domain.SeverityWarning,
		Passed:   true,
		Message:  message,
	}
}

func failed(name domain.CheckName, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityError, Passed: false, Message: message}
}

func (p *Planner) checkCSINodeFromObject(
	plan checkRecorder,
	sc *storagev1.StorageClass,
	node *corev1.Node,
	csiNode *storagev1.CSINode,
	err error,
) {
	if node == nil || sc == nil {
		plan.AddCheck(
			failed(
				domain.CheckNameCSINode,
				"node or StorageClass inventory returned an empty object",
			),
		)

		return
	}

	if apierrors.IsNotFound(err) {
		plan.AddCheck(
			warned(
				domain.CheckNameCSINode,
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
		plan.AddCheck(
			failed(domain.CheckNameCSINode, fmt.Sprintf("read CSINode %s: %v", node.Name, err)),
		)
		return
	}

	if csiNode == nil || csiNode.Name == "" {
		plan.AddCheck(
			failed(
				domain.CheckNameCSINode,
				fmt.Sprintf("read CSINode %s returned an empty object", node.Name),
			),
		)

		return
	}

	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == sc.Provisioner {
			plan.AddCheck(
				passed(
					domain.CheckNameCSINode,
					fmt.Sprintf("CSI driver %s is registered on %s", sc.Provisioner, node.Name),
				),
			)

			return
		}
	}

	plan.AddCheck(
		warned(
			domain.CheckNameCSINode,
			fmt.Sprintf(
				"provisioner %s is absent from CSINode %s; an in-tree or external provisioner may still support it",
				sc.Provisioner,
				node.Name,
			),
		),
	)
}

func migrationUnitExternalConsumers(
	workloadKind v1alpha1.WorkloadKind,
	migrationUnit map[string]types.UID,
	consumers []*corev1.Pod,
) []string {
	others := make([]string, 0)

	for _, consumer := range consumers {
		expectedUID, belongs := migrationUnit[consumer.Name]
		identityChanged := workloadKind != v1alpha1.WorkloadNone &&
			(expectedUID == "" || consumer.UID == "" || expectedUID != consumer.UID)

		if !belongs || identityChanged {
			others = append(others, consumer.Name)
		}
	}

	sort.Strings(others)

	return others
}

func migrationUnitConsumerCount(
	affectedPods []v1alpha1.LocalResourceReference,
	selected *corev1.Pod,
	consumers []*corev1.Pod,
) int {
	if selected == nil {
		return 0
	}

	migrationUnit := workloadPodUIDs(affectedPods, selected)

	count := 0
	for _, consumer := range consumers {
		expectedUID, belongs := migrationUnit[consumer.Name]
		if belongs && expectedUID != "" && consumer.UID == expectedUID {
			count++
		}
	}

	return count
}

func workloadPodUIDs(
	affectedPods []v1alpha1.LocalResourceReference,
	selected *corev1.Pod,
) map[string]types.UID {
	uids := map[string]types.UID{selected.Name: selected.UID}
	for _, ref := range affectedPods {
		if existing, ok := uids[ref.Name]; ok && existing != ref.UID {
			uids[ref.Name] = ""
			continue
		}

		uids[ref.Name] = ref.UID
	}

	return uids
}

func inferOnlineCopySourceNode(
	plan checkRecorder,
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
				domain.CheckNameSourceNode,
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
				domain.CheckNameSourceNode,
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
				domain.CheckNameSourceNodeInference,
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

func isRiskRole(role string) bool {
	switch strings.ToLower(role) {
	case "leader", "primary", "master", "unknown":
		return true
	default:
		return false
	}
}

func kubeBlocksRoleWarning(controllerKind string, spec *v1alpha1.KubeBlocksSpec) string {
	if spec == nil || controllerKind != domain.KindInstanceSet || !isRiskRole(spec.Role) {
		return ""
	}

	if spec.Role == "unknown" && spec.SwitchoverCandidate == "" {
		return "selected KubeBlocks instance role is unknown; possible leader downtime was explicitly acknowledged"
	}

	if spec.SwitchoverCandidate == "" {
		return fmt.Sprintf(
			"selected KubeBlocks instance role=%s; leader downtime was explicitly acknowledged",
			spec.Role,
		)
	}

	if spec.SwitchoverStrategy == v1alpha1.KubeBlocksSwitchoverMongoDBNative {
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
	plan checkRecorder,
	pvc *corev1.PersistentVolumeClaim,
) {
	if pvc == nil {
		return
	}

	custom := kube.BlockingPVCFinalizers(pvc)
	if len(custom) == 0 {
		return
	}

	plan.AddCheck(
		failed(
			domain.CheckNamePVCFinalizers,
			fmt.Sprintf(
				"PVC %s/%s has custom finalizer(s) %s; remove them or complete their controller cleanup before an operation that recreates the PVC",
				pvc.Namespace,
				pvc.Name,
				strings.Join(custom, ", "),
			),
		),
	)
}
