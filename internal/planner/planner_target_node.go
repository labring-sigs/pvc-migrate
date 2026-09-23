package planner

import (
	"context"
	"fmt"
	"slices"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (p *Planner) selectPlanTarget(
	state *planState,
	workloadKind v1alpha1.WorkloadKind,
	migratingPod *corev1.Pod,
	sourceZone string,
) {
	if state.autoTargetNode && len(state.plannedVolumes) > 0 {
		state.targetNode = p.selectTargetNodeFromNodesWithZone(
			state.plan,
			workloadKind,
			migratingPod,
			state.options.SourceNode,
			state.plannedVolumes,
			state.sourcePVs,
			state.storageClasses,
			planCapacityInventory(state),
			state.inventory.nodes,
			state.inventory.nodesErr,
			sourceZone,
		)
		if state.targetNode != nil {
			state.options.TargetNode = state.targetNode.Name
			state.plan.TargetNode = state.targetNode.Name
		}
	}
}

func (p *Planner) finalizePlanTarget(
	ctx context.Context,
	state *planState,
	capacityInventory *storageCapacityInventory,
) {
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
		validCapacityAwareness(domain.CapacityAwareness(state.options.CapacityAwareness)) &&
		state.options.CapacityAwareness != string(domain.CapacityAwarenessOff) {
		p.checkStorageCapacity(
			state.plan,
			&state.plan.StorageCapacity,
			state.targetNode,
			state.plannedVolumes,
			capacityInventory,
			domain.CapacityAwareness(state.options.CapacityAwareness),
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

		if !kube.StorageClassAllowsNode(sc, state.targetNode) {
			state.plan.AddCheck(failed(
				domain.CheckNameStorageTopology,
				fmt.Sprintf(
					"node %s does not satisfy StorageClass %s allowedTopologies",
					state.targetNode.Name, sc.Name,
				),
			))
		} else {
			state.plan.AddCheck(passed(
				domain.CheckNameStorageTopology,
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
		state.plan.AddCheck(
			failed(domain.CheckNameSourceNode, fmt.Sprintf("read source node: %v", err)),
		)
	case node == nil || node.Name == "":
		state.plan.AddCheck(
			failed(domain.CheckNameSourceNode, "read source node returned an empty object"),
		)
	case !kube.NodeReadyAndSchedulable(node):
		state.plan.AddCheck(failed(
			domain.CheckNameSourceNode,
			fmt.Sprintf("node %s must be Ready and schedulable for the source tool", node.Name),
		))
	default:
		for _, volume := range state.plannedVolumes {
			pv := state.sourcePVs[volume.SourcePV.Name]
			if pv != nil && !kube.PVSupportsNode(pv, node) {
				state.plan.AddCheck(failed(
					domain.CheckNameSourceNode,
					fmt.Sprintf(
						"source PV %s node affinity does not allow source node %s",
						pv.Name,
						node.Name,
					),
				))

				return
			}
		}

		state.plan.AddCheck(
			passed(
				domain.CheckNameSourceNode,
				fmt.Sprintf("node %s is Ready and schedulable", node.Name),
			),
		)
	}
}

func isAutoNode(value string) bool {
	return value == "" || strings.EqualFold(value, domain.AutoValue)
}

func (p *Planner) checkPodTargetScheduling(
	plan checkRecorder,
	sourcePod *corev1.Pod,
	workloadKind v1alpha1.WorkloadKind,
	node *corev1.Node,
) {
	if sourcePod == nil {
		return
	}

	schedulingSpec := sourcePod.Spec
	if workloadKind == v1alpha1.WorkloadStandalone {
		schedulingSpec = *sourcePod.Spec.DeepCopy()
		// The standalone adapter clears both direct and hostname placement from
		// the recreated Pod before applying the selected target node.
		schedulingSpec.NodeName = ""
		if hostname := schedulingSpec.NodeSelector[corev1.LabelHostname]; hostname != "" &&
			hostname != node.Labels[corev1.LabelHostname] {
			delete(schedulingSpec.NodeSelector, corev1.LabelHostname)
			plan.AddCheck(
				warned(
					domain.CheckNamePodScheduling,
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
		plan.AddCheck(failed(domain.CheckNamePodScheduling, strings.Join(issues, "; ")))
	} else {
		plan.AddCheck(
			passed(
				domain.CheckNamePodScheduling,
				"target node satisfies nodeSelector, required nodeAffinity, and taints",
			),
		)
	}

	if workloadKind == v1alpha1.WorkloadStandalone {
		resourceIssues, known := resourceFitIssues(schedulingSpec, node)
		if len(resourceIssues) > 0 {
			plan.AddCheck(failed(domain.CheckNamePodResources, strings.Join(resourceIssues, "; ")))
		} else if !known {
			plan.AddCheck(
				warned(
					domain.CheckNamePodResources,
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
	capacitySurplus apiresource.Quantity
	resourceUnknown int
}

type checkRecorder interface {
	AddCheck(check domain.Check)
}

type planCheckTask func(checkRecorder)

func (p *Planner) selectTargetNode(
	ctx context.Context,
	plan checkRecorder,
	workloadKind v1alpha1.WorkloadKind,
	sourcePod *corev1.Pod,
	sourceNode string,
	volumes []domain.PlannedVolume,
	capacityInventory *storageCapacityInventory,
) *corev1.Node {
	nodes, err := p.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if nodes == nil {
		return p.selectTargetNodeFromNodes(
			plan,
			workloadKind,
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
		workloadKind,
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
	plan checkRecorder,
	workloadKind v1alpha1.WorkloadKind,
	sourcePod *corev1.Pod,
	sourceNode string,
	volumes []domain.PlannedVolume,
	sourcePVs map[string]*corev1.PersistentVolume,
	storageClasses map[string]*storagev1.StorageClass,
	capacityInventory *storageCapacityInventory,
	nodes []corev1.Node,
	err error,
) *corev1.Node {
	return p.selectTargetNodeFromNodesWithZone(
		plan,
		workloadKind,
		sourcePod,
		sourceNode,
		volumes,
		sourcePVs,
		storageClasses,
		capacityInventory,
		nodes,
		err,
		"",
	)
}

func (p *Planner) selectTargetNodeFromNodesWithZone(
	plan checkRecorder,
	workloadKind v1alpha1.WorkloadKind,
	sourcePod *corev1.Pod,
	sourceNode string,
	volumes []domain.PlannedVolume,
	sourcePVs map[string]*corev1.PersistentVolume,
	storageClasses map[string]*storagev1.StorageClass,
	capacityInventory *storageCapacityInventory,
	nodes []corev1.Node,
	err error,
	sourceZone string,
) *corev1.Node {
	if len(volumes) == 0 {
		plan.AddCheck(
			failed(
				domain.CheckNameTargetNode,
				"target node auto-selection requires at least one valid source PVC",
			),
		)

		return nil
	}

	if err != nil {
		plan.AddCheck(
			failed(
				domain.CheckNameTargetNode,
				fmt.Sprintf("list nodes for auto-selection: %v", err),
			),
		)

		return nil
	}

	candidates := make([]targetNodeCandidate, 0, len(nodes))
	for i := range nodes {
		node := &nodes[i]

		candidate, ok := targetNodeCandidateFor(
			node,
			workloadKind,
			sourcePod,
			sourceNode,
			sourceZone,
			volumes,
			sourcePVs,
			storageClasses,
			capacityInventory,
		)
		if ok {
			candidates = append(candidates, candidate)
		}
	}

	if len(candidates) == 0 {
		message := "no Ready and schedulable node satisfies Pod scheduling and destination StorageClass topology"
		if capacityInventory != nil && capacityInventory.loaded &&
			len(capacityInventory.items) > 0 {
			message += " with sufficient CSI-reported capacity"
		}

		plan.AddCheck(failed(domain.CheckNameTargetNode, message))

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
			domain.CheckNameTargetNodeSelection,
			fmt.Sprintf(
				"auto selected target node %s (%s)",
				selected.node.Name,
				strings.Join(reasons, ", "),
			),
		),
	)

	return selected.node
}

func targetNodeCandidateFor(
	node *corev1.Node,
	workloadKind v1alpha1.WorkloadKind,
	sourcePod *corev1.Pod,
	sourceNode, sourceZone string,
	volumes []domain.PlannedVolume,
	sourcePVs map[string]*corev1.PersistentVolume,
	storageClasses map[string]*storagev1.StorageClass,
	capacityInventory *storageCapacityInventory,
) (targetNodeCandidate, bool) {
	if !kube.NodeReadyAndSchedulable(node) ||
		(sourceZone != "" && availabilityZone(node) != sourceZone) {
		return targetNodeCandidate{}, false
	}

	if sourcePod != nil && len(schedulingIssuesForTarget(sourcePod, workloadKind, node)) > 0 {
		return targetNodeCandidate{}, false
	}

	resourceUnknown := 0
	if sourcePod != nil && workloadKind == v1alpha1.WorkloadStandalone {
		spec := *sourcePod.Spec.DeepCopy()
		spec.NodeName = ""

		resourceIssues, known := resourceFitIssues(spec, node)
		if len(resourceIssues) > 0 {
			return targetNodeCandidate{}, false
		}

		if !known {
			resourceUnknown = 1
		}
	}

	if sourcePod != nil && len(podMigrationIssues(sourcePod.Spec, sourceNode, node.Name)) > 0 {
		return targetNodeCandidate{}, false
	}

	sourcePVMatches := 0
	for _, volume := range volumes {
		if sc := storageClasses[volume.StorageClass]; sc != nil &&
			!kube.StorageClassAllowsNode(sc, node) {
			return targetNodeCandidate{}, false
		}

		if pv := sourcePVs[volume.SourcePV.Name]; pv != nil && kube.PVSupportsNode(pv, node) {
			sourcePVMatches++
		}
	}

	capacityKnown, capacityUnknown, capacitySurplus, compatible := capacityScore(
		capacityInventory,
		node,
		volumes,
	)
	if !compatible {
		return targetNodeCandidate{}, false
	}

	return targetNodeCandidate{
		node:            node,
		distinctFromSrc: sourceNode == "" || node.Name != sourceNode,
		taintPenalty:    hardTaintCount(node),
		sourcePVMatches: sourcePVMatches,
		capacityKnown:   capacityKnown,
		capacityUnknown: capacityUnknown,
		capacitySurplus: capacitySurplus,
		resourceUnknown: resourceUnknown,
	}, true
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
	workloadKind v1alpha1.WorkloadKind,
	node *corev1.Node,
) []string {
	spec := sourcePod.Spec
	if workloadKind == v1alpha1.WorkloadStandalone {
		spec = *sourcePod.Spec.DeepCopy()

		spec.NodeName = ""
		if hostname := spec.NodeSelector[corev1.LabelHostname]; hostname != "" &&
			hostname != node.Labels[corev1.LabelHostname] {
			delete(spec.NodeSelector, corev1.LabelHostname)
		}
	}

	issues := schedulingIssues(spec, node)
	if workloadKind == v1alpha1.WorkloadStandalone {
		resourceIssues, _ := resourceFitIssues(spec, node)
		issues = append(issues, resourceIssues...)
	}

	return issues
}

func availabilityZone(node *corev1.Node) string {
	if node == nil {
		return ""
	}

	if zone := strings.TrimSpace(node.Labels[corev1.LabelTopologyZone]); zone != "" {
		return zone
	}

	return strings.TrimSpace(node.Labels[corev1.LabelFailureDomainBetaZone])
}
