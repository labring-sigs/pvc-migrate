package planner

import (
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

func (p *Planner) checkSharedRWOScheduling(state *planState) {
	if !state.options.OpenEBSLVMEnableShared || state.sourcePod == nil || state.targetNode == nil {
		return
	}

	for _, volume := range state.plannedVolumes {
		if !plannedVolumeRequiresSharedCollocation(volume) {
			continue
		}

		if state.inventory.namespacePodsErr != nil {
			state.plan.AddCheck(failed(
				"destination-shared-scheduling",
				fmt.Sprintf(
					"cannot verify same-node consumers for PVC %s/%s: list Pods: %v",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					state.inventory.namespacePodsErr,
				),
			))

			continue
		}

		issues := sharedRWOCollocationIssues(
			state.workload,
			state.sourcePod,
			volume.SourcePVC.Name,
			state.inventory.namespacePods,
			state.inventory.sourceNamespace,
			state.inventory.sourceNamespaceErr,
			state.targetNode,
			state.inventory.nodes,
			state.inventory.nodesErr,
		)
		if len(issues) > 0 {
			state.plan.AddCheck(failed(
				"destination-shared-scheduling",
				fmt.Sprintf(
					"OpenEBS LVM shared RWO PVC %s/%s requires all %d consumers on node %s: %s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					volume.ConcurrentConsumers,
					state.targetNode.Name,
					issues[0],
				),
			))

			continue
		}

		state.plan.AddCheck(passed(
			"destination-shared-scheduling",
			fmt.Sprintf(
				"%d consumers of OpenEBS LVM shared RWO PVC %s/%s can run together on node %s",
				volume.ConcurrentConsumers,
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
				state.targetNode.Name,
			),
		))
	}
}

func plannedVolumeRequiresSharedCollocation(volume domain.PlannedVolume) bool {
	return volume.ConcurrentConsumers > 1 &&
		slices.Contains(volume.AccessModes, corev1.ReadWriteOnce) &&
		volume.CSIProvisioner == kube.OpenEBSLVMCSIDriver
}

func sharedRWOCollocationIssues(
	workload domain.WorkloadSpec,
	selected *corev1.Pod,
	pvcName string,
	pods []corev1.Pod,
	namespace *corev1.Namespace,
	namespaceErr error,
	targetNode *corev1.Node,
	nodes []corev1.Node,
	nodesErr error,
) []string {
	consumers := migrationUnitPVCConsumers(workload, selected, pvcName, pods)
	if len(consumers) < 2 {
		return nil
	}

	unitUIDs := workloadPodUIDs(workload, selected, selected.Namespace)
	for _, consumer := range consumers {
		if issue := requiredAntiAffinityCollocationIssue(
			consumer,
			consumers,
			namespace,
			namespaceErr,
			targetNode,
		); issue != "" {
			return []string{issue}
		}

		if issue := strictTopologySpreadCollocationIssue(
			consumer,
			consumers,
			pods,
			unitUIDs,
			targetNode,
			nodes,
			nodesErr,
		); issue != "" {
			return []string{issue}
		}
	}

	return nil
}

func migrationUnitPVCConsumers(
	workload domain.WorkloadSpec,
	selected *corev1.Pod,
	pvcName string,
	pods []corev1.Pod,
) []*corev1.Pod {
	unitUIDs := workloadPodUIDs(workload, selected, selected.Namespace)

	consumers := make([]*corev1.Pod, 0, len(unitUIDs))
	for index := range pods {
		pod := &pods[index]

		expectedUID, belongs := unitUIDs[pod.Name]
		if !belongs || expectedUID == "" || pod.UID != expectedUID ||
			!slices.Contains(podPVCNames(pod), pvcName) {
			continue
		}

		consumers = append(consumers, pod)
	}

	return consumers
}

func requiredAntiAffinityCollocationIssue(
	incoming *corev1.Pod,
	consumers []*corev1.Pod,
	namespace *corev1.Namespace,
	namespaceErr error,
	targetNode *corev1.Node,
) string {
	if incoming.Spec.Affinity == nil || incoming.Spec.Affinity.PodAntiAffinity == nil {
		return ""
	}

	for _, term := range incoming.Spec.Affinity.PodAntiAffinity.
		RequiredDuringSchedulingIgnoredDuringExecution {
		if _, present := targetNode.Labels[term.TopologyKey]; !present {
			continue
		}

		selector, err := affinityTermSelector(term, incoming.Labels)
		if err != nil {
			return fmt.Sprintf("required podAntiAffinity has an invalid selector: %v", err)
		}

		for _, candidate := range consumers {
			if candidate.UID == incoming.UID {
				continue
			}

			included, includeErr := affinityTermIncludesNamespace(
				term,
				incoming.Namespace,
				candidate.Namespace,
				namespace,
				namespaceErr,
			)
			if includeErr != nil {
				return fmt.Sprintf(
					"cannot evaluate required podAntiAffinity namespaceSelector: %v",
					includeErr,
				)
			}

			if included && selector.Matches(labels.Set(candidate.Labels)) {
				return fmt.Sprintf(
					"required podAntiAffinity on topology %q matches another PVC consumer",
					term.TopologyKey,
				)
			}
		}
	}

	return ""
}

func affinityTermIncludesNamespace(
	term corev1.PodAffinityTerm,
	incomingNamespace string,
	candidateNamespace string,
	namespace *corev1.Namespace,
	namespaceErr error,
) (bool, error) {
	if slices.Contains(term.Namespaces, candidateNamespace) {
		return true, nil
	}

	if len(term.Namespaces) == 0 && term.NamespaceSelector == nil {
		return incomingNamespace == candidateNamespace, nil
	}

	if term.NamespaceSelector == nil {
		return false, nil
	}

	if namespaceErr != nil {
		return false, namespaceErr
	}

	if namespace == nil || namespace.Name != candidateNamespace {
		return false, fmt.Errorf("namespace %s inventory is unavailable", candidateNamespace)
	}

	selector, err := metav1.LabelSelectorAsSelector(term.NamespaceSelector)
	if err != nil {
		return false, err
	}

	return selector.Matches(labels.Set(namespace.Labels)), nil
}

func affinityTermSelector(
	term corev1.PodAffinityTerm,
	incomingLabels map[string]string,
) (labels.Selector, error) {
	return selectorWithPodLabelKeys(
		term.LabelSelector,
		term.MatchLabelKeys,
		term.MismatchLabelKeys,
		incomingLabels,
	)
}

func topologySpreadSelector(
	constraint corev1.TopologySpreadConstraint,
	incomingLabels map[string]string,
) (labels.Selector, error) {
	return selectorWithPodLabelKeys(
		constraint.LabelSelector,
		constraint.MatchLabelKeys,
		nil,
		incomingLabels,
	)
}

func selectorWithPodLabelKeys(
	base *metav1.LabelSelector,
	matchKeys []string,
	mismatchKeys []string,
	incomingLabels map[string]string,
) (labels.Selector, error) {
	if base == nil {
		return labels.Nothing(), nil
	}

	selector := base.DeepCopy()
	for _, key := range matchKeys {
		if value, present := incomingLabels[key]; present {
			selector.MatchExpressions = append(
				selector.MatchExpressions,
				metav1.LabelSelectorRequirement{
					Key: key, Operator: metav1.LabelSelectorOpIn, Values: []string{value},
				},
			)
		}
	}

	for _, key := range mismatchKeys {
		if value, present := incomingLabels[key]; present {
			selector.MatchExpressions = append(
				selector.MatchExpressions,
				metav1.LabelSelectorRequirement{
					Key: key, Operator: metav1.LabelSelectorOpNotIn, Values: []string{value},
				},
			)
		}
	}

	return metav1.LabelSelectorAsSelector(selector)
}

func strictTopologySpreadCollocationIssue(
	incoming *corev1.Pod,
	consumers []*corev1.Pod,
	pods []corev1.Pod,
	unitUIDs map[string]types.UID,
	targetNode *corev1.Node,
	nodes []corev1.Node,
	nodesErr error,
) string {
	for _, constraint := range incoming.Spec.TopologySpreadConstraints {
		if constraint.WhenUnsatisfiable != corev1.DoNotSchedule {
			continue
		}

		if nodesErr != nil {
			return fmt.Sprintf(
				"cannot evaluate topologySpread constraints: list Nodes: %v",
				nodesErr,
			)
		}

		selector, err := topologySpreadSelector(constraint, incoming.Labels)
		if err != nil {
			return fmt.Sprintf("topologySpread constraint has an invalid selector: %v", err)
		}

		matchingConsumers := 0
		for _, consumer := range consumers {
			if selector.Matches(labels.Set(consumer.Labels)) {
				matchingConsumers++
			}
		}

		if matchingConsumers < 2 {
			continue
		}

		domains, nodeDomains := topologySpreadDomains(incoming.Spec, constraint, nodes)

		targetDomain, targetEligible := nodeDomains[targetNode.Name]
		if !targetEligible {
			continue
		}

		counts := make(map[string]int, len(domains))
		for domain := range domains {
			counts[domain] = 0
		}

		for index := range pods {
			pod := &pods[index]
			if pod.Spec.NodeName == "" || pod.DeletionTimestamp != nil ||
				pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed ||
				migrationUnitContainsPod(unitUIDs, pod) ||
				!selector.Matches(labels.Set(pod.Labels)) {
				continue
			}

			if domain, eligible := nodeDomains[pod.Spec.NodeName]; eligible {
				counts[domain]++
			}
		}

		counts[targetDomain] += matchingConsumers - 1
		globalMinimum := topologySpreadGlobalMinimum(counts, constraint.MinDomains)

		prospectiveTargetCount := counts[targetDomain] + 1
		if prospectiveTargetCount-globalMinimum > int(constraint.MaxSkew) {
			return fmt.Sprintf(
				"DoNotSchedule topologySpread on %q permits maxSkew %d, but co-location would produce skew %d",
				constraint.TopologyKey,
				constraint.MaxSkew,
				prospectiveTargetCount-globalMinimum,
			)
		}
	}

	return ""
}

func topologySpreadDomains(
	spec corev1.PodSpec,
	constraint corev1.TopologySpreadConstraint,
	nodes []corev1.Node,
) (map[string]struct{}, map[string]string) {
	domains := make(map[string]struct{})

	nodeDomains := make(map[string]string)
	for index := range nodes {
		node := &nodes[index]

		domain, present := node.Labels[constraint.TopologyKey]
		if !present || !kube.NodeReadyAndSchedulable(node) ||
			!nodeEligibleForTopologySpread(spec, constraint, node) {
			continue
		}

		domains[domain] = struct{}{}
		nodeDomains[node.Name] = domain
	}

	return domains, nodeDomains
}

func nodeEligibleForTopologySpread(
	spec corev1.PodSpec,
	constraint corev1.TopologySpreadConstraint,
	node *corev1.Node,
) bool {
	if constraint.NodeAffinityPolicy == nil ||
		*constraint.NodeAffinityPolicy == corev1.NodeInclusionPolicyHonor {
		for key, expected := range spec.NodeSelector {
			if node.Labels[key] != expected {
				return false
			}
		}

		if affinity := spec.Affinity; affinity != nil && affinity.NodeAffinity != nil &&
			affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			matched := false
			for _, term := range affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.
				NodeSelectorTerms {
				if nodeSelectorTermMatches(term, node) {
					matched = true
					break
				}
			}

			if !matched {
				return false
			}
		}
	}

	if constraint.NodeTaintsPolicy != nil &&
		*constraint.NodeTaintsPolicy == corev1.NodeInclusionPolicyHonor {
		for _, taint := range node.Spec.Taints {
			if (taint.Effect == corev1.TaintEffectNoSchedule ||
				taint.Effect == corev1.TaintEffectNoExecute) &&
				!tolerates(spec.Tolerations, taint) {
				return false
			}
		}
	}

	return true
}

func migrationUnitContainsPod(unitUIDs map[string]types.UID, pod *corev1.Pod) bool {
	expectedUID, belongs := unitUIDs[pod.Name]
	return belongs && expectedUID != "" && expectedUID == pod.UID
}

func topologySpreadGlobalMinimum(counts map[string]int, minDomains *int32) int {
	minimumDomains := int32(1)
	if minDomains != nil {
		minimumDomains = *minDomains
	}

	if len(counts) < int(minimumDomains) {
		return 0
	}

	minimum := -1
	for _, count := range counts {
		if minimum == -1 || count < minimum {
			minimum = count
		}
	}

	if minimum < 0 {
		return 0
	}

	return minimum
}
