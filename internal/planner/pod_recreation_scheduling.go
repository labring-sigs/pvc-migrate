package planner

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// recreationSchedulingIssues evaluates whether the recreated Pod could
// actually be scheduled on the target node, the way kube-scheduler would:
// required podAffinity, required podAntiAffinity, and DoNotSchedule topology
// spread constraints are checked against the OTHER live Pods that will still
// exist after the source Pod is paused and deleted — the source Pod itself
// never competes with its own recreation. A constraint that the recreated Pod
// can satisfy is not an issue, even when source and target nodes differ.
func recreationSchedulingIssues(
	ctx context.Context,
	client kubernetes.Interface,
	sourcePod *corev1.Pod,
	targetNode string,
	recreatedSpec corev1.PodSpec,
) []string {
	hasAffinity, hasAntiAffinity, hasSpread := placementConstraintKinds(recreatedSpec)
	if !hasAffinity && !hasAntiAffinity && !hasSpread {
		return nil
	}

	otherPods, err := client.CoreV1().Pods(sourcePod.Namespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		// The layout cannot be verified; keep the conservative legacy warning
		// so the operator can judge instead of silently skipping the check.
		return []string{
			fmt.Sprintf(
				"pod placement constraints depend on the existing Pod layout during recreation (cannot list Pods: %v)",
				err,
			),
		}
	}

	issues := make([]string, 0)

	if hasAffinity {
		issues = append(issues, podAffinityIssuesForRecreation(
			ctx, client, otherPods.Items, sourcePod, targetNode,
			recreatedSpec.Affinity.PodAffinity.
				RequiredDuringSchedulingIgnoredDuringExecution,
		)...)
	}

	if hasAntiAffinity {
		issues = append(issues, podAntiAffinityIssuesForRecreation(
			ctx, client, otherPods.Items, sourcePod, targetNode,
			recreatedSpec.Affinity.PodAntiAffinity.
				RequiredDuringSchedulingIgnoredDuringExecution,
		)...)
	}

	if hasSpread {
		issues = append(issues, topologySpreadIssuesForRecreation(
			ctx, client, otherPods.Items, sourcePod, targetNode,
			recreatedSpec,
		)...)
	}

	return issues
}

func podAffinityIssuesForRecreation(
	ctx context.Context,
	client kubernetes.Interface,
	otherPods []corev1.Pod,
	sourcePod *corev1.Pod,
	targetNode string,
	terms []corev1.PodAffinityTerm,
) []string {
	issues := make([]string, 0)

	// The recreated Pod must share a topology domain with a live Pod matching
	// each term. When every matching live Pod sits outside the target domain,
	// placement there violates the term.
	for _, term := range terms {
		selector, selErr := metav1.LabelSelectorAsSelector(term.LabelSelector)
		if selErr != nil {
			issues = append(issues, fmt.Sprintf(
				"required podAffinity term has an invalid label selector: %v",
				selErr,
			))

			continue
		}

		domainHasMatch := false
		matchedElsewhere := 0

		for _, other := range otherPods {
			if other.Name == sourcePod.Name || !selector.Matches(labels.Set(other.Labels)) {
				continue
			}

			topologyValue := nodeTopologyValue(
				ctx, client, other.Spec.NodeName, term.TopologyKey,
			)
			if topologyValue == nodeTopologyValue(
				ctx, client, targetNode, term.TopologyKey,
			) {
				domainHasMatch = true

				break
			}

			matchedElsewhere++
		}

		if !domainHasMatch && matchedElsewhere > 0 {
			issues = append(issues, fmt.Sprintf(
				"required podAffinity cannot be satisfied on the target node: %d matching live Pod(s) run outside topology %s=%s",
				matchedElsewhere,
				term.TopologyKey,
				nodeTopologyValue(ctx, client, targetNode, term.TopologyKey),
			))
		}
	}

	return issues
}

func podAntiAffinityIssuesForRecreation(
	ctx context.Context,
	client kubernetes.Interface,
	otherPods []corev1.Pod,
	sourcePod *corev1.Pod,
	targetNode string,
	terms []corev1.PodAffinityTerm,
) []string {
	issues := make([]string, 0)

	for _, term := range terms {
		selector, selErr := metav1.LabelSelectorAsSelector(term.LabelSelector)
		if selErr != nil {
			issues = append(issues, fmt.Sprintf(
				"required podAntiAffinity term has an invalid label selector: %v",
				selErr,
			))

			continue
		}

		targetDomain := nodeTopologyValue(ctx, client, targetNode, term.TopologyKey)

		for _, other := range otherPods {
			if other.Name == sourcePod.Name || !selector.Matches(labels.Set(other.Labels)) {
				continue
			}

			if nodeTopologyValue(
				ctx,
				client,
				other.Spec.NodeName,
				term.TopologyKey,
			) == targetDomain {
				issues = append(issues, fmt.Sprintf(
					"required podAntiAffinity conflicts with live Pod %s/%s in topology %s=%s",
					other.Namespace, other.Name, term.TopologyKey, targetDomain,
				))

				break
			}
		}
	}

	return issues
}

func topologySpreadIssuesForRecreation(
	ctx context.Context,
	client kubernetes.Interface,
	otherPods []corev1.Pod,
	sourcePod *corev1.Pod,
	targetNode string,
	recreatedSpec corev1.PodSpec,
) []string {
	hard := make([]corev1.TopologySpreadConstraint, 0, len(recreatedSpec.TopologySpreadConstraints))
	for _, constraint := range recreatedSpec.TopologySpreadConstraints {
		if constraint.WhenUnsatisfiable == corev1.DoNotSchedule {
			hard = append(hard, constraint)
		}
	}

	if len(hard) == 0 {
		return nil
	}

	// kube-scheduler evaluates skew over the eligible domains of the whole
	// cluster, including domains that currently hold zero matching Pods.
	// Counting only the domains of matching Pods hides empty domains, lets a
	// violating placement pass planning, and leaves the recreated Pod
	// unschedulable after cutover.
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return []string{
			fmt.Sprintf(
				"topologySpread constraints depend on the cluster's eligible topology domains during recreation (cannot list Nodes: %v)",
				err,
			),
		}
	}

	issues := make([]string, 0)
	for _, constraint := range hard {
		selector, selErr := metav1.LabelSelectorAsSelector(constraint.LabelSelector)
		if selErr != nil {
			issues = append(issues, fmt.Sprintf(
				"topologySpread constraint %q has an invalid label selector: %v",
				constraint.TopologyKey, selErr,
			))

			continue
		}

		eligible, domains := spreadDomainsForConstraint(nodes.Items, recreatedSpec, constraint)

		if issue := topologySpreadSkewIssue(
			ctx, client, otherPods, sourcePod, targetNode,
			constraint, selector, eligible, domains,
		); issue != "" {
			issues = append(issues, issue)
		}
	}

	return issues
}

// spreadDomainsForConstraint mirrors kube-scheduler's eligible-domain set:
// a node counts only when it carries every declared constraint's topologyKey
// (the scheduler bypasses nodes missing any of them), satisfies the recreated
// Pod's nodeSelector and required nodeAffinity (nodeAffinityPolicy defaults
// to Honor), and — only when the constraint opts in with nodeTaintsPolicy
// Honor, whose default is Ignore — tolerates the node's hard taints.
func spreadDomainsForConstraint(
	nodes []corev1.Node,
	recreatedSpec corev1.PodSpec,
	constraint corev1.TopologySpreadConstraint,
) (eligible []corev1.Node, domains map[string]string) {
	requiredKeys := make([]string, 0, len(recreatedSpec.TopologySpreadConstraints))
	for _, other := range recreatedSpec.TopologySpreadConstraints {
		if other.TopologyKey != "" {
			requiredKeys = append(requiredKeys, other.TopologyKey)
		}
	}

	honorTaints := constraint.NodeTaintsPolicy != nil &&
		*constraint.NodeTaintsPolicy == corev1.NodeInclusionPolicyHonor

	eligible = make([]corev1.Node, 0, len(nodes))

	domains = make(map[string]string, len(nodes))
	for i := range nodes {
		node := &nodes[i]

		if node.Labels[constraint.TopologyKey] == "" {
			continue
		}

		missingKey := false
		for _, key := range requiredKeys {
			if node.Labels[key] == "" {
				missingKey = true
				break
			}
		}

		if missingKey {
			continue
		}

		if !nodeMatchesPodNodeAffinity(recreatedSpec, node) {
			continue
		}

		if honorTaints && !nodeToleratesHardTaints(recreatedSpec, node) {
			continue
		}

		eligible = append(eligible, *node)
		domains[node.Name] = node.Labels[constraint.TopologyKey]
	}

	return eligible, domains
}

func topologySpreadSkewIssue(
	ctx context.Context,
	client kubernetes.Interface,
	otherPods []corev1.Pod,
	sourcePod *corev1.Pod,
	targetNode string,
	constraint corev1.TopologySpreadConstraint,
	selector labels.Selector,
	eligibleNodes []corev1.Node,
	nodeDomains map[string]string,
) string {
	domainOf := func(nodeName string) string {
		if value := nodeDomains[nodeName]; value != "" {
			return value
		}

		// A Pod on a node outside the eligible set still occupies its domain;
		// kube-scheduler counts those Pods even though the domain is not a
		// placement candidate, so the maximum must include them.
		return nodeTopologyValue(ctx, client, nodeName, constraint.TopologyKey)
	}

	counts := make(map[string]int, len(nodeDomains))
	for _, node := range eligibleNodes {
		counts[nodeDomains[node.Name]] = 0
	}

	for _, other := range otherPods {
		if other.Name == sourcePod.Name || !selector.Matches(labels.Set(other.Labels)) {
			continue
		}

		counts[domainOf(other.Spec.NodeName)]++
	}

	targetDomain := domainOf(targetNode)
	counts[targetDomain]++ // the recreated Pod lands in the target domain

	// minDomains semantics: below the threshold the global minimum is treated
	// as zero, exactly like the scheduler's own calculation.
	minDomains := 1
	if constraint.MinDomains != nil && *constraint.MinDomains > 1 {
		minDomains = int(*constraint.MinDomains)
	}

	minCount := 0
	if minDomains <= 1 || len(nodeDomains) >= minDomains {
		minCount = -1

		for _, count := range counts {
			if minCount == -1 || count < minCount {
				minCount = count
			}
		}
	}

	maxCount := -1
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}

	maxSkew := 1
	if constraint.MaxSkew > 0 {
		maxSkew = int(constraint.MaxSkew)
	}

	if maxCount-minCount > maxSkew {
		return fmt.Sprintf(
			"topologySpread constraint %q cannot be satisfied on the target node (topology %s would hold %d Pods vs minimum %d, maxSkew %d)",
			constraint.TopologyKey,
			targetDomain,
			counts[targetDomain],
			minCount,
			maxSkew,
		)
	}

	return ""
}

// nodeMatchesPodNodeAffinity reports whether the node satisfies the spec's
// nodeSelector and required nodeAffinity — the node-level constraints whose
// default nodeAffinityPolicy Honor restricts eligible spread domains.
func nodeMatchesPodNodeAffinity(spec corev1.PodSpec, node *corev1.Node) bool {
	for key, expected := range spec.NodeSelector {
		if node.Labels[key] != expected {
			return false
		}
	}

	if affinity := spec.Affinity; affinity != nil && affinity.NodeAffinity != nil &&
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		selector := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution

		matched := false
		for _, term := range selector.NodeSelectorTerms {
			if nodeSelectorTermMatches(term, node) {
				matched = true
				break
			}
		}

		if !matched {
			return false
		}
	}

	return true
}

func nodeToleratesHardTaints(spec corev1.PodSpec, node *corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Effect != corev1.TaintEffectNoSchedule &&
			taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}

		if !tolerates(spec.Tolerations, taint) {
			return false
		}
	}

	return true
}

// placementConstraintKinds reports which scheduling constraint families the
// recreated Pod declares.
func placementConstraintKinds(
	recreatedSpec corev1.PodSpec,
) (hasAffinity, hasAntiAffinity, hasSpread bool) {
	if recreatedSpec.Affinity != nil {
		if recreatedSpec.Affinity.PodAffinity != nil {
			hasAffinity = len(
				recreatedSpec.Affinity.PodAffinity.
					RequiredDuringSchedulingIgnoredDuringExecution,
			) > 0
		}

		if recreatedSpec.Affinity.PodAntiAffinity != nil {
			hasAntiAffinity = len(
				recreatedSpec.Affinity.PodAntiAffinity.
					RequiredDuringSchedulingIgnoredDuringExecution,
			) > 0
		}
	}

	for _, constraint := range recreatedSpec.TopologySpreadConstraints {
		if constraint.WhenUnsatisfiable == corev1.DoNotSchedule {
			hasSpread = true

			break
		}
	}

	return hasAffinity, hasAntiAffinity, hasSpread
}

func nodeTopologyValue(
	ctx context.Context,
	client kubernetes.Interface,
	nodeName, topologyKey string,
) string {
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nodeName
	}

	if value := node.Labels[topologyKey]; value != "" {
		return value
	}

	return nodeName
}
