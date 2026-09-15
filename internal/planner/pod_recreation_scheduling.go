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
			recreatedSpec.TopologySpreadConstraints,
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
	constraints []corev1.TopologySpreadConstraint,
) []string {
	issues := make([]string, 0)

	for _, constraint := range constraints {
		if constraint.WhenUnsatisfiable != corev1.DoNotSchedule {
			continue
		}

		selector, selErr := metav1.LabelSelectorAsSelector(constraint.LabelSelector)
		if selErr != nil {
			issues = append(issues, fmt.Sprintf(
				"topologySpread constraint %q has an invalid label selector: %v",
				constraint.TopologyKey, selErr,
			))

			continue
		}

		if issue := topologySpreadSkewIssue(
			ctx, client, otherPods, sourcePod, targetNode, constraint, selector,
		); issue != "" {
			issues = append(issues, issue)
		}
	}

	return issues
}

func topologySpreadSkewIssue(
	ctx context.Context,
	client kubernetes.Interface,
	otherPods []corev1.Pod,
	sourcePod *corev1.Pod,
	targetNode string,
	constraint corev1.TopologySpreadConstraint,
	selector labels.Selector,
) string {
	counts := map[string]int{}
	for _, other := range otherPods {
		if other.Name == sourcePod.Name || !selector.Matches(labels.Set(other.Labels)) {
			continue
		}

		domain := nodeTopologyValue(ctx, client, other.Spec.NodeName, constraint.TopologyKey)
		counts[domain]++
	}

	targetDomain := nodeTopologyValue(ctx, client, targetNode, constraint.TopologyKey)
	counts[targetDomain]++ // the recreated Pod lands in the target domain

	minCount := -1

	maxCount := -1
	for _, count := range counts {
		if minCount == -1 || count < minCount {
			minCount = count
		}

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
