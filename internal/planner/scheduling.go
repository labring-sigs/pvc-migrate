package planner

import (
	"fmt"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

func podMigrationIssues(spec corev1.PodSpec, sourceNode, targetNode string) []string {
	issues := make([]string, 0)
	if spec.SchedulerName != "" && spec.SchedulerName != "default-scheduler" {
		issues = append(issues, fmt.Sprintf("custom scheduler %q cannot guarantee the requested target node", spec.SchedulerName))
	}
	for _, volume := range spec.Volumes {
		switch {
		case volume.HostPath != nil && sourceNode != targetNode:
			issues = append(issues, fmt.Sprintf("hostPath volume %q is node-local and source/target nodes differ", volume.Name))
		case volume.Ephemeral != nil:
			issues = append(issues, fmt.Sprintf("generic ephemeral volume %q is recreated empty", volume.Name))
		}
	}
	if sourceNode != targetNode && spec.Affinity != nil {
		if spec.Affinity.PodAffinity != nil && len(spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0 {
			issues = append(issues, "required podAffinity depends on co-located Pods during recreation")
		}
		if spec.Affinity.PodAntiAffinity != nil && len(spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0 {
			issues = append(issues, "required podAntiAffinity depends on the existing Pod layout during recreation")
		}
	}
	if sourceNode != targetNode {
		for _, constraint := range spec.TopologySpreadConstraints {
			if constraint.WhenUnsatisfiable == corev1.DoNotSchedule {
				issues = append(issues, fmt.Sprintf("topologySpread constraint %q can reject the recreated Pod on the target node", constraint.TopologyKey))
			}
		}
	}
	return issues
}

func schedulingIssues(spec corev1.PodSpec, node *corev1.Node) []string {
	issues := make([]string, 0)
	keys := make([]string, 0, len(spec.NodeSelector))
	for key := range spec.NodeSelector {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		expected := spec.NodeSelector[key]
		if node.Labels[key] != expected {
			issues = append(issues, fmt.Sprintf("nodeSelector %s=%s is unsatisfied", key, expected))
		}
	}
	if affinity := spec.Affinity; affinity != nil && affinity.NodeAffinity != nil && affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		selector := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		matched := false
		for _, term := range selector.NodeSelectorTerms {
			if nodeSelectorTermMatches(term, node) {
				matched = true
				break
			}
		}
		if !matched {
			issues = append(issues, "required nodeAffinity is unsatisfied")
		}
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if !tolerates(spec.Tolerations, taint) {
			issues = append(issues, fmt.Sprintf("taint %s=%s:%s is untolerated", taint.Key, taint.Value, taint.Effect))
		}
	}
	return issues
}

// resourceFitIssues checks constraints that can be proven from the Pod and
// Node objects alone. Current cluster usage remains dynamic, so an unknown
// allocatable entry is reported to the caller as an unverifiable fit.
func resourceFitIssues(spec corev1.PodSpec, node *corev1.Node) (issues []string, known bool) {
	requests := podResourceRequests(spec)
	if len(requests) == 0 {
		return nil, true
	}
	if len(node.Status.Allocatable) == 0 {
		return nil, false
	}
	known = true
	for name, request := range requests {
		allocatable, ok := node.Status.Allocatable[name]
		if !ok {
			known = false
			continue
		}
		if request.Cmp(allocatable) > 0 {
			issues = append(issues, fmt.Sprintf("Pod resource request %s=%s exceeds node allocatable %s=%s", name, request.String(), name, allocatable.String()))
		}
	}
	sort.SliceStable(issues, func(i, j int) bool { return issues[i] < issues[j] })
	return issues, known
}

func podResourceRequests(spec corev1.PodSpec) corev1.ResourceList {
	requests := corev1.ResourceList{}
	for _, container := range spec.Containers {
		addResourceList(requests, container.Resources.Requests)
	}
	for _, container := range spec.InitContainers {
		for name, request := range container.Resources.Requests {
			current, ok := requests[name]
			if !ok || request.Cmp(current) > 0 {
				requests[name] = request.DeepCopy()
			}
		}
	}
	addResourceList(requests, spec.Overhead)
	return requests
}

func addResourceList(target, source corev1.ResourceList) {
	for name, value := range source {
		current, ok := target[name]
		if !ok {
			target[name] = value.DeepCopy()
			continue
		}
		current.Add(value)
		target[name] = current
	}
}

func nodeSelectorTermMatches(term corev1.NodeSelectorTerm, node *corev1.Node) bool {
	for _, requirement := range term.MatchExpressions {
		if !selectorRequirementMatches(requirement, node.Labels[requirement.Key], node.Labels) {
			return false
		}
	}
	for _, requirement := range term.MatchFields {
		actual := ""
		if requirement.Key == "metadata.name" {
			actual = node.Name
		}
		if !selectorRequirementMatches(requirement, actual, nil) {
			return false
		}
	}
	return true
}

func selectorRequirementMatches(requirement corev1.NodeSelectorRequirement, actual string, all map[string]string) bool {
	_, exists := all[requirement.Key]
	if all == nil {
		exists = actual != ""
	}
	switch requirement.Operator {
	case corev1.NodeSelectorOpIn:
		return exists && contains(requirement.Values, actual)
	case corev1.NodeSelectorOpNotIn:
		return !exists || !contains(requirement.Values, actual)
	case corev1.NodeSelectorOpExists:
		return exists
	case corev1.NodeSelectorOpDoesNotExist:
		return !exists
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		if len(requirement.Values) != 1 {
			return false
		}
		left, leftErr := strconv.ParseInt(actual, 10, 64)
		right, rightErr := strconv.ParseInt(requirement.Values[0], 10, 64)
		if leftErr != nil || rightErr != nil {
			return false
		}
		if requirement.Operator == corev1.NodeSelectorOpGt {
			return left > right
		}
		return left < right
	default:
		return false
	}
}

func tolerates(tolerations []corev1.Toleration, taint corev1.Taint) bool {
	for _, toleration := range tolerations {
		if toleration.Effect != "" && toleration.Effect != taint.Effect {
			continue
		}
		if toleration.Operator == corev1.TolerationOpExists {
			if toleration.Key == "" || toleration.Key == taint.Key {
				return true
			}
			continue
		}
		if toleration.Key == taint.Key && toleration.Value == taint.Value {
			return true
		}
	}
	return false
}
