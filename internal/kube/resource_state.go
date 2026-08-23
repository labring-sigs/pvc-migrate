package kube

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

// NodeReady reports whether the kubelet currently reports the node as Ready.
func NodeReady(node *corev1.Node) bool {
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

// NodeReadyAndSchedulable reports whether new Pods can target the node.
func NodeReadyAndSchedulable(node *corev1.Node) bool {
	return NodeReady(node) && !node.Spec.Unschedulable
}

// PodReady reports whether the Pod has a true Ready condition.
func PodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

// StorageClassAllowsNode reports whether a node satisfies one of the
// StorageClass allowed-topology terms.
func StorageClassAllowsNode(sc *storagev1.StorageClass, node *corev1.Node) bool {
	if sc == nil || node == nil {
		return false
	}

	if len(sc.AllowedTopologies) == 0 {
		return true
	}

	for _, term := range sc.AllowedTopologies {
		matches := true
		for _, expression := range term.MatchLabelExpressions {
			actual, exists := node.Labels[expression.Key]
			if !exists || !slices.Contains(expression.Values, actual) {
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
