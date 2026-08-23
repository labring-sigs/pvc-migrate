package kube

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestResourceReadiness(t *testing.T) {
	readyNode := &corev1.Node{
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:   corev1.NodeReady,
			Status: corev1.ConditionTrue,
		}}},
	}
	if !NodeReadyAndSchedulable(readyNode) {
		t.Fatal("ready node should be schedulable")
	}

	readyNode.Spec.Unschedulable = true
	if !NodeReady(readyNode) || NodeReadyAndSchedulable(readyNode) {
		t.Fatal("cordoned node should remain Ready without being schedulable")
	}

	readyPod := &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	}}}}
	if !PodReady(readyPod) || PodReady(nil) || NodeReady(nil) {
		t.Fatal("resource readiness did not handle Ready and nil resources")
	}
}

func TestStorageClassAllowsNode(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "worker-a",
		Labels: map[string]string{"topology.example.io/zone": "zone-a"},
	}}

	if !StorageClassAllowsNode(&storagev1.StorageClass{}, node) {
		t.Fatal("StorageClass without allowed topologies should allow every node")
	}

	sc := &storagev1.StorageClass{AllowedTopologies: []corev1.TopologySelectorTerm{{
		MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{
			Key:    "topology.example.io/zone",
			Values: []string{"zone-a", "zone-b"},
		}},
	}}}
	if !StorageClassAllowsNode(sc, node) {
		t.Fatal("matching node should satisfy allowed topologies")
	}

	node.Labels["topology.example.io/zone"] = "zone-c"
	if StorageClassAllowsNode(sc, node) || StorageClassAllowsNode(nil, node) ||
		StorageClassAllowsNode(sc, nil) {
		t.Fatal("mismatched or empty resources should not satisfy allowed topologies")
	}
}
