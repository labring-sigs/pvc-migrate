package kube

import (
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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

	sc.AllowedTopologies = []corev1.TopologySelectorTerm{{}}
	if StorageClassAllowsNode(sc, node) {
		t.Fatal("empty topology selector term must match no node")
	}
}

func TestValidateStorageClassPlacement(t *testing.T) {
	readyNode := func(name, zone string) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					corev1.LabelHostname:     name,
					corev1.LabelTopologyZone: zone,
				},
			},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			}}},
		}
	}

	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "zonal"},
		AllowedTopologies: []corev1.TopologySelectorTerm{{
			MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{
				Key: corev1.LabelTopologyZone, Values: []string{"zone-b"},
			}},
		}},
	}
	nodeA := readyNode("node-a", "zone-a")
	nodeB := readyNode("node-b", "zone-b")
	client := fake.NewClientset(storageClass, nodeA, nodeB)

	if err := ValidateStorageClassPlacement(t.Context(), client, "zonal", "node-b"); err != nil {
		t.Fatalf("matching explicit target: %v", err)
	}

	err := ValidateStorageClassPlacement(t.Context(), client, "zonal", "node-a")
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "allowedTopologies") {
		t.Fatalf("mismatched explicit target category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := ValidateStorageClassPlacement(t.Context(), client, "zonal", ""); err != nil {
		t.Fatalf("scheduler-selected compatible target: %v", err)
	}

	nodeB.Spec.Taints = []corev1.Taint{{
		Key: "maintenance", Value: "true", Effect: corev1.TaintEffectNoSchedule,
	}}
	if _, err := client.CoreV1().
		Nodes().
		Update(t.Context(), nodeB, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := ValidateStorageClassPlacement(t.Context(), client, "zonal", "node-b"); err != nil {
		t.Fatalf("pinned tool Pods inject node tolerations: %v", err)
	}

	err = ValidateStorageClassPlacement(t.Context(), client, "zonal", "")
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "unpinned binding Pod") {
		t.Fatalf("tainted scheduler target category=%s error=%v", domain.CategoryOf(err), err)
	}

	nodeB.Spec.Unschedulable = true
	if _, err := client.CoreV1().
		Nodes().
		Update(t.Context(), nodeB, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = ValidateStorageClassPlacement(t.Context(), client, "zonal", "node-b")
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "not cordoned") {
		t.Fatalf("cordoned explicit target category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestValidateBoundVolumeCapacity(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("2Gi"),
			}},
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-data"},
		Spec: corev1.PersistentVolumeSpec{Capacity: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("1Gi"),
		}},
	}

	err := ValidateBoundVolumeCapacity(pvc, pv, nil)
	if err == nil || !strings.Contains(err.Error(), "volume expansion is incomplete") {
		t.Fatalf("incomplete expansion error=%v", err)
	}

	pv.Spec.Capacity[corev1.ResourceStorage] = resource.MustParse("2Gi")
	required := resource.MustParse("3Gi")

	err = ValidateBoundVolumeCapacity(pvc, pv, &required)
	if err == nil || !strings.Contains(err.Error(), "below required capacity 3Gi") {
		t.Fatalf("undersized PV error=%v", err)
	}

	required = resource.MustParse("2Gi")
	if err := ValidateBoundVolumeCapacity(pvc, pv, &required); err != nil {
		t.Fatalf("stable capacity: %v", err)
	}
}
