package kube

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestPVSupportsNodeHonorsMatchFields(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: types.UID("node-uid")}}
	pv := &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{NodeAffinity: &corev1.VolumeNodeAffinity{
		Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-a"}}},
		}}},
	}}}
	if !pvSupportsNode(pv, node) {
		t.Fatal("PV with matching metadata.name field was rejected")
	}
	pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchFields[0].Values = []string{"node-b"}
	if pvSupportsNode(pv, node) {
		t.Fatal("PV with non-matching metadata.name field was accepted")
	}
	pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchFields = []corev1.NodeSelectorRequirement{{Key: "metadata.uid", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-uid"}}}
	if !pvSupportsNode(pv, node) {
		t.Fatal("PV with matching metadata.uid field was rejected")
	}
	pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchFields[0].Values = []string{"other-uid"}
	if pvSupportsNode(pv, node) {
		t.Fatal("PV with non-matching metadata.uid field was accepted")
	}
}

func TestAccessModesAndDestinationVolumeModeAreFenced(t *testing.T) {
	if HasWritableAccessMode([]corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}) {
		t.Fatal("ReadOnlyMany must not be treated as writable")
	}
	if !HasWritableAccessMode([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}) {
		t.Fatal("ReadWriteMany must be treated as writable")
	}
	if !HasWritableAccessMode([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}) {
		t.Fatal("ReadWriteOncePod must be treated as writable")
	}
	if !accessModesEqual([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce, corev1.ReadOnlyMany}, []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany, corev1.ReadWriteOnce}) {
		t.Fatal("access mode comparison must ignore API ordering")
	}
	if effectiveVolumeMode(&corev1.PersistentVolumeClaim{}) != corev1.PersistentVolumeFilesystem {
		t.Fatal("nil VolumeMode must default to Filesystem")
	}
}
