package kube

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestPVSupportsNodeHonorsMatchFields(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: types.UID("node-uid")}}

	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{NodeAffinity: &corev1.VolumeNodeAffinity{
			Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchFields: []corev1.NodeSelectorRequirement{
						{
							Key:      "metadata.name",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node-a"},
						},
					},
				},
			}},
		}},
	}
	if !PVSupportsNode(pv, node) {
		t.Fatal("PV with matching metadata.name field was rejected")
	}

	pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchFields[0].Values = []string{"node-b"}
	if PVSupportsNode(pv, node) {
		t.Fatal("PV with non-matching metadata.name field was accepted")
	}

	pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchFields = []corev1.NodeSelectorRequirement{
		{Key: "metadata.uid", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-uid"}},
	}
	if !PVSupportsNode(pv, node) {
		t.Fatal("PV with matching metadata.uid field was rejected")
	}

	pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchFields[0].Values = []string{"other-uid"}
	if PVSupportsNode(pv, node) {
		t.Fatal("PV with non-matching metadata.uid field was accepted")
	}
}

func TestPVUniqueNodeName(t *testing.T) {
	term := func(expressions, fields []corev1.NodeSelectorRequirement) corev1.NodeSelectorTerm {
		return corev1.NodeSelectorTerm{MatchExpressions: expressions, MatchFields: fields}
	}
	requirement := func(key string, values ...string) corev1.NodeSelectorRequirement {
		return corev1.NodeSelectorRequirement{
			Key:      key,
			Operator: corev1.NodeSelectorOpIn,
			Values:   values,
		}
	}
	pvWithTerms := func(terms ...corev1.NodeSelectorTerm) *corev1.PersistentVolume {
		return &corev1.PersistentVolume{
			Spec: corev1.PersistentVolumeSpec{NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{NodeSelectorTerms: terms},
			}},
		}
	}

	for _, test := range []struct {
		name  string
		pv    *corev1.PersistentVolume
		nodes []corev1.Node
		want  string
	}{
		{name: "nil PV"},
		{name: "single hostname resolves object name", pv: pvWithTerms(term([]corev1.NodeSelectorRequirement{requirement(corev1.LabelHostname, "host-a")}, nil)), nodes: []corev1.Node{
			{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "host-a"}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: "host-b"}}},
		}, want: "node-a"},
		{name: "single metadata name", pv: pvWithTerms(term(nil, []corev1.NodeSelectorRequirement{requirement("metadata.name", "node-a")})), nodes: []corev1.Node{
			{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}, {ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
		}, want: "node-a"},
		{name: "multiple OR terms same node", pv: pvWithTerms(
			term([]corev1.NodeSelectorRequirement{requirement(corev1.LabelHostname, "host-a")}, nil),
			term(nil, []corev1.NodeSelectorRequirement{requirement("metadata.name", "node-a")}),
		), nodes: []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "host-a"}}}}, want: "node-a"},
		{name: "single term multiple nodes", pv: pvWithTerms(term([]corev1.NodeSelectorRequirement{requirement(corev1.LabelHostname, "host-a", "host-b")}, nil)), nodes: []corev1.Node{
			{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "host-a"}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: "host-b"}}},
		}},
		{name: "OR terms different nodes", pv: pvWithTerms(
			term([]corev1.NodeSelectorRequirement{requirement(corev1.LabelHostname, "host-a")}, nil),
			term([]corev1.NodeSelectorRequirement{requirement(corev1.LabelHostname, "host-b")}, nil),
		), nodes: []corev1.Node{
			{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "host-a"}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: "host-b"}}},
		}},
		{name: "zone selects current sole node", pv: pvWithTerms(term([]corev1.NodeSelectorRequirement{requirement(corev1.LabelTopologyZone, "zone-a")}, nil)), nodes: []corev1.Node{
			{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelTopologyZone: "zone-a"}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelTopologyZone: "zone-b"}}},
		}, want: "node-a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := PVUniqueNodeName(test.pv, test.nodes); got != test.want {
				t.Fatalf("PVUniqueNodeName()=%q want=%q", got, test.want)
			}
		})
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

	if !accessModesEqual(
		[]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce, corev1.ReadOnlyMany},
		[]corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany, corev1.ReadWriteOnce},
	) {
		t.Fatal("access mode comparison must ignore API ordering")
	}

	if effectiveVolumeMode(&corev1.PersistentVolumeClaim{}) != corev1.PersistentVolumeFilesystem {
		t.Fatal("nil VolumeMode must default to Filesystem")
	}
}
