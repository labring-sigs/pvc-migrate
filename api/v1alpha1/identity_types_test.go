package v1alpha1_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func assertIdentityPlanRoundTrip[T any](t *testing.T, plan T) {
	t.Helper()

	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(data), `"volumes":`) {
		t.Fatalf("identity plan exposes transfer volumes: %s", data)
	}

	var restored T
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(plan, restored) {
		t.Fatalf("identity plan changed in storage: want=%+v got=%+v", plan, restored)
	}
}

func TestIdentityPlansPreserveEndpointsAndOwnedTemplates(t *testing.T) {
	template := v1alpha1.PVCSourceTemplate{
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-data"},
		Metadata: v1alpha1.PVCMetadata{
			Labels:      map[string]string{"app": "demo"},
			Annotations: map[string]string{"owner": "team"},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "Pod", Name: "owner", UID: "owner-uid"},
			},
		},
		ReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
	}
	rename := v1alpha1.RenamePlan{PVCIdentityFields: v1alpha1.PVCIdentityFields{
		SourcePVC:      v1alpha1.LocalResourceReference{Name: "data", UID: "pvc-uid"},
		SourcePV:       v1alpha1.LocalResourceReference{Name: "pv-data", UID: "pv-uid"},
		DestinationPVC: v1alpha1.LocalResourceReference{Name: "renamed"},
		SourceTemplate: *template.DeepCopy(),
	}}
	move := v1alpha1.MovePlan{
		SourceNamespace: "source", DestinationNamespace: "destination", SessionNamespace: "control",
		Identity: v1alpha1.MoveIdentity{
			SourcePVC:      rename.SourcePVC,
			SourcePV:       rename.SourcePV,
			DestinationPVC: rename.DestinationPVC,
			SourceTemplate: *template.DeepCopy(),
		},
	}
	assertIdentityPlanRoundTrip(t, rename)
	assertIdentityPlanRoundTrip(t, move)

	for name, copied := range map[string]*v1alpha1.PVCSourceTemplate{
		"rename": &rename.DeepCopy().SourceTemplate,
		"move":   &move.DeepCopy().Identity.SourceTemplate,
	} {
		copied.Metadata.Labels["app"] = "changed"
		copied.Metadata.Annotations["owner"] = "changed"

		copied.Metadata.OwnerReferences[0].Name = "changed"
		if rename.SourceTemplate.Metadata.Labels["app"] != "demo" ||
			move.Identity.SourceTemplate.Metadata.Labels["app"] != "demo" ||
			rename.SourceTemplate.Metadata.OwnerReferences[0].Name != "owner" ||
			move.Identity.SourceTemplate.Metadata.OwnerReferences[0].Name != "owner" {
			t.Fatalf("%s deepcopy shares metadata", name)
		}
	}
}
