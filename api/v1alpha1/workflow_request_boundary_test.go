package v1alpha1_test

import (
	"encoding/json"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func TestPodMigrationSpecDoesNotReadWorkloadSnapshots(t *testing.T) {
	object := &v1alpha1.PodMigration{
		Spec: v1alpha1.PodMigrationSpec{
			Pod: v1alpha1.LocalResourceReference{Name: "database-0"}, PrecopyPasses: 0,
		},
		Status: v1alpha1.PodMigrationStatus{Plan: &v1alpha1.PodMigrationPlan{
			Workload: v1alpha1.WorkloadSpec{
				OriginalObject: &apiextensionsv1.JSON{Raw: []byte(`{invalid snapshot`)},
			},
		}},
	}
	for _, spec := range []any{object.Spec, v1alpha1.ClusterPodMigrationSpec{
		SourceNamespace: "app", TemporaryNamespace: "app", SessionNamespace: "app",
		PodMigrationSpec: object.Spec,
	}} {
		encoded, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}

		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}

		if fields["workload"] != nil || fields["originalObject"] != nil ||
			string(fields["precopyPasses"]) != "0" {
			t.Fatalf("spec crossed snapshot boundary: %s", encoded)
		}
	}
}

func TestCopyDeepCopyOwnsSpecAndPlanInputs(t *testing.T) {
	object := &v1alpha1.Copy{
		Spec: v1alpha1.CopySpec{
			TransferOptions: v1alpha1.TransferOptions{Strategies: []string{"mount"}},
			Volumes: []v1alpha1.VolumeRequest{
				{
					SourcePVC: v1alpha1.LocalResourceReference{Name: "data"},
					TransferScope: &v1alpha1.TransferScope{
						SourcePath:      "source",
						DestinationPath: "destination",
					},
				},
			},
		},
		Status: v1alpha1.CopyStatus{Plan: &v1alpha1.CopyPlan{
			Strategies: []string{"mount"},
			Volumes: []v1alpha1.VolumeSpec{
				{
					SourcePVC: v1alpha1.LocalResourceReference{Name: "data", UID: "source"},
					TransferScope: &v1alpha1.TransferScope{
						SourcePath:      "source",
						DestinationPath: "destination",
					},
				},
			},
		}},
	}
	cloned := object.DeepCopy()
	cloned.Spec.Strategies[0] = "local"
	cloned.Spec.Volumes[0].TransferScope.SourcePath = "changed"
	cloned.Status.Plan.Strategies[0] = "clusterip"

	cloned.Status.Plan.Volumes[0].TransferScope.SourcePath = "another"
	if object.Spec.Strategies[0] != "mount" ||
		object.Spec.Volumes[0].TransferScope.SourcePath != "source" ||
		object.Status.Plan.Strategies[0] != "mount" ||
		object.Status.Plan.Volumes[0].TransferScope.SourcePath != "source" {
		t.Fatal("copy mutation changed the original CRD spec or plan")
	}
}
