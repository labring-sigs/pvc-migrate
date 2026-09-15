package controller

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func TestOnlyReclaimPoliciesCanChangeAfterExecutionStarts(t *testing.T) {
	original := &v1alpha1.ClusterMigration{
		Spec: v1alpha1.ClusterMigrationSpec{
			MigrationSpec: v1alpha1.MigrationSpec{
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
				},
				SourcePVReclaimPolicy: "Retain",
			},
		},
	}

	observedHash, err := kube.WorkflowExecutionIntentHash(original)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		sourcePVC string
		allowed   bool
	}{{"data", true}, {"other", false}} {
		current := original.DeepCopy()
		current.Spec.Volumes[0].SourcePVC.Name = tc.sourcePVC
		current.Spec.SourcePVReclaimPolicy = "Delete"
		current.Spec.DestinationPVCReclaimPolicy = "Retain"

		currentHash, err := kube.WorkflowExecutionIntentHash(current)
		if err != nil {
			t.Fatal(err)
		}

		for _, generation := range []int64{1, 2, 3} {
			for _, deleting := range []bool{false, true} {
				if err := workflowSpecMutationError(
					observedHash,
					currentHash,
					generation,
					1,
					deleting,
					nil,
				); (err == nil) != tc.allowed {
					t.Fatalf(
						"generation=%d deleting=%v source=%s err=%v",
						generation,
						deleting,
						tc.sourcePVC,
						err,
					)
				}
			}
		}
	}
}
