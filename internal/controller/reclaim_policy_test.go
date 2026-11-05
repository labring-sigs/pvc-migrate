package controller

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func TestOnlyUnusedStoragePolicyCanChangeAfterExecutionStarts(t *testing.T) {
	original := &v1alpha1.ClusterMigration{
		Spec: v1alpha1.ClusterMigrationSpec{
			MigrationSpec: v1alpha1.MigrationSpec{
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
				},
				TransferOptions: v1alpha1.TransferOptions{
					UnusedStoragePolicy: v1alpha1.UnusedStorageKeep,
				},
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
		current.Spec.UnusedStoragePolicy = v1alpha1.UnusedStorageDelete
		current.Status.ExecutionIntentHash = observedHash

		// The shipped fence is content-based: only UnusedStoragePolicy is
		// canonicalized out of the intent hash, so the policy-only change
		// keeps the hash equal and any other spec change breaks it.
		if err := executionIntentMutationError(current); (err == nil) != tc.allowed {
			t.Fatalf("source=%s err=%v", tc.sourcePVC, err)
		}
	}
}

func TestManualCutoverToggleKeepsExecutionIntent(t *testing.T) {
	original := &v1alpha1.PodMigration{
		Spec: v1alpha1.PodMigrationSpec{
			ManualCutover: true,
		},
	}

	observedHash, err := kube.WorkflowExecutionIntentHash(original)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		mutate  func(*v1alpha1.PodMigration)
		allowed bool
	}{
		{
			name:    "clearing the flag approves the held pause",
			mutate:  func(o *v1alpha1.PodMigration) { o.Spec.ManualCutover = false },
			allowed: true,
		},
		{
			name:    "changing an execution input still trips the fence",
			mutate:  func(o *v1alpha1.PodMigration) { o.Spec.PrecopyPasses = 1 },
			allowed: false,
		},
	} {
		current := original.DeepCopy()
		tc.mutate(current)
		current.Status.ExecutionIntentHash = observedHash

		if err := executionIntentMutationError(current); (err == nil) != tc.allowed {
			t.Fatalf("%s: err=%v", tc.name, err)
		}
	}
}
