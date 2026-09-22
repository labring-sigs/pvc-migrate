package cli

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

// TestCopyApprovalTargetCoversPodSelection pins the panic fix for
// Pod-selected copies: the typed approval must fall back to the Pod name —
// or the workflow name — instead of indexing an empty volume list.
func TestCopyApprovalTargetCoversPodSelection(t *testing.T) {
	volume := v1alpha1.VolumeRequest{}
	volume.SourcePVC.Name = "data"

	if name := copyApprovalTarget(
		v1alpha1.CopySpec{Volumes: []v1alpha1.VolumeRequest{volume}},
		"workflow",
	); name != "data" {
		t.Fatalf("volume approval target=%q", name)
	}

	podCopy := v1alpha1.CopySpec{Pod: &v1alpha1.LocalResourceReference{Name: "app-0"}}
	if name := copyApprovalTarget(podCopy, "workflow"); name != "app-0" {
		t.Fatalf("pod approval target=%q", name)
	}

	if name := copyApprovalTarget(v1alpha1.CopySpec{}, "workflow"); name != "workflow" {
		t.Fatalf("fallback approval target=%q", name)
	}
}
