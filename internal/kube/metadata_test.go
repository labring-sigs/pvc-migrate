package kube

import "testing"

func TestPVCAnnotationsForRecreation(t *testing.T) {
	input := map[string]string{
		"example.com/retained":              "value",
		pvcBindCompletedAnnotation:          "yes",
		pvcBoundByControllerAnnotation:      "yes",
		pvcSelectedNodeAnnotation:           "worker-a",
		pvcStorageProvisionerAnnotation:     "example.csi.io",
		pvcBetaStorageProvisionerAnnotation: "example.csi.io",
		PVCStorageResizerAnnotation:         "example.csi.io",
		kubectlLastAppliedConfigAnnotation:  "stale",
		SessionKey:                          "old-session",
	}

	got := PVCAnnotationsForRecreation(input)
	if len(got) != 1 || got["example.com/retained"] != "value" {
		t.Fatalf("recreated annotations = %#v", got)
	}

	if len(input) != 9 {
		t.Fatalf("input annotations were mutated: %#v", input)
	}

	if empty := PVCAnnotationsForRecreation(nil); empty == nil || len(empty) != 0 {
		t.Fatalf("nil annotations produced %#v", empty)
	}
}
