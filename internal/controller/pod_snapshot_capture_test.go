package controller

import (
	"encoding/json"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCapturedPodSnapshotIsIsolated(t *testing.T) {
	pod := trustedSnapshotPod("tenant-a")
	ref := v1alpha1.ObjectReference{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}

	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}

	input := &apiextensionsv1.JSON{Raw: raw}
	digest := kube.PodSnapshotHash(raw)

	snapshot, gotDigest, err := kube.CaptureStandalonePodSnapshot(
		t.Context(),
		nil,
		ref,
		input,
		digest,
	)
	if err != nil || gotDigest != digest {
		t.Fatalf("validate persisted snapshot: digest=%s error=%v", gotDigest, err)
	}

	snapshot.Raw[0] = ' '
	if input.Raw[0] != '{' || kube.PodSnapshotHash(input.Raw) != digest {
		t.Fatal("returned snapshot aliases persisted input")
	}
}

func TestCapturePodSnapshotRejectsMissingIdentityBeforeRead(t *testing.T) {
	client := fake.NewClientset(trustedSnapshotPod("tenant-a"))

	_, _, err := kube.CaptureStandalonePodSnapshot(t.Context(), client,
		v1alpha1.ObjectReference{Namespace: "tenant-a", Name: "worker"}, nil, "")
	if err == nil {
		t.Fatal("missing UID accepted")
	}

	if len(client.Actions()) != 0 {
		t.Fatal("read Pod before validating the supplied identity")
	}
}
