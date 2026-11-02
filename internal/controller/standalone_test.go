package controller

import (
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestStandaloneWorkloadAppliesSnapshotLimit pins that an oversized standalone
// Pod is rejected at discovery instead of pushing a status write past the
// snapshot size limit.
func TestStandaloneWorkloadAppliesSnapshotLimit(t *testing.T) {
	smallPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "small"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
	}

	workload, err := standaloneWorkload(smallPod)
	if err != nil {
		t.Fatalf("small Pod rejected: %v", err)
	}

	if workload.OriginalObject == nil || len(workload.OriginalObject.Raw) == 0 {
		t.Fatal("small Pod snapshot is empty")
	}

	oversizedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "oversized",
			Annotations: map[string]string{
				"mft.io/bulk": strings.Repeat("x", domain.MaxOriginalPodSnapshotBytes),
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
	}

	_, err = standaloneWorkload(oversizedPod)
	if err == nil {
		t.Fatal("oversized Pod snapshot was accepted")
	}

	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category = %v, want precondition", domain.CategoryOf(err))
	}

	if !strings.Contains(err.Error(), "exceeds the") {
		t.Fatalf("rejection does not state the limit: %v", err)
	}
}
