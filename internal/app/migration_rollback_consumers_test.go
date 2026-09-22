package app

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRollbackDestinationConsumed(t *testing.T) {
	seedPod := func(name string, phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "apps"},
			Status:     corev1.PodStatus{Phase: phase},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "d",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				},
			}}},
		}
	}

	// no pods: clean
	if err := rollbackDestinationConsumed(
		t.Context(), fake.NewSimpleClientset(), "apps", "data", "wf-1",
	); err != nil {
		t.Fatalf("clean namespace rejected: %v", err)
	}

	// running external consumer: must conflict
	err := rollbackDestinationConsumed(
		t.Context(), fake.NewSimpleClientset(seedPod("external", corev1.PodRunning)),
		"apps", "data", "wf-1",
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("external consumer accepted: %v", err)
	}

	// completed pod does not count
	if err := rollbackDestinationConsumed(
		t.Context(), fake.NewSimpleClientset(seedPod("done", corev1.PodSucceeded)),
		"apps", "data", "wf-1",
	); err != nil {
		t.Fatalf("completed pod rejected: %v", err)
	}
}
