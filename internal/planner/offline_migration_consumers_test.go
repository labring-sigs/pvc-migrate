package planner

import (
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type recordingChecks struct{ checks []domain.Check }

func (r *recordingChecks) AddCheck(check domain.Check) { r.checks = append(r.checks, check) }

func offlineConsumerPod(name string, phase corev1.PodPhase, node, claim string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name},
		Spec: corev1.PodSpec{
			NodeName: node,
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: claim,
					},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// TestOfflineMigrationConsumerMessageSeparatesTerminalPods pins the remediation
// wording: an active consumer must be stopped, while a finished Pod only holds
// the PVC protection boundary and needs its object deleted — calling a
// terminal Pod "active" sends operators to stop a Pod that already ended.
func TestOfflineMigrationConsumerMessageSeparatesTerminalPods(t *testing.T) {
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
	}

	tests := []struct {
		name        string
		pods        []corev1.Pod
		wantStrings []string
		notWant     string
	}{
		{
			name: "terminal succeeded pod",
			pods: []corev1.Pod{
				offlineConsumerPod("writer", corev1.PodSucceeded, "node-1", "data"),
			},
			wantStrings: []string{
				"terminal Pod(s) writer (phase Succeeded)",
				"delete the finished Pod objects",
			},
			notWant: "active Pod consumer(s) writer",
		},
		{
			name: "active and terminal pods together",
			pods: []corev1.Pod{
				offlineConsumerPod("worker", corev1.PodRunning, "node-1", "data"),
				offlineConsumerPod("writer", corev1.PodSucceeded, "node-1", "data"),
			},
			wantStrings: []string{
				"active Pod consumer(s) worker",
				"terminal Pod(s) writer (phase Succeeded)",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := &recordingChecks{}
			checkOfflineMigrationPlanConsumers(
				recorder,
				[]planVolumeInput{{pvc: claim}},
				test.pods,
				nil,
				domain.PresentationCLI,
			)

			if len(recorder.checks) != 1 || recorder.checks[0].Passed {
				t.Fatalf("expected one failed pvc-consumers check, got %#v", recorder.checks)
			}

			message := recorder.checks[0].Message
			for _, want := range test.wantStrings {
				if !strings.Contains(message, want) {
					t.Fatalf("message %q missing %q", message, want)
				}
			}

			if test.notWant != "" && strings.Contains(message, test.notWant) {
				t.Fatalf("message %q must not call a terminal Pod active", message)
			}
		})
	}
}
