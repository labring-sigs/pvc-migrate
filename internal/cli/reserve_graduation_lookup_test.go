package cli

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// graduatedSessionClient seeds the session storage with the record a
// reservation graduation persists: the ConfigMap replaced in place by the
// copy payload, carrying the copy kind in its stored TypeMeta.
func graduatedSessionClient(t *testing.T, kind string, payload []byte) *fake.Clientset {
	t.Helper()

	client := fake.NewClientset()
	if _, err := client.CoreV1().ConfigMaps("sessions").Create(
		t.Context(),
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "sessions",
				Name:      kube.SessionConfigMapName("workflow"),
				Labels: map[string]string{
					kube.ManagedByLabel:    kube.ManagedByValue,
					kube.SessionKey:        "workflow",
					kube.WorkflowKindLabel: kind,
				},
			},
			Data: map[string]string{kube.SessionDataKey: string(payload)},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	return client
}

// TestReserveLookupRejectsGraduatedRecordsWithCopyClosureHint pins the
// recovery hint for the graduation trap: the reserve family cannot address
// the record once its ConfigMap holds the copy, so the error must name the
// copy family that owns the closure instead of a bare kind mismatch.
func TestReserveLookupRejectsGraduatedRecordsWithCopyClosureHint(t *testing.T) {
	namespaced, err := json.Marshal(&v1alpha1.Copy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "Copy",
		},
		ObjectMeta: metav1.ObjectMeta{Namespace: "data", Name: "workflow"},
		Status: v1alpha1.CopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseWarmCopied},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	cluster, err := json.Marshal(&v1alpha1.ClusterCopy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "ClusterCopy",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "workflow"},
		Status: v1alpha1.ClusterCopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseWarmCopied},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		cluster bool
		kind    string
		payload []byte
		want    string
	}{
		{
			name:    "namespaced",
			kind:    "Copy",
			payload: namespaced,
			want:    "graduated to a copy; close it with the copy cleanup family",
		},
		{
			name:    "cluster",
			cluster: true,
			kind:    "ClusterCopy",
			payload: cluster,
			want: "graduated to a cluster copy; close it with the cluster-copy " +
				"cleanup family",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			state := &rootState{global: globals{sessionNamespace: "sessions"}}
			cmd := &cobra.Command{}
			cmd.SetErr(io.Discard)

			scope := namespacedRecords
			if testCase.cluster {
				scope = clusterRecords
			}

			_, _, err := state.loadReservationWithBackend(
				t.Context(),
				cmd,
				&commandRuntime{
					clients: &kube.Clients{
						Kubernetes: graduatedSessionClient(t, testCase.kind, testCase.payload),
					},
				},
				"workflow",
				sourceSession,
				scope,
			)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error = %v, want graduation hint %q", err, testCase.want)
			}
		})
	}
}
