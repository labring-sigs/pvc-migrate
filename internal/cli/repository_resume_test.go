package cli

import (
	"io"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRepositoryResumeLeavesTerminalWorkflowsUnchanged(t *testing.T) {
	for _, phase := range []v1alpha1.WorkflowPhase{domain.PhaseAborted, domain.PhaseCompleted} {
		for _, operation := range []string{"backup", "restore"} {
			for _, dryRun := range []string{"true", "false"} {
				t.Run(operation+"/"+string(phase)+"/dry-run="+dryRun, func(t *testing.T) {
					client := fake.NewClientset()
					metadata := metav1.ObjectMeta{Namespace: "pvc-migrate-system", Name: "finished"}

					var object crclient.Object
					if operation == "backup" {
						object = &v1alpha1.Backup{
							ObjectMeta: metadata,
							Status: v1alpha1.BackupStatus{
								WorkflowStatus: v1alpha1.WorkflowStatus{Phase: phase},
							},
						}
					} else {
						object = &v1alpha1.Restore{
							ObjectMeta: metadata,
							Status: v1alpha1.RestoreStatus{
								WorkflowStatus: v1alpha1.WorkflowStatus{Phase: phase},
							},
						}
					}

					store, err := kube.NewConfigMapWorkflowStore(
						client,
						"pvc-migrate-system",
						func() crclient.Object {
							if operation == "backup" {
								return &v1alpha1.Backup{}
							}
							return &v1alpha1.Restore{}
						},
					)
					if err != nil {
						t.Fatal(err)
					}

					if err := store.Create(t.Context(), object); err != nil {
						t.Fatal(err)
					}

					client.ClearActions()

					command := NewRoot(
						Options{
							Out:    io.Discard,
							ErrOut: io.Discard,
							runtimeFactory: func(state *rootState) (*commandRuntime, error) {
								return &commandRuntime{
									clients: &kube.Clients{Kubernetes: client},
									printer: printerFor(state),
								}, nil
							},
						},
					)
					command.SetArgs(
						[]string{operation, "resume", object.GetName(), "--dry-run=" + dryRun},
					)

					if err := command.Execute(); err != nil {
						t.Fatal(err)
					}

					// A terminal workflow must only be read, never executed or
					// mutated by a resume attempt.
					for _, action := range client.Actions() {
						if action.GetVerb() != "get" && action.GetVerb() != "list" {
							t.Fatalf("terminal resume accessed execution resources: %v", action)
						}
					}
				})
			}
		}
	}
}
