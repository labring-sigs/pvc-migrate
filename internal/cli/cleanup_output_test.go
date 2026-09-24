package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// testSessionLocker stands in for the lease machinery: the contract under
// test is the CLI's closing confirmation, and lease fencing has its own
// coverage at the app and kube layers.
type testSessionLocker struct{}

func (testSessionLocker) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return testSessionLock{}, nil
}

type testSessionLock struct{}

func (testSessionLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

func (testSessionLock) Err() error                    { return nil }
func (testSessionLock) Release(context.Context) error { return nil }
func (testSessionLock) Delete(context.Context) error  { return nil }

// TestMigratePodCleanupPrintsDeletionConfirmation pins the cleanup contract:
// closing a workflow with --delete-session prints the same closing
// confirmation every other workflow family prints instead of exiting
// silently. Session records and controller-owned CRs both follow it.
func TestMigratePodCleanupPrintsDeletionConfirmation(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{
			name: "session record",
			args: []string{
				"--yes", "--session-namespace", "sessions",
				"migrate-pod", "cleanup", "mig-cleanup-test",
				"--finalize", "--delete-session", "--dry-run=false",
			},
		},
		{
			name: "controller CR",
			args: []string{
				"--yes",
				"cr", "migrate-pod", "cleanup", "mig-cleanup-test", "-n", "app",
				"--finalize", "--delete-session", "--dry-run=false",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}

			kubeClient := kubernetesfake.NewClientset()
			crClient := crfake.NewClientBuilder().WithScheme(scheme).Build()

			sessionStore, err := kube.NewConfigMapWorkflowStore(
				kubeClient,
				"sessions",
				func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
			)
			if err != nil {
				t.Fatal(err)
			}

			namespacedStore, err := kube.NewCRDWorkflowStore(
				crClient,
				func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
			)
			if err != nil {
				t.Fatal(err)
			}

			// An aborted migration without a plan cleans up without touching
			// storage, so no PVC/PV fixtures are needed.
			record := &v1alpha1.PodMigration{
				ObjectMeta: metav1.ObjectMeta{Name: "mig-cleanup-test", Namespace: "app"},
				Status: v1alpha1.PodMigrationStatus{
					WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseAborted},
				},
			}

			sessionExecutor := app.NewPodMigrationExecutor(
				kubeClient,
				sessionStore,
				testSessionLocker{},
				copyengine.NewPVMigrate(),
				app.PodMigrationExecutorConfig{},
			)
			namespacedExecutor := app.NewPodMigrationExecutor(
				kubeClient,
				namespacedStore,
				testSessionLocker{},
				copyengine.NewPVMigrate(),
				app.PodMigrationExecutorConfig{},
			)

			// Seed the session record exactly as the store persists it, with
			// the identity a real API server stamps and the fake clients do
			// not: the cleanup lease fence requires it.
			encoded, err := json.Marshal(&v1alpha1.PodMigration{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.GroupVersion.String(),
					Kind:       "PodMigration",
				},
				ObjectMeta: record.ObjectMeta,
				Spec:       record.Spec,
				Status:     record.Status,
			})
			if err != nil {
				t.Fatal(err)
			}

			if _, err := kubeClient.CoreV1().ConfigMaps("sessions").Create(
				t.Context(),
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:            kube.SessionConfigMapName(record.Name),
						Namespace:       "sessions",
						UID:             types.UID("uid-mig-cleanup-test"),
						ResourceVersion: "1",
						Labels: map[string]string{
							kube.ManagedByLabel: kube.ManagedByValue,
							kube.SessionKey:     record.Name,
						},
					},
					Data: map[string]string{kube.SessionDataKey: string(encoded)},
				},
				metav1.CreateOptions{},
			); err != nil {
				t.Fatal(err)
			}

			crRecord := record.DeepCopy()

			crRecord.UID = "uid-mig-cleanup-test"
			if err := crClient.Create(t.Context(), crRecord); err != nil {
				t.Fatal(err)
			}

			var stdout bytes.Buffer

			root := NewRoot(Options{
				Out: &stdout, ErrOut: io.Discard,
				runtimeFactory: func(state *rootState) (*commandRuntime, error) {
					return &commandRuntime{
						clients: &kube.Clients{Kubernetes: kubeClient, Runtime: crClient},
						printer: printerFor(state),

						podMigrationSessionStore:    sessionStore,
						podMigrationSessionExecutor: sessionExecutor,
						podMigrationStore:           namespacedStore,
						podMigrationExecutor:        namespacedExecutor,
					}, nil
				},
			})
			root.SetArgs(test.args)

			if err := root.ExecuteContext(t.Context()); err != nil {
				t.Fatalf("cleanup failed: %v", err)
			}

			if !strings.Contains(
				stdout.String(),
				"Deleted pod migration workflow mig-cleanup-test.",
			) {
				t.Fatalf(
					"cleanup must confirm the deletion instead of exiting silently: %q",
					stdout.String(),
				)
			}

			// Each variant closes the record its own backend owns; the other
			// backend's copy stays untouched.
			if test.name == "session record" {
				if _, err := sessionStore.Load(
					t.Context(),
					crclient.ObjectKey{Name: "mig-cleanup-test"},
				); err == nil {
					t.Fatal("session record must be gone after --delete-session")
				}

				if err := crClient.Get(
					t.Context(),
					crclient.ObjectKey{Namespace: "app", Name: "mig-cleanup-test"},
					&v1alpha1.PodMigration{},
				); err != nil {
					t.Fatal("the untouched CR copy must survive the session cleanup")
				}

				return
			}

			if err := crClient.Get(
				t.Context(),
				crclient.ObjectKey{Namespace: "app", Name: "mig-cleanup-test"},
				&v1alpha1.PodMigration{},
			); err == nil {
				t.Fatal("workflow CR must be gone after --delete-session")
			}

			if _, err := sessionStore.Load(
				t.Context(),
				crclient.ObjectKey{Name: "mig-cleanup-test"},
			); err != nil {
				t.Fatal("the untouched session record must survive the CR cleanup")
			}
		})
	}
}
