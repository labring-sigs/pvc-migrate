package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func activeMigrationCLIObjects() []crclient.Object {
	plan := &v1alpha1.MigrationPlan{
		Volumes: []v1alpha1.VolumeSpec{{
			SourcePVC:      v1alpha1.LocalResourceReference{Name: "source", UID: "source"},
			SourcePV:       v1alpha1.LocalResourceReference{Name: "source-pv", UID: "source-pv"},
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "destination"},
			SourceCapacity: "1Gi", Capacity: "1Gi",
		}},
	}

	return []crclient.Object{
		&v1alpha1.Migration{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.GroupVersion.String(),
				Kind:       "Migration",
			},
			ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "pvc-migrate-system"},
			Spec: v1alpha1.MigrationSpec{
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "source"}},
				},
			},
			Status: v1alpha1.MigrationStatus{
				WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseReserving},
				Plan:           plan.DeepCopy(),
			},
		},
		&v1alpha1.ClusterMigration{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.GroupVersion.String(),
				Kind:       "ClusterMigration",
			},
			ObjectMeta: metav1.ObjectMeta{Name: "migration"},
			Spec: v1alpha1.ClusterMigrationSpec{
				MigrationSpec: v1alpha1.MigrationSpec{
					Volumes: []v1alpha1.VolumeRequest{
						{SourcePVC: v1alpha1.LocalResourceReference{Name: "source"}},
					},
				},
				SourceNamespace:    "source",
				TemporaryNamespace: "destination",
				SessionNamespace:   "pvc-migrate-system",
			},
			Status: v1alpha1.ClusterMigrationStatus{
				WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseReserving},
				Plan: &v1alpha1.ClusterMigrationPlan{
					Volumes:              plan.Volumes,
					DestinationNamespace: "source",
					SourceNamespace:      "source",
					TemporaryNamespace:   "destination",
					SessionNamespace:     "pvc-migrate-system",
				},
			},
		},
	}
}

func TestMigrationCLIAbortAndCleanupUseConcreteStorage(t *testing.T) {
	for _, object := range []crclient.Object{
		&v1alpha1.Migration{
			TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Migration"},
			ObjectMeta: metav1.ObjectMeta{Name: "unplanned", Namespace: "pvc-migrate-system"},
		},
		&v1alpha1.ClusterMigration{
			TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "ClusterMigration"},
			ObjectMeta: metav1.ObjectMeta{Name: "unplanned"},
			Spec: v1alpha1.ClusterMigrationSpec{
				SourceNamespace:  "tenant",
				SessionNamespace: "pvc-migrate-system",
			},
		},
	} {
		t.Run(object.GetName(), func(t *testing.T) {
			client := fake.NewClientset()
			client.PrependReactor(
				"create",
				"leases",
				func(action ktesting.Action) (bool, runtime.Object, error) {
					create, ok := action.(ktesting.CreateAction)
					if !ok {
						t.Fatal("expected lease create")
					}

					metadata, ok := create.GetObject().(metav1.Object)
					if !ok {
						t.Fatal("expected lease metadata")
					}

					metadata.SetUID("lease")

					return false, nil, nil
				},
			)

			store, err := kube.NewConfigMapWorkflowStore(
				client,
				"pvc-migrate-system",
				func() crclient.Object { return object },
			)
			if err != nil {
				t.Fatal(err)
			}

			if err := store.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			// The stored session must carry a durable identity so the abort
			// lease fence can verify it, as a real API-server create would.
			stored, err := client.CoreV1().
				ConfigMaps("pvc-migrate-system").
				Get(t.Context(), kube.SessionConfigMapName(object.GetName()), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			stored.UID, stored.ResourceVersion = "record", "1"
			if _, err := client.CoreV1().
				ConfigMaps(stored.Namespace).
				Update(t.Context(), stored, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			execute := func(args ...string) {
				t.Helper()

				command := NewRoot(Options{
					Out: io.Discard, ErrOut: io.Discard,
					runtimeFactory: func(state *rootState) (*commandRuntime, error) {
						return &commandRuntime{
							clients: &kube.Clients{Kubernetes: client},
							printer: printerFor(state),
						}, nil
					},
				})
				command.SetArgs(args)

				if err := command.Execute(); err != nil {
					t.Fatal(err)
				}
			}

			execute("--yes", "migrate", "abort", object.GetName(), "--dry-run=false")

			loaded, err := store.Load(t.Context(), crclient.ObjectKey{Name: object.GetName()})
			if err != nil {
				t.Fatal(err)
			}

			switch current := loaded.(type) {
			case *v1alpha1.Migration:
				if current.Status.Phase != domain.PhaseAborted {
					t.Fatalf("phase = %s", current.Status.Phase)
				}
			case *v1alpha1.ClusterMigration:
				if current.Status.Phase != domain.PhaseAborted {
					t.Fatalf("phase = %s", current.Status.Phase)
				}
			default:
				t.Fatalf("unexpected stored type %T", loaded)
			}

			execute("migrate", "status")
			execute(
				"--yes",
				"migrate",
				"cleanup",
				object.GetName(),
				"--finalize",
				"--delete-session",
				"--dry-run=false",
			)

			if _, err := store.Load(
				t.Context(),
				crclient.ObjectKey{Name: object.GetName()},
			); !apierrors.IsNotFound(
				err,
			) {
				t.Fatalf("workflow not deleted: %v", err)
			}
		})
	}
}

func TestMigrationCLICleanupDryRunPreservesPolicyGuidance(t *testing.T) {
	for _, object := range activeMigrationCLIObjects() {
		t.Run(object.GetName(), func(t *testing.T) {
			client := fake.NewClientset()

			store, err := kube.NewConfigMapWorkflowStore(
				client,
				"pvc-migrate-system",
				func() crclient.Object { return object },
			)
			if err != nil {
				t.Fatal(err)
			}

			if err := store.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			client.ClearActions()

			var stderr bytes.Buffer

			command := NewRoot(Options{
				Out: io.Discard, ErrOut: &stderr,
				runtimeFactory: func(state *rootState) (*commandRuntime, error) {
					return &commandRuntime{
						clients: &kube.Clients{Kubernetes: client},
						printer: printerFor(state),
					}, nil
				},
			})
			command.SetArgs(
				[]string{
					"migrate",
					"cleanup",
					object.GetName(),
					"--unused-storage-policy",
					"Delete",
					"--finalize",
					"--dry-run",
				},
			)

			if err := command.Execute(); domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("error = %v", err)
			}

			for _, want := range []string{"Revalidate cleanup before retrying:", "--unused-storage-policy Delete", "--finalize"} {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("missing %q in %q", want, stderr.String())
				}
			}

			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("dry-run mutated resources: %v", action)
				}
			}
		})
	}
}

func TestMigrationCLIStatusReadsSessionWithoutLegacyService(t *testing.T) {
	for _, object := range activeMigrationCLIObjects() {
		t.Run(object.GetName(), func(t *testing.T) {
			client := fake.NewClientset()

			store, err := kube.NewConfigMapWorkflowStore(
				client,
				"pvc-migrate-system",
				func() crclient.Object { return object },
			)
			if err != nil {
				t.Fatal(err)
			}

			if err := store.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			var stdout bytes.Buffer

			command := NewRoot(Options{
				Out: &stdout, ErrOut: io.Discard,
				runtimeFactory: func(state *rootState) (*commandRuntime, error) {
					return &commandRuntime{
						clients: &kube.Clients{Kubernetes: client},
						printer: printerFor(state),
					}, nil
				},
			})
			command.SetArgs([]string{"--output", "json", "migrate", "status", object.GetName()})

			if err := command.Execute(); err != nil {
				t.Fatal(err)
			}

			var header metav1.TypeMeta
			if err := json.Unmarshal(stdout.Bytes(), &header); err != nil {
				t.Fatal(err)
			}

			if header.Kind != object.GetObjectKind().GroupVersionKind().Kind {
				t.Fatalf("status changed API kind: %s", stdout.String())
			}
		})
	}
}
