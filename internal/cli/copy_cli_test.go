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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func activeCopyCLIObjects() []crclient.Object {
	plan := v1alpha1.CopyPlan{Volumes: []v1alpha1.VolumeSpec{{
		SourcePVC:      v1alpha1.LocalResourceReference{Name: "source", UID: "source"},
		SourcePV:       v1alpha1.LocalResourceReference{Name: "source-pv", UID: "source-pv"},
		DestinationPVC: v1alpha1.LocalResourceReference{Name: "destination"},
		SourceCapacity: "1Gi", Capacity: "1Gi",
	}}}

	return []crclient.Object{
		// The namespaced record carries a tenant namespace that differs from
		// the session storage namespace its ConfigMap lives in: the copy
		// family stores records next to every other session family, exactly
		// like migrate.
		&v1alpha1.Copy{
			TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Copy"},
			ObjectMeta: metav1.ObjectMeta{Name: "copy", Namespace: "app"},
			Status: v1alpha1.CopyStatus{
				WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseReserving},
				Plan:           plan.DeepCopy(),
			},
		},
		&v1alpha1.ClusterCopy{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.GroupVersion.String(),
				Kind:       "ClusterCopy",
			},
			ObjectMeta: metav1.ObjectMeta{Name: "copy"},
			Spec: v1alpha1.ClusterCopySpec{
				SourceNamespace:      "source",
				DestinationNamespace: "destination",
				SessionNamespace:     "pvc-migrate-system",
			},
			Status: v1alpha1.ClusterCopyStatus{
				WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseReserving},
				Plan: &v1alpha1.ClusterCopyPlan{
					CopyPlan:             *plan.DeepCopy(),
					SourceNamespace:      "source",
					DestinationNamespace: "destination",
					SessionNamespace:     "pvc-migrate-system",
				},
			},
		},
	}
}

// copySessionFamilyCommand resolves the session command family that owns one
// stored copy record: namespaced records hang under copy, cluster-scoped
// records under cluster-copy.
func copySessionFamilyCommand(object crclient.Object) string {
	if _, cluster := object.(*v1alpha1.ClusterCopy); cluster {
		return "cluster-copy"
	}

	return "copy"
}

func TestCopyCLICleanupDryRunPreservesPolicyGuidance(t *testing.T) {
	for _, object := range activeCopyCLIObjects() {
		name := object.GetObjectKind().GroupVersionKind().Kind
		if name == "" {
			name = object.GetName()
		}

		t.Run(name, func(t *testing.T) {
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
					copySessionFamilyCommand(object),
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

func TestCopyCLIStatusReadsSessionWithoutLegacyService(t *testing.T) {
	for _, object := range activeCopyCLIObjects() {
		name := object.GetObjectKind().GroupVersionKind().Kind
		if name == "" {
			name = object.GetName()
		}

		t.Run(name, func(t *testing.T) {
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
			command.SetArgs(
				[]string{
					"--output", "json",
					copySessionFamilyCommand(object),
					"status",
					object.GetName(),
				},
			)

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

// TestNamespacedSessionLifecycleHasNoRecordNamespace pins the unified record
// convention: namespaced copy and reserve session records live in the session
// storage namespace, so their lifecycle verbs take no -n/--namespace flag —
// the same surface the migrate family exposes.
func TestNamespacedSessionLifecycleHasNoRecordNamespace(t *testing.T) {
	root := NewRoot(Options{Version: "test"})

	for _, path := range [][]string{
		{"copy", "status"},
		{"copy", "resume"},
		{"copy", "abort"},
		{"copy", "cleanup"},
		{"reserve", "status"},
		{"reserve", "resume"},
		{"reserve", "abort"},
		{"reserve", "cleanup"},
	} {
		t.Run(strings.Join(path, " "), func(t *testing.T) {
			command := findSubCommandT(t, root, path...)
			if flag := command.Flags().Lookup("namespace"); flag != nil {
				t.Fatalf("--namespace must not leak onto %s", command.CommandPath())
			}
		})
	}
}

// TestSessionRunCommandLooksUpRecordsInSessionNamespace pins the run-command
// side of the unified record convention: even with the tenant -n bound, the
// copy and reserve entrypoints resolve existing records from the session
// storage namespace, never from the tenant namespace.
func TestSessionRunCommandLooksUpRecordsInSessionNamespace(t *testing.T) {
	for _, test := range []struct {
		family string
		name   string
	}{
		{family: "copy", name: "copy"},
		{family: "reserve", name: "reserve"},
	} {
		t.Run(test.family, func(t *testing.T) {
			client := fake.NewClientset()

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
					test.family,
					"-n", "app",
					"--session", test.name,
					"--dry-run",
				},
			)

			if err := command.Execute(); err == nil {
				t.Fatal("lookup of a missing record must fail")
			}

			want := "--namespace pvc-migrate-system get configmap pvc-migrate-session-" + test.name
			if !strings.Contains(stderr.String(), want) {
				t.Fatalf(
					"lookup must inspect the session storage namespace %q in: %s",
					want,
					stderr.String(),
				)
			}
		})
	}
}
