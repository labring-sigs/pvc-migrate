package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestTransferSubmissionPersistsConcreteSpecs(t *testing.T) {
	options := v1alpha1.TransferOptions{
		UnusedStoragePolicy: "Delete", Strategies: []string{"mount"},
		SourceNode: "worker", TargetNode: "auto", SourcePath: "logs", DestinationPath: ".",
		VerifyChecksum: true, DeleteExtraneous: new(true),
	}
	volumes := []v1alpha1.VolumeRequest{
		{
			SourcePVC: v1alpha1.LocalResourceReference{
				Name:            "data",
				UID:             "source-uid",
				ResourceVersion: "7",
			},
			SourcePV: &v1alpha1.LocalResourceReference{Name: "pv-data", UID: "pv-uid"},
			Capacity: "3Gi",
			TransferScope: &v1alpha1.TransferScope{
				SourcePath:      "logs",
				DestinationPath: ".",
			},
		},
	}
	migration := &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "app"},
		Spec: v1alpha1.MigrationSpec{
			Volumes:         volumes,
			TransferOptions: options,
		},
	}
	pod := &v1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-migration", Namespace: "app"},
		Spec: v1alpha1.PodMigrationSpec{
			Volumes:         volumes,
			TransferOptions: options,
			Pod: v1alpha1.LocalResourceReference{
				Name: "database",
				UID:  "pod-uid",
			},
			PrecopyPasses: 0,
		},
	}
	copyObject := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy", Namespace: "app"},
		Spec: v1alpha1.CopySpec{
			Volumes:         volumes,
			TransferOptions: options,
			Online:          true,
		},
	}
	reservation := &v1alpha1.Reservation{
		ObjectMeta: metav1.ObjectMeta{Name: "reservation", Namespace: "app"},
		Spec: v1alpha1.ReservationSpec{
			Volumes:         volumes,
			TransferOptions: options,
		},
	}
	clusterMigration := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration"},
		Spec: v1alpha1.ClusterMigrationSpec{
			SourceNamespace:      "app",
			DestinationNamespace: "archive",
			TemporaryNamespace:   "staging",
			SessionNamespace:     "sessions",
			MigrationSpec:        migration.Spec,
		},
	}
	clusterCopy := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy"},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace:      "app",
			DestinationNamespace: "archive",
			SessionNamespace:     "sessions",
			CopySpec:             copyObject.Spec,
		},
	}
	clusterReservation := &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{Name: "reservation"},
		Spec: v1alpha1.ClusterReservationSpec{
			SourceNamespace:      "app",
			DestinationNamespace: "archive",
			SessionNamespace:     "sessions",
			ReservationSpec:      reservation.Spec,
		},
	}

	for _, test := range []struct {
		kind   domain.ControllerKind
		object crclient.Object
		spec   func() any
		submit func(context.Context, *cobra.Command, *commandRuntime) error
	}{
		{"Migration", migration, func() any { return migration.Spec }, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
			return submitMigration(ctx, cmd, rt, migration)
		}},
		{"PodMigration", pod, func() any { return pod.Spec }, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
			return submitPodMigration(ctx, cmd, rt, pod)
		}},
		{"Copy", copyObject, func() any { return copyObject.Spec }, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
			return submitCopy(ctx, cmd, rt, copyObject)
		}},
		{"Reservation", reservation, func() any { return reservation.Spec }, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
			return submitReservation(ctx, cmd, rt, reservation)
		}},
		{"ClusterMigration", clusterMigration, func() any { return clusterMigration.Spec }, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
			return submitClusterMigration(ctx, cmd, rt, clusterMigration)
		}},
		{"ClusterCopy", clusterCopy, func() any { return clusterCopy.Spec }, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
			return submitClusterCopy(ctx, cmd, rt, clusterCopy)
		}},
		{"ClusterReservation", clusterReservation, func() any { return clusterReservation.Spec }, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
			return submitClusterReservation(ctx, cmd, rt, clusterReservation)
		}},
	} {
		t.Run(string(test.kind), func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}

			client := crfake.NewClientBuilder().WithScheme(scheme).Build()
			kubeClient := kubernetesfake.NewClientset()

			var out, diagnostics bytes.Buffer

			cmd := &cobra.Command{}
			cmd.SetErr(&diagnostics)

			rt := &commandRuntime{
				clients: &kube.Clients{Runtime: client, Kubernetes: kubeClient},
				printer: output.Printer{
					Writer: &out,
					Format: output.JSON,
				},
			}

			// Serialization happens before submission: the API server stamps
			// metadata on the object, but its spec must survive untouched.
			want, err := json.Marshal(test.spec())
			if err != nil {
				t.Fatal(err)
			}

			if err := test.submit(t.Context(), cmd, rt); err != nil {
				t.Fatal(err)
			}

			// The cluster families carry their namespace roles in the spec;
			// every other kind lands in the tenant namespace.
			namespace, clusterScoped := "app", strings.HasPrefix(string(test.kind), "Cluster")
			if clusterScoped {
				namespace = ""
			}

			stored := kube.WorkflowObjectForKind(test.kind)
			if err := client.Get(
				t.Context(),
				crclient.ObjectKey{Namespace: namespace, Name: test.object.GetName()},
				stored,
			); err != nil {
				t.Fatal(err)
			}

			encoded, err := json.Marshal(stored)
			if err != nil {
				t.Fatal(err)
			}

			var body struct {
				Spec   json.RawMessage `json:"spec"`
				Status struct {
					Plan json.RawMessage `json:"plan"`
				} `json:"status"`
			}
			if err := json.Unmarshal(encoded, &body); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(body.Spec, want) || len(body.Status.Plan) != 0 ||
				len(stored.GetFinalizers()) == 0 {
				t.Fatalf(
					"submitted CRD changed its spec or manufactured a plan: %s; expected %s",
					encoded,
					want,
				)
			}

			if after, err := json.Marshal(test.spec()); err != nil || !bytes.Equal(want, after) {
				t.Fatal("submission mutated the caller-owned spec")
			}

			if test.kind == "PodMigration" &&
				!bytes.Contains(body.Spec, []byte(`"precopyPasses":0`)) {
				t.Fatalf("zero passes lost: %s", body.Spec)
			}

			for _, action := range kubeClient.Actions() {
				if action.GetVerb() != "get" ||
					action.GetResource().Resource != "configmaps" {
					t.Fatalf("submission performed data-plane discovery: %v", action)
				}
			}

			if clusterScoped && strings.Contains(diagnostics.String(), "kubectl -n") {
				t.Fatalf(
					"cluster workflow received namespaced guidance: %s",
					diagnostics.String(),
				)
			}
		})
	}
}

func TestTransferSubmissionWaitsForOperationCheckpoint(t *testing.T) {
	for _, phase := range []v1alpha1.WorkflowPhase{domain.PhaseReserved, domain.PhaseWarmCopied} {
		t.Run(string(phase), func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}

			metadata := metav1.ObjectMeta{
				Name:            "operation",
				UID:             "workflow-uid",
				ResourceVersion: "2",
			}

			var completed runtime.Object
			if phase == domain.PhaseReserved {
				completed = &v1alpha1.ClusterReservation{
					ObjectMeta: metadata,
					Status: v1alpha1.ClusterReservationStatus{
						WorkflowStatus: v1alpha1.WorkflowStatus{Phase: phase},
					},
				}
			} else {
				completed = &v1alpha1.ClusterCopy{
					ObjectMeta: metadata,
					Status: v1alpha1.ClusterCopyStatus{
						WorkflowStatus: v1alpha1.WorkflowStatus{Phase: phase},
					},
				}
			}

			var out, diagnostics bytes.Buffer

			cmd := &cobra.Command{}
			cmd.SetErr(&diagnostics)

			rt := &commandRuntime{
				clients: &kube.Clients{
					Runtime: repositorySubmissionClient{
						crfake.NewClientBuilder().WithScheme(scheme).Build(),
					},
					Kubernetes: kubernetesfake.NewClientset(),
					Dynamic:    dynamicfake.NewSimpleDynamicClient(scheme, completed),
				},
				waitForController: true, printer: output.Printer{Writer: &out, Format: output.JSON},
			}

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()

			var err error
			if phase == domain.PhaseReserved {
				err = submitClusterReservation(
					ctx,
					cmd,
					rt,
					&v1alpha1.ClusterReservation{
						ObjectMeta: metav1.ObjectMeta{Name: metadata.Name},
						Spec: v1alpha1.ClusterReservationSpec{
							SourceNamespace:      "app",
							DestinationNamespace: "archive",
						},
					},
				)
			} else {
				err = submitClusterCopy(
					ctx,
					cmd,
					rt,
					&v1alpha1.ClusterCopy{
						ObjectMeta: metav1.ObjectMeta{Name: metadata.Name},
						Spec: v1alpha1.ClusterCopySpec{
							SourceNamespace:      "app",
							DestinationNamespace: "archive",
						},
					},
				)
			}

			if err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(out.String(), string(phase)) {
				t.Fatalf("final checkpoint not printed: %s", out.String())
			}
		})
	}
}

func TestTransferCommandsSubmitWithoutLegacyPlannerOrSessionStore(t *testing.T) {
	for _, test := range []struct {
		kind domain.ControllerKind
		args []string
	}{
		{"Migration", []string{"cr", "migrate", "create", "--source-pvc", "missing"}},
		{"PodMigration", []string{"cr", "migrate-pod", "create", "--pod", "missing", "--precopy-passes", "0"}},
		{"Copy", []string{"cr", "copy", "create", "--source-pvc", "missing"}},
		{"Reservation", []string{"cr", "reserve", "create", "--source-pvc", "missing"}},
	} {
		t.Run(string(test.kind), func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}

			client := crfake.NewClientBuilder().WithScheme(scheme).Build()

			var out, diagnostics bytes.Buffer

			root := NewRoot(Options{
				Out: &out, ErrOut: &diagnostics,
				runtimeFactory: func(*rootState) (*commandRuntime, error) {
					return &commandRuntime{
						clients: &kube.Clients{
							Runtime:    client,
							Kubernetes: kubernetesfake.NewClientset(),
						},
						printer:           output.Printer{Writer: &out, Format: output.JSON},
						waitForController: false,
					}, nil
				},
			})
			root.SetArgs(
				append(
					test.args,
					"--yes",
					"--namespace",
					"app",
					"--session",
					"operation",
					"--dry-run=false",
					"--wait=false",
				),
			)

			if err := root.ExecuteContext(t.Context()); err != nil {
				t.Fatalf("command failed: %v; %s", err, diagnostics.String())
			}

			stored := kube.WorkflowObjectForKind(test.kind)
			if err := client.Get(
				t.Context(),
				crclient.ObjectKey{Namespace: "app", Name: "operation"},
				stored,
			); err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(out.String(), `"kind": "`+string(test.kind)+`"`) {
				t.Fatalf("command did not print the concrete CRD: %s", out.String())
			}
		})
	}
}
