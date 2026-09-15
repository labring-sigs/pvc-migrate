package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
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
	for _, scope := range []string{"namespaced", "data namespaces", "session namespace"} {
		t.Run(scope, func(t *testing.T) {
			destination, sessionNamespace := v1alpha1.NamespaceName(
				"app",
			), v1alpha1.NamespaceName(
				"app",
			)
			if scope == "data namespaces" {
				destination = "staging"
			}

			if scope == "session namespace" {
				sessionNamespace = "sessions"
			}

			options := v1alpha1.TransferOptions{
				DestinationPVCReclaimPolicy: "Delete", Strategies: []string{"mount"},
				SourceNode: "worker", TargetNode: "auto", SourcePath: "logs", DestinationPath: ".",
				VerifyChecksum: true, DeleteExtraneous: true,
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
			migration := &v1alpha1.ClusterMigration{
				ObjectMeta: metav1.ObjectMeta{Name: "migration"},
				Spec: v1alpha1.ClusterMigrationSpec{
					SourceNamespace:    "app",
					TemporaryNamespace: destination,
					SessionNamespace:   sessionNamespace,
					MigrationSpec: v1alpha1.MigrationSpec{
						Volumes:               volumes,
						TransferOptions:       options,
						SourcePVReclaimPolicy: "Delete",
					},
				},
			}
			pod := &v1alpha1.ClusterPodMigration{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-migration"},
				Spec: v1alpha1.ClusterPodMigrationSpec{
					SourceNamespace:    "app",
					TemporaryNamespace: destination,
					SessionNamespace:   sessionNamespace,
					PodMigrationSpec: v1alpha1.PodMigrationSpec{
						Volumes:               volumes,
						TransferOptions:       options,
						SourcePVReclaimPolicy: "Delete",
						Pod: v1alpha1.LocalResourceReference{
							Name: "database",
							UID:  "pod-uid",
						},
						PrecopyPasses: 0,
					},
				},
			}
			copyObject := &v1alpha1.ClusterCopy{
				ObjectMeta: metav1.ObjectMeta{Name: "copy"},
				Spec: v1alpha1.ClusterCopySpec{
					SourceNamespace:      "app",
					DestinationNamespace: destination,
					SessionNamespace:     sessionNamespace,
					CopySpec: v1alpha1.CopySpec{
						Volumes:         volumes,
						TransferOptions: options,
						Online:          true,
					},
				},
			}

			reservation := &v1alpha1.ClusterReservation{
				ObjectMeta: metav1.ObjectMeta{Name: "reservation"},
				Spec: v1alpha1.ClusterReservationSpec{
					SourceNamespace:      "app",
					DestinationNamespace: destination,
					SessionNamespace:     sessionNamespace,
					ReservationSpec: v1alpha1.ReservationSpec{
						Volumes:         volumes,
						TransferOptions: options,
					},
				},
			}
			for _, test := range []struct {
				kind        domain.ControllerKind
				object      crclient.Object
				localSpec   any
				clusterSpec any
				submit      func(context.Context, *cobra.Command, *commandRuntime) error
			}{
				{"Migration", migration, migration.Spec.MigrationSpec, migration.Spec, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
					return submitMigration(ctx, cmd, rt, migration)
				}},
				{"PodMigration", pod, pod.Spec.PodMigrationSpec, pod.Spec, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
					return submitPodMigration(ctx, cmd, rt, pod)
				}},
				{"Copy", copyObject, copyObject.Spec.CopySpec, copyObject.Spec, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
					return submitCopy(ctx, cmd, rt, copyObject)
				}},
				{"Reservation", reservation, reservation.Spec.ReservationSpec, reservation.Spec, func(ctx context.Context, cmd *cobra.Command, rt *commandRuntime) error {
					return submitReservation(ctx, cmd, rt, reservation)
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

					before := test.object.DeepCopyObject()
					if err := test.submit(t.Context(), cmd, rt); err != nil {
						t.Fatal(err)
					}

					kind, namespace, expected := test.kind, "app", test.localSpec
					if scope != "namespaced" {
						kind, namespace, expected = domain.ControllerKind(
							"Cluster"+string(kind),
						), "", test.clusterSpec
					}

					stored := kube.WorkflowObjectForKind(kind)
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

					want, err := json.Marshal(expected)
					if err != nil {
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

					if !reflect.DeepEqual(test.object, before) {
						t.Fatal("submission mutated caller-owned CRD")
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

					if scope != "namespaced" &&
						strings.Contains(diagnostics.String(), "kubectl -n") {
						t.Fatalf(
							"cluster workflow received namespaced guidance: %s",
							diagnostics.String(),
						)
					}
				})
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
				err = submitReservation(
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
				err = submitCopy(
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
		{"Migration", []string{"migrate", "create", "--source-pvc", "missing", "--temporary-namespace", "app"}},
		{"PodMigration", []string{"migrate-pod", "create", "--pod", "missing", "--temporary-namespace", "app", "--precopy-passes", "0"}},
		{"Copy", []string{"copy", "create", "--source-pvc", "missing"}},
		{"Reservation", []string{"reserve", "create", "--source-pvc", "missing"}},
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
					"--session-namespace",
					"app",
					"--source-namespace",
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
