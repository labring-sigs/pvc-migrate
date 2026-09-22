package cli

import (
	"errors"
	"io"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCopyAdoptionLocksRecordedNamespaceBeforeHandoff(t *testing.T) {
	for _, source := range activeReservationCLIObjects() {
		t.Run(source.GetNamespace(), func(t *testing.T) {
			source.SetUID("stored-reservation")
			source.SetResourceVersion("1")

			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}

			client := fake.NewClientset()
			stopped := errors.New("lease unavailable")
			lockNamespace := ""
			client.PrependReactor(
				"create",
				"leases",
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					lockNamespace = action.GetNamespace()
					return true, nil, stopped
				},
			)
			rt := &commandRuntime{
				clients: &kube.Clients{
					Kubernetes: client,
					Runtime: crfake.NewClientBuilder().
						WithScheme(scheme).
						WithObjects(source).
						Build(),
				},
			}
			state := &rootState{}
			state.global.workflowNamespace = "lookup-scope"
			cmd := &cobra.Command{}
			cmd.SetErr(io.Discard)

			flags := &copyFlags{}
			flags.bind(cmd)

			var (
				err      error
				expected string
			)
			switch object := source.(type) {
			case *v1alpha1.Reservation:
				object.Status.Phase = domain.PhaseReserved
				object.Status.Volumes = []v1alpha1.ReservationVolumeStatus{
					{VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
						SourcePVCName: "source",
						Reserved:      true,
						DestinationPVC: &v1alpha1.LocalResourceReference{
							Name: "destination",
							UID:  "destination",
						},
						DestinationPV: &v1alpha1.LocalResourceReference{
							Name: "destination-pv",
							UID:  "destination-pv",
						},
					}},
				}
				expected = object.Namespace
				err = state.adoptReservation(
					t.Context(),
					cmd,
					rt,
					object,
					flags,
					false,
					backendConfigMap,
				)
			case *v1alpha1.ClusterReservation:
				object.Status.Phase = domain.PhaseReserved
				object.Status.Volumes = []v1alpha1.ClusterReservationVolumeStatus{
					{ClusterVolumeReservationStatus: v1alpha1.ClusterVolumeReservationStatus{
						SourcePVCName: "source",
						Reserved:      true,
						DestinationPVC: &v1alpha1.ObjectReference{
							Namespace: string(object.Spec.DestinationNamespace),
							Name:      "destination",
							UID:       "destination",
						},
						DestinationPV: &v1alpha1.ObjectReference{
							Name: "destination-pv",
							UID:  "destination-pv",
						},
					}},
				}
				expected = string(object.Spec.SessionNamespace)
				err = state.adoptClusterReservation(
					t.Context(),
					cmd,
					rt,
					object,
					flags,
					false,
					backendConfigMap,
				)
			}

			if !errors.Is(err, stopped) || lockNamespace != expected {
				t.Fatalf("namespace=%s want=%s error=%v", lockNamespace, expected, err)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() == "create" && action.GetResource().Resource != "leases" {
					t.Fatalf("failed lock reached data-plane mutation: %v", action)
				}
			}
		})
	}
}
