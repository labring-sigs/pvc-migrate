package v1alpha1_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func TestReservationTransferSettingsSurviveSerialization(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete=%t", remove), func(t *testing.T) {
			spec := v1alpha1.ReservationSpec{
				TransferOptions: v1alpha1.TransferOptions{
					SourceNode: "source-node", TargetNode: "target-node",
					Strategies:     []string{"mount", "clusterip"},
					VerifyChecksum: true, DeleteExtraneous: new(remove), SkipSourceUsageCheck: true,
				},
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
				},
			}
			plan := v1alpha1.ReservationPlan{
				SourceNode:           "source-node",
				TargetNode:           "target-node",
				ToolImage:            "registry.example/tool:v1",
				Strategies:           []string{"mount", "clusterip"},
				VerifyChecksum:       true,
				DeleteExtraneous:     remove,
				SkipSourceUsageCheck: true,
				Volumes: []v1alpha1.VolumeSpec{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data", UID: "source"}},
				},
			}
			namespaced := v1alpha1.Reservation{
				Spec: spec, Status: v1alpha1.ReservationStatus{Plan: &plan},
			}
			cluster := v1alpha1.ClusterReservation{
				Spec: v1alpha1.ClusterReservationSpec{
					ReservationSpec:      spec,
					SourceNamespace:      "app",
					DestinationNamespace: "archive",
					SessionNamespace:     "control",
				},
				Status: v1alpha1.ClusterReservationStatus{Plan: &v1alpha1.ClusterReservationPlan{
					ReservationPlan:      plan,
					SourceNamespace:      "app",
					DestinationNamespace: "archive",
					SessionNamespace:     "control",
				}},
			}

			if restored := reservationJSONRoundTrip(
				t,
				namespaced,
			); !reflect.DeepEqual(
				restored,
				namespaced,
			) {
				t.Fatalf("namespaced reservation lost spec or plan settings: %+v", restored)
			}

			if restored := reservationJSONRoundTrip(
				t,
				cluster,
			); !reflect.DeepEqual(
				restored,
				cluster,
			) {
				t.Fatalf("cluster reservation lost spec or plan settings: %+v", restored)
			}
		})
	}
}

func reservationJSONRoundTrip[T any](t *testing.T, input T) T {
	t.Helper()

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}

	var output T
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatal(err)
	}

	return output
}
