package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNamespacedReservationPlanningUsesOperationPlanDirectly(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	object := &v1alpha1.Reservation{
		ObjectMeta: metav1.ObjectMeta{Name: "reserve", Namespace: "app"},
		Spec: v1alpha1.ReservationSpec{
			Volumes: []v1alpha1.VolumeRequest{
				{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
			},
		},
	}
	before := object.Spec.DeepCopy()

	report, err := New(client, nil).PlanReservation(t.Context(), object, "example/tool:v1")
	if err != nil || report == nil || !report.Ready || object.Status.Plan == nil {
		t.Fatalf("planning failed: %+v; %v", report, err)
	}

	if !reflect.DeepEqual(object.Spec, *before) || len(object.Status.Plan.Volumes) != 1 ||
		object.Status.Plan.Volumes[0].SourcePVC.UID != "pvc-uid" {
		t.Fatal("planning lost source identity or changed input")
	}

	if report.SessionNamespace != object.Namespace ||
		report.DestinationNamespace != object.Namespace {
		t.Fatal("planning escaped metadata.namespace")
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" &&
			action.GetResource().Resource != "selfsubjectaccessreviews" {
			t.Fatalf("planning performed a resource mutation: %v", action)
		}
	}

	resolved := object.Status.Plan.DeepCopy()

	client.ClearActions()

	if _, err := New(
		client,
		nil,
	).PlanReservation(t.Context(), object, "example/tool:v1"); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("replanning existing identities accepted: %v", err)
	}

	if len(client.Actions()) != 0 || !reflect.DeepEqual(object.Status.Plan, resolved) {
		t.Fatal("replanning touched resources or the frozen plan")
	}
}
