package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlanMoveUsesIndependentCRDNamespacesAndPreservesSpec(t *testing.T) {
	objects := append(
		plannerObjects("2Gi"),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "archive"}},
	)
	planner := New(plannerClient(objects...), nil)
	object := &v1alpha1.Move{
		ObjectMeta: metav1.ObjectMeta{Name: "move"},
		Spec: v1alpha1.MoveSpec{
			SourceNamespace: "app", DestinationNamespace: "archive", SessionNamespace: "system",
			SourcePVC: v1alpha1.LocalResourceReference{Name: "data"},
		},
	}
	before := object.DeepCopy()

	report, err := planner.PlanMove(t.Context(), object, "system")
	if err != nil || report == nil || !report.Ready {
		t.Fatalf("Move planning failed: %v; %+v", err, report)
	}

	plan := object.Status.Plan
	if !reflect.DeepEqual(object.Spec, before.Spec) || plan == nil ||
		plan.SourceNamespace != "app" || plan.DestinationNamespace != "archive" ||
		plan.SessionNamespace != "system" || plan.Identity.DestinationPVC.Name != "data" ||
		plan.Identity.SourcePVC.UID != "pvc-uid" {
		t.Fatalf("Move lost its independent namespace roles: %+v", object)
	}

	for _, phase := range []domain.Phase{domain.PhasePlanned, domain.PhaseMoving, domain.PhaseFailed, domain.PhaseCompleted} {
		object.Status.Phase = phase

		before = object.DeepCopy()
		if _, err := planner.PlanMove(
			t.Context(),
			object,
			"system",
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("replanning accepted in %s: %v", phase, err)
		}

		if !reflect.DeepEqual(object, before) {
			t.Fatal("rejected replanning changed durable identity")
		}
	}
}

func TestPlanMoveRejectsStorageNamespaceRedirection(t *testing.T) {
	object := &v1alpha1.Move{Spec: v1alpha1.MoveSpec{SessionNamespace: "owned"}}
	if _, err := New(
		plannerClient(),
		nil,
	).PlanMove(t.Context(), object, "other"); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("different storage namespace accepted: %v", err)
	}
}
