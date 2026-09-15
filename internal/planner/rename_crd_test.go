package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlanRenameUsesCRDAndProtectsExecutionIdentity(t *testing.T) {
	planner := New(plannerClient(plannerObjects("2Gi")...), nil)
	object := &v1alpha1.Rename{
		ObjectMeta: metav1.ObjectMeta{Name: "rename", Namespace: "app"},
		Spec: v1alpha1.RenameSpec{
			SourcePVC:      v1alpha1.LocalResourceReference{Name: "data"},
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "renamed"},
		},
	}
	before := object.DeepCopy()

	report, err := planner.PlanRename(t.Context(), object, "system")
	if err != nil || !report.Ready {
		t.Fatalf("planning failed: %v; report=%+v", err, report)
	}

	if !reflect.DeepEqual(object.Spec, before.Spec) || object.Status.Plan == nil ||
		object.Status.Plan.SourcePVC.UID == "" || object.Status.Plan.SourcePV.UID == "" {
		t.Fatalf("incomplete CRD planning result: %+v", object)
	}

	for _, phase := range []domain.Phase{domain.PhasePlanned, domain.PhaseRenaming, domain.PhaseFailed, domain.PhaseCompleted} {
		object.Status.Phase = phase

		before = object.DeepCopy()
		if _, err := planner.PlanRename(
			t.Context(),
			object,
			"system",
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("replanning accepted in %s: %v", phase, err)
		}

		if !reflect.DeepEqual(object, before) {
			t.Fatal("rejected replanning changed the execution checkpoint")
		}
	}
}

func TestPlanRenameRejectsMissingObject(t *testing.T) {
	if _, err := New(
		plannerClient(),
		nil,
	).PlanRename(t.Context(), nil, "system"); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("missing object accepted: %v", err)
	}
}
