package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNamespacedMigrationPlanningUsesOperationPlanDirectly(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	object := &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "app"},
		Spec: v1alpha1.MigrationSpec{
			Volumes: []v1alpha1.VolumeRequest{
				{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
			},
		},
	}
	before := object.Spec.DeepCopy()

	report, err := New(client, nil).PlanNamespacedMigration(t.Context(), object, "example/tool:v1")
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
	).PlanNamespacedMigration(t.Context(), object, "example/tool:v1"); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("replanning existing identities accepted: %v", err)
	}

	if len(client.Actions()) != 0 || !reflect.DeepEqual(object.Status.Plan, resolved) {
		t.Fatal("replanning touched resources or the frozen plan")
	}
}

func TestNamespacedMigrationRejectsExecutionStateBeforeDiscovery(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*v1alpha1.Migration)
	}{
		{name: "reservation checkpoint", mutate: func(object *v1alpha1.Migration) { object.Status.Volumes = []v1alpha1.MigrationVolumeStatus{{}} }},
		{name: "active pass", mutate: func(object *v1alpha1.Migration) { object.Status.Phase = v1alpha1.WorkflowPhase("FinalSyncing") }},
		{name: "deleting", mutate: func(object *v1alpha1.Migration) { now := metav1.Now(); object.DeletionTimestamp = &now }},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := plannerClient(plannerObjects("2Gi")...)
			object := &v1alpha1.Migration{
				ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "app"},
			}
			test.mutate(object)
			before := object.DeepCopy()

			_, err := New(
				client,
				nil,
			).PlanNamespacedMigration(t.Context(), object, "example/tool:v1")
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("execution state accepted for planning: %v", err)
			}

			if len(client.Actions()) != 0 || !reflect.DeepEqual(object, before) {
				t.Fatal("rejected planning touched resources or workflow")
			}
		})
	}
}
