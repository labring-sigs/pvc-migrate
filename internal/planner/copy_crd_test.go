package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCopyPlannerPreservesCRDInputAndChecksIdentity(t *testing.T) {
	object := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy"},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "app",
			CopySpec: v1alpha1.CopySpec{
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data", UID: "replaced"}},
				},
				TransferOptions: v1alpha1.TransferOptions{Strategies: []string{"mount"}},
			},
		},
	}
	before := object.DeepCopy()

	_, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).PlanCopy(t.Context(), object, "example/tool:v1")
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("copy planner accepted changed source identity: %v", err)
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("planning mutated the submitted CRD")
	}
}

func TestCopyPlannerRejectsExistingExecutionBeforeDiscovery(t *testing.T) {
	for _, name := range []string{"plan", "progress", "placement", "active", "deleting"} {
		t.Run(name, func(t *testing.T) {
			object := &v1alpha1.ClusterCopy{ObjectMeta: metav1.ObjectMeta{Name: "copy"}}
			switch name {
			case "plan":
				object.Status.Plan = &v1alpha1.ClusterCopyPlan{}
			case "progress":
				object.Status.Volumes = []v1alpha1.ClusterCopyVolumeStatus{{SourcePVCName: "data"}}
			case "placement":
				object.Status.SourceNode = "source-node"
			case "active":
				object.Status.Phase = domain.PhaseWarmCopying
			case "deleting":
				now := metav1.Now()
				object.DeletionTimestamp = &now
			}

			before := object.DeepCopy()

			_, err := New(nil, nil).PlanCopy(t.Context(), object, "example/tool:v1")
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("error = %v", err)
			}

			if !reflect.DeepEqual(object, before) {
				t.Fatal("replanning changed existing execution progress")
			}
		})
	}
}
