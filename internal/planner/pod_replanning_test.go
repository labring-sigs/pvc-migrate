package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodMigrationRejectsReplanningBeforeDiscovery(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*v1alpha1.PodMigration)
	}{
		{"existing plan", func(o *v1alpha1.PodMigration) { o.Status.Plan = &v1alpha1.PodMigrationPlan{} }},
		{"volume checkpoint", func(o *v1alpha1.PodMigration) {
			o.Status.Volumes = []v1alpha1.PodMigrationVolumeStatus{{}}
		}},
		{"workload checkpoint", func(o *v1alpha1.PodMigration) {
			o.Status.Workload = &v1alpha1.PodMigrationWorkloadStatus{}
		}},
		{"warm pass", func(o *v1alpha1.PodMigration) { o.Status.WarmPassesCompleted = 1 }},
		{"pod snapshot", func(o *v1alpha1.PodMigration) { o.Status.OriginalPodSnapshotHash = "snapshot" }},
		{"shared mount", func(o *v1alpha1.PodMigration) {
			o.Status.OpenEBSLVMSharedMounts = []v1alpha1.SharedMountStatus{{}}
		}},
		{"active phase", func(o *v1alpha1.PodMigration) { o.Status.Phase = domain.PhaseWarmCopying }},
		{"deleting", func(o *v1alpha1.PodMigration) { now := metav1.Now(); o.DeletionTimestamp = &now }},
	} {
		t.Run(test.name, func(t *testing.T) {
			object := &v1alpha1.PodMigration{
				ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "app"},
			}
			test.mutate(object)
			before := object.DeepCopy()

			_, err := New(nil, nil).PlanNamespacedPodMigration(t.Context(), object, "")
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("replanning accepted: %v", err)
			}

			if !reflect.DeepEqual(object, before) {
				t.Fatal("rejected planning mutated persisted state")
			}
		})
	}
}
