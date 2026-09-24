package planner

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TestNamespacedPodMigrationRejectsTerminatingSource pins the data-safety
// fence: a PVC or PV whose deletion was already requested can vanish the
// moment a cutover pauses the workload, so it must never plan as a source.
func TestNamespacedPodMigrationRejectsTerminatingSource(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(objects []runtime.Object)
		check  string
	}{
		{
			name: "terminating PVC",
			mutate: func(objects []runtime.Object) {
				for _, object := range objects {
					if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok && pvc.Name == "data" {
						now := metav1.Now()
						pvc.DeletionTimestamp = &now
					}
				}
			},
			check: "PVC app/data is terminating",
		},
		{
			name: "terminating PV",
			mutate: func(objects []runtime.Object) {
				for _, object := range objects {
					if pv, ok := object.(*corev1.PersistentVolume); ok && pv.Name == "pv-source" {
						now := metav1.Now()
						pv.DeletionTimestamp = &now
					}
				}
			},
			check: "PV pv-source is terminating",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			objects := plannerObjects("2Gi")
			node := testutil.MustType[*corev1.Node](t, objects[2]).DeepCopy()
			node.Name = "node-a"
			node.Labels[corev1.LabelHostname] = "node-a"
			pod := podWithPVC("writer")
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			}
			objects = append(objects, node, pod, &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "app"},
			})
			testCase.mutate(objects)

			planner := New(
				plannerClient(objects...),
				controller.NewManager(plannerClient(objects...), nil, nil),
			)
			object := &v1alpha1.PodMigration{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-migration", Namespace: "app"},
				Spec: v1alpha1.PodMigrationSpec{
					Pod:             v1alpha1.LocalResourceReference{Name: "writer"},
					TransferOptions: v1alpha1.TransferOptions{TargetNode: "node-b"},
				},
			}

			report, err := planner.PlanNamespacedPodMigration(
				t.Context(),
				object,
				"example/tool:v1",
			)
			if err != nil {
				t.Fatal(err)
			}

			if report.Ready {
				t.Fatal("terminating source planned successfully")
			}

			found := false
			for _, check := range report.Summary().Checks {
				if !check.Passed && strings.Contains(check.Message, testCase.check) {
					found = true
				}
			}

			if !found {
				t.Fatalf("terminating-source check missing: %+v", report.Summary().Checks)
			}
		})
	}
}
