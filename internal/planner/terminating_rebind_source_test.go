package planner

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// terminatingRebindCase mutates the planner world into the terminating shape
// under test.
type terminatingRebindCase struct {
	name   string
	check  string
	mutate func(objects []runtime.Object)
}

func terminatingRebindCases() []terminatingRebindCase {
	return []terminatingRebindCase{
		{
			name:  "terminating PVC",
			check: "PVC app/data is terminating",
			mutate: func(objects []runtime.Object) {
				for _, object := range objects {
					if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok && pvc.Name == "data" {
						now := metav1.Now()
						pvc.DeletionTimestamp = &now
					}
				}
			},
		},
		{
			name:  "terminating PV",
			check: "PV pv-source is terminating",
			mutate: func(objects []runtime.Object) {
				for _, object := range objects {
					if pv, ok := object.(*corev1.PersistentVolume); ok && pv.Name == "pv-source" {
						now := metav1.Now()
						pv.DeletionTimestamp = &now
					}
				}
			},
		},
	}
}

func TestPlanMoveRejectsTerminatingSource(t *testing.T) {
	for _, testCase := range terminatingRebindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			objects := append(
				plannerObjects("2Gi"),
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "archive"}},
			)
			testCase.mutate(objects)

			planner := New(plannerClient(objects...), nil)
			object := &v1alpha1.Move{
				ObjectMeta: metav1.ObjectMeta{Name: "move"},
				Spec: v1alpha1.MoveSpec{
					SourceNamespace:      "app",
					DestinationNamespace: "archive",
					SessionNamespace:     "system",
					SourcePVC:            v1alpha1.LocalResourceReference{Name: "data"},
				},
			}

			report, err := planner.PlanMove(t.Context(), object, "system")
			if err != nil {
				t.Fatal(err)
			}

			if report.Ready || object.Status.Plan != nil {
				t.Fatalf("terminating source planned successfully: %+v", report)
			}

			for _, check := range report.Summary().Checks {
				if !check.Passed && strings.Contains(check.Message, testCase.check) {
					return
				}
			}

			t.Fatalf("terminating-source check missing: %+v", report.Summary().Checks)
		})
	}
}

func TestPlanRenameRejectsTerminatingSource(t *testing.T) {
	for _, testCase := range terminatingRebindCases() {
		t.Run(testCase.name, func(t *testing.T) {
			objects := plannerObjects("2Gi")
			testCase.mutate(objects)

			planner := New(plannerClient(objects...), nil)
			object := &v1alpha1.Rename{
				ObjectMeta: metav1.ObjectMeta{Name: "rename", Namespace: "app"},
				Spec: v1alpha1.RenameSpec{
					SourcePVC:      v1alpha1.LocalResourceReference{Name: "data"},
					DestinationPVC: v1alpha1.LocalResourceReference{Name: "renamed"},
				},
			}

			report, err := planner.PlanRename(t.Context(), object, "system")
			if err != nil {
				t.Fatal(err)
			}

			if report.Ready || object.Status.Plan != nil {
				t.Fatalf("terminating source planned successfully: %+v", report)
			}

			for _, check := range report.Summary().Checks {
				if !check.Passed && strings.Contains(check.Message, testCase.check) {
					return
				}
			}

			t.Fatalf("terminating-source check missing: %+v", report.Summary().Checks)
		})
	}
}
