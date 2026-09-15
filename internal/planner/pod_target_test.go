package planner

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodTargetConstraintsDoNotRestrictCopy(t *testing.T) {
	for _, target := range []string{"node-b", "auto"} {
		t.Run(target, func(t *testing.T) {
			p, migration := podMigrationPlanFixture(t)

			pod, err := p.client.CoreV1().Pods("app").Get(
				t.Context(), "writer", metav1.GetOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}

			pod.Spec.NodeSelector = map[string]string{"workload-pool": "unavailable"}
			if _, err := p.client.CoreV1().Pods("app").Update(
				t.Context(), pod, metav1.UpdateOptions{},
			); err != nil {
				t.Fatal(err)
			}

			migration.Spec.TargetNode = target

			report, err := p.PlanPodMigration(t.Context(), migration, "")
			if err != nil {
				t.Fatal(err)
			}

			check := domain.CheckNamePodScheduling
			if target == "auto" {
				check = domain.CheckNameTargetNode
			}

			if report.Ready || migration.Status.Plan != nil ||
				!hasFailedCheck(report.Checks, check) {
				t.Fatalf("PodMigration ignored Pod target constraints: checks=%+v", report.Checks)
			}

			copyObject := &v1alpha1.ClusterCopy{
				ObjectMeta: metav1.ObjectMeta{Name: "copy"},
				Spec: v1alpha1.ClusterCopySpec{
					SourceNamespace:      "app",
					DestinationNamespace: "system",
					SessionNamespace:     "system",
					CopySpec: v1alpha1.CopySpec{
						Pod:             &v1alpha1.LocalResourceReference{Name: "writer"},
						TransferOptions: v1alpha1.TransferOptions{TargetNode: target},
						Online:          true,
					},
				},
			}

			copyReport, err := p.PlanCopy(t.Context(), copyObject, "")
			if err != nil {
				t.Fatal(err)
			}

			if !copyReport.Ready || copyObject.Status.Plan == nil {
				t.Fatalf("Copy inherited Pod target constraints: checks=%+v", copyReport.Checks)
			}
		})
	}
}
