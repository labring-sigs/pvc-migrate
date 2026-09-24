package planner

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestKubeBlocksCapacityRuleBelongsToPodMigration(t *testing.T) {
	p, migration := podMigrationPlanFixture(t)

	pvc, err := p.client.CoreV1().
		PersistentVolumeClaims("app").
		Get(t.Context(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pvc.Labels = map[string]string{"apps.kubeblocks.io/component-name": "database"}
	if _, err := p.client.CoreV1().
		PersistentVolumeClaims("app").
		Update(t.Context(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	options := v1alpha1.TransferOptions{
		TargetNode: "node-b", DestinationCapacity: "1Gi",
		AllowVolumeShrink: true, SkipSourceUsageCheck: true,
	}
	migration.Spec.TransferOptions = options

	report, err := p.PlanNamespacedPodMigration(t.Context(), migration, "")
	if err != nil {
		t.Fatal(err)
	}

	if report.Ready || migration.Status.Plan != nil ||
		!hasFailedCheck(report.Checks, "destination-capacity") {
		t.Fatalf("PodMigration accepted KubeBlocks capacity change: checks=%+v", report.Checks)
	}

	copyObject := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy"},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace: "app", DestinationNamespace: "system", SessionNamespace: "system",
			CopySpec: v1alpha1.CopySpec{
				Pod:             &v1alpha1.LocalResourceReference{Name: "writer"},
				TransferOptions: options, Online: true,
			},
		},
	}

	report, err = p.PlanCopy(t.Context(), copyObject, "")
	if err != nil {
		t.Fatal(err)
	}

	if !report.Ready || copyObject.Status.Plan == nil ||
		copyObject.Status.Plan.Volumes[0].Capacity != "1Gi" {
		t.Fatalf("Copy inherited PodMigration capacity restriction: checks=%+v", report.Checks)
	}
}
