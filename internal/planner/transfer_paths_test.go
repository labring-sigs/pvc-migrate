package planner

import (
	"context"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlanPersistsTransferScopeAndWarnsForOrchestratedMigration(t *testing.T) {
	object := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "partial-path"},
		Spec: v1alpha1.ClusterMigrationSpec{
			SourceNamespace: "app", TemporaryNamespace: "system", SessionNamespace: "system",
			MigrationSpec: v1alpha1.MigrationSpec{
				Volumes: testSourceVolumes("data"),
				TransferOptions: v1alpha1.TransferOptions{
					SourcePath:              "mysql/current",
					DestinationPath:         "restore/mysql",
					TargetNode:              "node-b",
					DestinationStorageClass: "fast",
				},
			},
		},
	}

	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).PlanOfflineMigration(context.Background(), object, "")
	if err != nil {
		t.Fatal(err)
	}

	if len(plan.Volumes) != 1 || plan.Volumes[0].TransferScope == nil ||
		plan.Volumes[0].TransferScope.SourcePath != "mysql/current" ||
		plan.Volumes[0].TransferScope.DestinationPath != "restore/mysql" {
		t.Fatalf("planned volumes=%#v", plan.Volumes)
	}

	if len(object.Status.Plan.Volumes) != 1 ||
		object.Status.Plan.Volumes[0].TransferScope == nil ||
		object.Status.Plan.Volumes[0].TransferScope == plan.Volumes[0].TransferScope {
		t.Fatalf(
			"session scope=%#v plan scope=%#v",
			object.Status.Plan.Volumes,
			plan.Volumes,
		)
	}

	foundWarning := false
	for _, check := range plan.Checks {
		if check.Name == "transfer-scope" && check.Severity == domain.SeverityWarning &&
			strings.Contains(check.Message, "content outside") {
			foundWarning = true
		}
	}

	if !foundWarning {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPartialSourceShrinkTreatsWholeVolumeUsageAsInconclusive(t *testing.T) {
	options := planOptions{
		operationKind: domain.OperationCopy,
		Volumes: testSourceVolumes(
			"data",
		), SessionID: "partial-shrink",

		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "system",
		StagingNamespace:     "system",
		SessionNamespace:     "system",

		TransferOptions: v1alpha1.TransferOptions{
			SourcePath:              "selected",
			DestinationCapacity:     "1Gi",
			AllowVolumeShrink:       true,
			TargetNode:              "node-b",
			DestinationStorageClass: "fast",
		},
	}

	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).WithVolumeUsageReader(staticUsageReader{bytes: 1536 << 20}).
		plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready ||
		!hasFailedCheckContaining(
			plan.Checks,
			"source-usage",
			"cannot prove that selected source directory",
		) {
		t.Fatalf("checks=%#v", plan.Checks)
	}

	options.SkipSourceUsageCheck = true

	plan, err = New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).WithVolumeUsageReader(staticUsageReader{bytes: 1536 << 20}).
		plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("explicit skip plan ready=%t error=%v checks=%#v", plan.Ready, err, plan.Checks)
	}
}
