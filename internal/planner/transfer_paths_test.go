package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestResolveTransferScopesSupportsSingleAndNamedPartialMappings(t *testing.T) {
	single, err := resolveTransferScopes([]string{"data/mysql"}, []string{"."}, []string{"data"})
	if err != nil || len(single) != 1 || single[0] == nil || single[0].SourcePath != "data/mysql" ||
		single[0].DestinationPath != "." {
		t.Fatalf("single scopes=%#v error=%v", single, err)
	}

	multiple, err := resolveTransferScopes(
		[]string{"logs=archive/current"},
		[]string{"data=restored/data", "logs=."},
		[]string{"data", "logs", "cache"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(multiple) != 3 || multiple[0] == nil ||
		multiple[0].SourcePath != domain.VolumeRootPath ||
		multiple[0].DestinationPath != "restored/data" ||
		multiple[1] == nil ||
		multiple[1].SourcePath != "archive/current" ||
		multiple[1].DestinationPath != domain.VolumeRootPath ||
		multiple[2] != nil {
		t.Fatalf("multiple scopes=%#v", multiple)
	}
}

func TestResolveTransferScopesRejectsAmbiguousAndUnsafeMappings(t *testing.T) {
	for _, test := range []struct {
		name        string
		source      []string
		destination []string
		want        string
	}{
		{name: "bare multi PVC", source: []string{"data/mysql"}, want: "bare --source-path"},
		{name: "unknown", source: []string{"other=data"}, want: "unknown source PVC"},
		{name: "duplicate", source: []string{"data=a", "data=b"}, want: "more than once"},
		{name: "empty", destination: []string{"data="}, want: "use source-pvc-name=relative-path"},
		{name: "traversal", source: []string{"data=../secret"}, want: "parent traversal"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolveTransferScopes(test.source, test.destination, []string{"data", "logs"})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestPlanPersistsTransferScopeAndWarnsForOrchestratedMigration(t *testing.T) {
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).plan(context.Background(), planOptions{
		SessionID:            "partial-path",
		Operation:            domain.OperationMigrate,
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "app",
		StagingNamespace:     "system",
		SessionNamespace:     "system",
		SourcePVCs: []string{
			"data",
		},
		SourcePaths:      []string{"data=mysql/current"},
		DestinationPaths: []string{"data=restore/mysql"},
		TargetNode:       "node-b",
		DestinationClass: "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(plan.Volumes) != 1 || plan.Volumes[0].TransferScope == nil ||
		plan.Volumes[0].TransferScope.SourcePath != "mysql/current" ||
		plan.Volumes[0].TransferScope.DestinationPath != "restore/mysql" {
		t.Fatalf("planned volumes=%#v", plan.Volumes)
	}

	if len(plan.SessionSpec.Volumes) != 1 || plan.SessionSpec.Volumes[0].TransferScope == nil ||
		plan.SessionSpec.Volumes[0].TransferScope == plan.Volumes[0].TransferScope {
		t.Fatalf("session scope=%#v plan scope=%#v", plan.SessionSpec.Volumes, plan.Volumes)
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
		SessionID:            "partial-shrink",
		Operation:            domain.OperationCopy,
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "system",
		StagingNamespace:     "system",
		SessionNamespace:     "system",
		SourcePVCs: []string{
			"data",
		},
		SourcePaths:           []string{"data=selected"},
		DestinationCapacities: []string{"1Gi"},
		AllowVolumeShrink:     true,
		TargetNode:            "node-b",
		DestinationClass:      "fast",
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
			plan,
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
