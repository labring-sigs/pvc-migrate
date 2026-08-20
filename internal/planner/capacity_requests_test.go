package planner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func planWithDestinationCapacity(
	t *testing.T,
	capacities []string,
	allowShrink bool,
) *domain.MigrationPlan {
	t.Helper()

	return planWithDestinationCapacityObjects(
		t,
		plannerObjects("2Gi"),
		[]string{"data"},
		capacities,
		allowShrink,
	)
}

func planWithDestinationCapacityObjects(
	t *testing.T,
	objects []runtime.Object,
	sourcePVCs, capacities []string,
	allowShrink bool,
) *domain.MigrationPlan {
	t.Helper()

	plan, err := New(
		plannerClient(objects...),
		nil,
	).WithVolumeUsageReader(staticUsageReader{bytes: 1024}).
		Plan(context.Background(), Options{
			SessionID:             "capacity-test",
			Operation:             domain.OperationCopy,
			SourceNamespace:       "app",
			TemporaryNamespace:    "system",
			DestinationNamespace:  "system",
			StagingNamespace:      "system",
			SessionNamespace:      "system",
			SourcePVCs:            sourcePVCs,
			TargetNode:            "node-b",
			DestinationClass:      "fast",
			DestinationCapacities: capacities,
			AllowVolumeShrink:     allowShrink,
		})
	if err != nil {
		t.Fatal(err)
	}

	return plan
}

type staticUsageReader struct{ bytes int64 }

func (p staticUsageReader) Read(
	context.Context,
	kube.VolumeUsageReadOptions,
) (kube.VolumeUsageReadResult, error) {
	return kube.VolumeUsageReadResult{UsedBytes: p.bytes, Source: "test storage CRD"}, nil
}

func TestPlanUsesRequestedDestinationCapacity(t *testing.T) {
	plan := planWithDestinationCapacity(t, []string{"3Gi"}, false)
	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}

	if len(plan.Volumes) != 1 || plan.Volumes[0].SourceCapacity != "2Gi" ||
		plan.Volumes[0].Capacity != "3Gi" {
		t.Fatalf("planned volumes=%#v", plan.Volumes)
	}

	volume := plan.SessionSpec.Volumes[0]
	if volume.SourceCapacity != "2Gi" || volume.Capacity != "3Gi" {
		t.Fatalf("session volume=%#v", volume)
	}

	if plan.TemporaryUsage.StorageRequests != "3Gi" ||
		plan.TemporaryUsage.ByStorageClass["fast"] != "3Gi" {
		t.Fatalf("temporary usage=%#v", plan.TemporaryUsage)
	}

	if plan.RollbackRetention.StorageRequests != "2Gi" ||
		plan.RollbackRetention.ByStorageClass["fast"] != "2Gi" {
		t.Fatalf("rollback retention=%#v", plan.RollbackRetention)
	}
}

func TestPlanDefaultsDestinationCapacityToSourcePVCapacity(t *testing.T) {
	plan := planWithDestinationCapacity(t, nil, false)
	if !plan.Ready || len(plan.Volumes) != 1 {
		t.Fatalf("plan=%#v", plan)
	}

	if plan.Volumes[0].SourceCapacity != "2Gi" || plan.Volumes[0].Capacity != "2Gi" {
		t.Fatalf("planned volume=%#v", plan.Volumes[0])
	}

	if plan.SessionSpec.Volumes[0].SourceCapacity != "2Gi" ||
		plan.SessionSpec.Volumes[0].Capacity != "2Gi" {
		t.Fatalf("session volume=%#v", plan.SessionSpec.Volumes[0])
	}
}

func TestPlanRejectsDestinationShrinkWithoutExplicitApproval(t *testing.T) {
	plan := planWithDestinationCapacity(t, []string{"1Gi"}, false)
	if plan.Ready || !hasFailedCheck(plan, "destination-capacity") {
		t.Fatalf("plan=%#v", plan)
	}

	if !strings.Contains(planCheckMessage(plan, "destination-capacity"), "--allow-volume-shrink") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanAllowsExplicitDestinationShrinkWithWarning(t *testing.T) {
	plan := planWithDestinationCapacity(t, []string{"1Gi"}, true)
	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}

	if plan.Volumes[0].SourceCapacity != "2Gi" || plan.Volumes[0].Capacity != "1Gi" ||
		plan.TemporaryUsage.StorageRequests != "1Gi" {
		t.Fatalf("plan=%#v", plan)
	}

	found := false
	for _, check := range plan.Checks {
		if check.Name == "destination-capacity" && check.Passed &&
			check.Severity == domain.SeverityWarning &&
			strings.Contains(check.Message, "known to fit") {
			found = true
		}
	}

	if !found {
		t.Fatalf("shrink warning missing: %#v", plan.Checks)
	}
}

type errorUsageReader struct{}

func (errorUsageReader) Read(
	context.Context,
	kube.VolumeUsageReadOptions,
) (kube.VolumeUsageReadResult, error) {
	return kube.VolumeUsageReadResult{}, errors.New("backend CRD has no usage field")
}

func TestPlanRejectsBackendUsageAboveShrinkTarget(t *testing.T) {
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).WithVolumeUsageReader(staticUsageReader{bytes: 2 << 30}).
		Plan(context.Background(), Options{
			SessionID:             "capacity-overflow",
			Operation:             domain.OperationCopy,
			SourceNamespace:       "app",
			TemporaryNamespace:    "system",
			DestinationNamespace:  "system",
			StagingNamespace:      "system",
			SessionNamespace:      "system",
			SourcePVCs:            []string{"data"},
			TargetNode:            "node-b",
			DestinationClass:      "fast",
			DestinationCapacities: []string{"1Gi"},
			AllowVolumeShrink:     true,
		})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "source-usage") {
		t.Fatalf("expected measured overflow failure: %#v", plan.Checks)
	}
}

func TestPlanRequiresExplicitSourceUsageSkip(t *testing.T) {
	base := Options{
		SessionID:             "capacity-unknown",
		Operation:             domain.OperationCopy,
		SourceNamespace:       "app",
		TemporaryNamespace:    "system",
		DestinationNamespace:  "system",
		StagingNamespace:      "system",
		SessionNamespace:      "system",
		SourcePVCs:            []string{"data"},
		TargetNode:            "node-b",
		DestinationClass:      "fast",
		DestinationCapacities: []string{"1Gi"},
		AllowVolumeShrink:     true,
	}

	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).WithVolumeUsageReader(errorUsageReader{}).
		Plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "source-usage") {
		t.Fatalf("expected unknown usage failure: %#v", plan.Checks)
	}

	base.SkipSourceUsageCheck = true

	plan, err = New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).WithVolumeUsageReader(errorUsageReader{}).
		Plan(context.Background(), base)
	if err != nil || !plan.Ready {
		t.Fatalf("expected explicit source-usage skip: err=%v checks=%#v", err, plan.Checks)
	}
}

func TestPlanRequiresTrustedReaderByDefault(t *testing.T) {
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).Plan(context.Background(), Options{
		SessionID:             "capacity-no-reader",
		Operation:             domain.OperationCopy,
		SourceNamespace:       "app",
		TemporaryNamespace:    "system",
		DestinationNamespace:  "system",
		StagingNamespace:      "system",
		SessionNamespace:      "system",
		SourcePVCs:            []string{"data"},
		TargetNode:            "node-b",
		DestinationClass:      "fast",
		DestinationCapacities: []string{"1Gi"},
		AllowVolumeShrink:     true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "source-usage") {
		t.Fatalf("expected missing trusted reader failure: %#v", plan.Checks)
	}
}

func TestPlanKeepsCompleteDiagnosticsForInvalidCapacity(t *testing.T) {
	plan := planWithDestinationCapacity(t, []string{"invalid"}, false)
	if plan.Ready || !hasFailedCheck(plan, "destination-capacity") {
		t.Fatalf("plan=%#v", plan)
	}

	if len(plan.Volumes) != 1 || plan.Volumes[0].Capacity != "2Gi" ||
		plan.TemporaryUsage.StorageRequests != "2Gi" {
		t.Fatalf("failed plan lost source-sized diagnostics: %#v", plan)
	}
}

func TestPlanRejectsCapacityCountMismatchWithoutDroppingVolumes(t *testing.T) {
	objects := plannerObjects("2Gi")
	dataPVC := testutil.MustType[*corev1.PersistentVolumeClaim](t, objects[5])
	dataPV := testutil.MustType[*corev1.PersistentVolume](t, objects[6])
	logsPVC := dataPVC.DeepCopy()
	logsPVC.Name = "logs"
	logsPVC.UID = types.UID("logs-pvc-uid")
	logsPVC.ResourceVersion = "11"
	logsPVC.Spec.VolumeName = "pv-logs"
	logsPV := dataPV.DeepCopy()
	logsPV.Name = "pv-logs"
	logsPV.UID = types.UID("logs-pv-uid")
	logsPV.ResourceVersion = "21"
	logsPV.Spec.ClaimRef = &corev1.ObjectReference{Namespace: "app", Name: "logs", UID: logsPVC.UID}
	objects = append(objects, logsPVC, logsPV)

	plan := planWithDestinationCapacityObjects(
		t,
		objects,
		[]string{"data", "logs"},
		[]string{"data=3Gi"},
		false,
	)
	if plan.Ready ||
		!hasFailedCheckContaining(plan, "destination-capacity", "missing source PVC mapping") {
		t.Fatalf("checks=%#v", plan.Checks)
	}

	if len(plan.Volumes) != 2 || plan.TemporaryUsage.StorageRequests != "4Gi" {
		t.Fatalf("failed plan lost volume diagnostics: %#v", plan)
	}
}

func TestPlanSeparatesDestinationUsageFromSourceRollbackRetention(t *testing.T) {
	objects := plannerObjects("2Gi")
	sourceClass := "slow"
	testutil.MustType[*corev1.PersistentVolumeClaim](t, objects[5]).Spec.StorageClassName = &sourceClass
	testutil.MustType[*corev1.PersistentVolume](t, objects[6]).Spec.StorageClassName = sourceClass
	objects = append(objects, &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: sourceClass},
		Provisioner: "source.example.io",
	})

	plan := planWithDestinationCapacityObjects(t, objects, []string{"data"}, []string{"3Gi"}, false)
	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}

	if plan.TemporaryUsage.ByStorageClass["fast"] != "3Gi" ||
		plan.TemporaryUsage.PVCsByStorageClass["fast"] != 1 {
		t.Fatalf("temporary usage=%#v", plan.TemporaryUsage)
	}

	if plan.RollbackRetention.ByStorageClass[sourceClass] != "2Gi" ||
		plan.RollbackRetention.PVCsByStorageClass[sourceClass] != 1 {
		t.Fatalf("rollback retention=%#v", plan.RollbackRetention)
	}

	if _, exists := plan.RollbackRetention.ByStorageClass["fast"]; exists {
		t.Fatalf("rollback retention used destination class: %#v", plan.RollbackRetention)
	}
}

func TestResolveDestinationCapacitiesBroadcastsOrMatchesExplicitPVCNames(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []string
		pvcs   []string
		want   []string
	}{
		{name: "defaults", pvcs: []string{"data", "logs"}, want: []string{"", ""}},
		{name: "broadcast", values: []string{" 3Gi "}, pvcs: []string{"data", "logs"}, want: []string{"3Gi", "3Gi"}},
		{name: "named", values: []string{"logs=4Gi", "data=3Gi"}, pvcs: []string{"data", "logs"}, want: []string{"3Gi", "4Gi"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveDestinationCapacities(test.values, test.pvcs)
			if err != nil || strings.Join(got, ",") != strings.Join(test.want, ",") {
				t.Fatalf("got=%v error=%v want=%v", got, err, test.want)
			}
		})
	}

	if _, err := resolveDestinationCapacities(
		[]string{"1Gi", "2Gi"},
		[]string{"data", "logs"},
	); err == nil ||
		!strings.Contains(err.Error(), "pvc-name=capacity") {
		t.Fatalf("mismatch error=%v", err)
	}
}

func TestResolveDestinationPVCsRequiresCompleteExplicitMappings(t *testing.T) {
	got, err := resolveDestinationPVCs(
		[]string{"logs=logs-new", "data=data-new"},
		[]string{"data", "logs"},
	)
	if err != nil || strings.Join(got, ",") != "data-new,logs-new" {
		t.Fatalf("got=%v error=%v", got, err)
	}

	for _, values := range [][]string{
		{"data=data-new"},
		{"unknown=logs-new", "data=data-new"},
		{"data=data-new", "data=data-other"},
	} {
		if got, err := resolveDestinationPVCs(
			values,
			[]string{"data", "logs"},
		); err == nil ||
			got != nil {
			t.Fatalf("expected mapping error for %v: values=%v error=%v", values, got, err)
		}
	}
}

func planCheckMessage(plan *domain.MigrationPlan, name string) string {
	for _, check := range plan.Checks {
		if check.Name == name {
			return check.Message
		}
	}

	return ""
}
