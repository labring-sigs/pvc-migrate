package planner

import (
	"context"
	"errors"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func planWithDestinationCapacity(
	t *testing.T,
	capacity string,
	allowShrink bool,
) *domain.TransferPlan {
	t.Helper()

	return planWithDestinationCapacityObjects(
		t,
		plannerObjects("2Gi"),
		[]string{"data"},
		capacity,
		allowShrink,
	)
}

func planWithDestinationCapacityObjects(
	t *testing.T,
	objects []runtime.Object,
	sourcePVCs []string,
	capacity string,
	allowShrink bool,
) *domain.TransferPlan {
	t.Helper()

	object := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "capacity-test"},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace: "app", DestinationNamespace: "system", SessionNamespace: "system",
			CopySpec: v1alpha1.CopySpec{
				Volumes: testSourceVolumes(sourcePVCs...),
				TransferOptions: v1alpha1.TransferOptions{
					TargetNode: "node-b", DestinationStorageClass: "fast",
					DestinationCapacity: capacity, AllowVolumeShrink: allowShrink,
				},
			},
		},
	}

	plan, err := New(
		plannerClient(objects...),
		nil,
	).WithVolumeUsageReader(staticUsageReader{bytes: 1024}).
		PlanCopy(t.Context(), object, "")
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready {
		if object.Status.Plan == nil || len(object.Status.Plan.Volumes) != len(plan.Volumes) {
			t.Fatalf("execution plan missing volumes: %+v", object.Status.Plan)
		}

		for i, volume := range object.Status.Plan.Volumes {
			if volume.SourceCapacity != plan.Volumes[i].SourceCapacity ||
				volume.Capacity != plan.Volumes[i].Capacity {
				t.Fatalf("execution capacity differs from report: %+v", volume)
			}
		}
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
	plan := planWithDestinationCapacity(t, "3Gi", false)
	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}

	if len(plan.Volumes) != 1 || plan.Volumes[0].SourceCapacity != "2Gi" ||
		plan.Volumes[0].Capacity != "3Gi" {
		t.Fatalf("planned volumes=%#v", plan.Volumes)
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

func TestPlanInitialLargerDestinationDoesNotRequireVolumeExpansion(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		if storageClass, ok := object.(*storagev1.StorageClass); ok {
			storageClass.AllowVolumeExpansion = new(false)
		}
	}

	plan := planWithDestinationCapacityObjects(
		t,
		objects,
		[]string{"data"},
		"3Gi",
		false,
	)
	if !plan.Ready || len(plan.Volumes) != 1 || plan.Volumes[0].Capacity != "3Gi" {
		t.Fatalf("initial provisioning was treated as expansion: %#v", plan)
	}
}

func TestPlanRejectsIncompleteSourceVolumeExpansion(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		switch typed := object.(type) {
		case *corev1.PersistentVolumeClaim:
			typed.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("3Gi")
		case *storagev1.StorageClass:
			typed.AllowVolumeExpansion = new(true)
		}
	}

	plan := planWithDestinationCapacityObjects(
		t,
		objects,
		[]string{"data"},
		"",
		false,
	)
	if plan.Ready || !hasFailedCheckContaining(
		plan.Checks,
		domain.CheckNameCapacity,
		"volume expansion is incomplete",
	) {
		t.Fatalf("incomplete source expansion plan=%#v", plan)
	}
}

func TestPlanDefaultsDestinationCapacityToSourcePVCapacity(t *testing.T) {
	plan := planWithDestinationCapacity(t, "", false)
	if !plan.Ready || len(plan.Volumes) != 1 {
		t.Fatalf("plan=%#v", plan)
	}

	if plan.Volumes[0].SourceCapacity != "2Gi" || plan.Volumes[0].Capacity != "2Gi" {
		t.Fatalf("planned volume=%#v", plan.Volumes[0])
	}
}

func TestPlanRejectsDestinationShrinkWithoutExplicitApproval(t *testing.T) {
	plan := planWithDestinationCapacity(t, "1Gi", false)
	if plan.Ready || !hasFailedCheck(
		plan.Checks, "destination-capacity",
	) {
		t.Fatalf("plan=%#v", plan)
	}

	if !strings.Contains(planCheckMessage(plan, "destination-capacity"), "--allow-volume-shrink") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanAllowsExplicitDestinationShrinkWithWarning(t *testing.T) {
	plan := planWithDestinationCapacity(t, "1Gi", true)
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
		plan(context.Background(), planOptions{
			operationKind: domain.OperationCopy,
			Volumes:       testSourceVolumes("data"),
			SessionID:     "capacity-overflow",

			SourceNamespace:      "app",
			TemporaryNamespace:   "system",
			DestinationNamespace: "system",
			StagingNamespace:     "system",
			SessionNamespace:     "system",

			TransferOptions: v1alpha1.TransferOptions{
				TargetNode:              "node-b",
				DestinationStorageClass: "fast",
				DestinationCapacity:     "1Gi",
				AllowVolumeShrink:       true,
			},
		})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "source-usage",
	) {
		t.Fatalf("expected measured overflow failure: %#v", plan.Checks)
	}
}

func TestPlanRequiresExplicitSourceUsageSkip(t *testing.T) {
	base := planOptions{
		operationKind: domain.OperationCopy,
		Volumes:       testSourceVolumes("data"),
		SessionID:     "capacity-unknown",

		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "system",
		StagingNamespace:     "system",
		SessionNamespace:     "system",

		TransferOptions: v1alpha1.TransferOptions{
			TargetNode:              "node-b",
			DestinationStorageClass: "fast",
			DestinationCapacity:     "1Gi",
			AllowVolumeShrink:       true,
		},
	}

	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).WithVolumeUsageReader(errorUsageReader{}).
		plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "source-usage",
	) {
		t.Fatalf("expected unknown usage failure: %#v", plan.Checks)
	}

	base.SkipSourceUsageCheck = true

	plan, err = New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).WithVolumeUsageReader(errorUsageReader{}).
		plan(context.Background(), base)
	if err != nil || !plan.Ready {
		t.Fatalf("expected explicit source-usage skip: err=%v checks=%#v", err, plan.Checks)
	}
}

func TestPlanRequiresTrustedReaderByDefault(t *testing.T) {
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).plan(context.Background(), planOptions{
		operationKind: domain.OperationCopy,
		Volumes:       testSourceVolumes("data"),
		SessionID:     "capacity-no-reader",

		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "system",
		StagingNamespace:     "system",
		SessionNamespace:     "system",

		TransferOptions: v1alpha1.TransferOptions{
			TargetNode:              "node-b",
			DestinationStorageClass: "fast",
			DestinationCapacity:     "1Gi",
			AllowVolumeShrink:       true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "source-usage",
	) {
		t.Fatalf("expected missing trusted reader failure: %#v", plan.Checks)
	}
}

func TestPlanKeepsCompleteDiagnosticsForInvalidCapacity(t *testing.T) {
	plan := planWithDestinationCapacity(t, "invalid", false)
	if plan.Ready || !hasFailedCheck(
		plan.Checks, "destination-capacity",
	) {
		t.Fatalf("plan=%#v", plan)
	}

	if len(plan.Volumes) != 1 || plan.Volumes[0].Capacity != "2Gi" ||
		plan.TemporaryUsage.StorageRequests != "2Gi" {
		t.Fatalf("failed plan lost source-sized diagnostics: %#v", plan)
	}
}

func TestPlanPreservesPartialCapacityOverrides(t *testing.T) {
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

	plan, err := New(plannerClient(objects...), nil).PlanCopy(t.Context(), &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "partial-capacity"},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace: "app", DestinationNamespace: "system", SessionNamespace: "system",
			CopySpec: v1alpha1.CopySpec{
				TransferOptions: v1alpha1.TransferOptions{
					TargetNode:              "node-b",
					DestinationStorageClass: "fast",
				},
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}, Capacity: "3Gi"},
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "logs"}},
				},
			},
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}

	if len(plan.Volumes) != 2 || plan.TemporaryUsage.StorageRequests != "5Gi" ||
		plan.Volumes[0].Capacity != "3Gi" || plan.Volumes[1].Capacity != "2Gi" {
		t.Fatalf("partial override lost volume defaults: %#v", plan)
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

	plan := planWithDestinationCapacityObjects(t, objects, []string{"data"}, "3Gi", false)
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

func planCheckMessage(plan *domain.TransferPlan, name domain.CheckName) string {
	for _, check := range plan.Checks {
		if check.Name == name {
			return check.Message
		}
	}

	return ""
}
