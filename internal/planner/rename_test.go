package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
)

func TestPlanRenameValidatesRequiredAndDistinctIdentities(t *testing.T) {
	planner := New(plannerClient(), nil)
	tests := []struct {
		name    string
		options RenameOptions
		want    string
	}{
		{name: "missing names", options: RenameOptions{SessionID: "rename"}, want: "source and destination PVC names are required"},
		{name: "same identity", options: RenameOptions{SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationNamespace: "app", DestinationPVC: "data"}, want: "identities must differ"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planner.PlanRename(context.Background(), tt.options)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Ready || len(plan.Checks) != 1 || !strings.Contains(plan.Checks[0].Message, tt.want) {
				t.Fatalf("plan=%#v", plan)
			}
		})
	}
}

func TestPlanRenameSameNamespacePreservesDurableMetadataWithoutQuotaDemand(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		pvc, ok := object.(*corev1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		pvc.Labels = map[string]string{"application": "database"}
		pvc.Annotations = map[string]string{
			"application.example/setting":                      "keep",
			"volume.kubernetes.io/selected-node":               "drop",
			"pv.kubernetes.io/bind-completed":                  "drop",
			"kubectl.kubernetes.io/last-applied-configuration": "drop",
			kube.SessionAnnotation:                             "drop",
		}
	}
	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationNamespace: "app", DestinationPVC: "renamed", SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}
	if plan.TemporaryUsage.StorageRequests != "0" || plan.TemporaryUsage.PVCs != 0 {
		t.Fatalf("temporary usage=%#v", plan.TemporaryUsage)
	}
	metadata := plan.SessionSpec.Volumes[0].SourcePVCMetadata
	if metadata.Labels["application"] != "database" || metadata.Annotations["application.example/setting"] != "keep" {
		t.Fatalf("preserved metadata=%#v", metadata)
	}
	for _, key := range []string{"volume.kubernetes.io/selected-node", "pv.kubernetes.io/bind-completed", "kubectl.kubernetes.io/last-applied-configuration", kube.SessionAnnotation} {
		if _, exists := metadata.Annotations[key]; exists {
			t.Fatalf("transient annotation %q was preserved", key)
		}
	}
}

func TestPlanRenameRequiresOfflinePVC(t *testing.T) {
	objects := append(plannerObjects("2Gi"), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "consumer"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
		}}}},
	})
	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationPVC: "renamed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready || !hasFailedCheck(plan, "rename-offline") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanMoveCrossNamespaceRejectsOwnersAndAccountsForStorage(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "archive"}})
	for _, object := range objects {
		if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok {
			pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: types.UID("sts-uid")}}
		}
	}
	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		Operation: domain.OperationMove, SessionID: "move", SourceNamespace: "app", SourcePVC: "data", DestinationNamespace: "archive", SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready || !hasFailedCheck(plan, "pvc-ownership") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
	if plan.TemporaryUsage.StorageRequests != "2Gi" || plan.TemporaryUsage.PVCs != 1 {
		t.Fatalf("temporary usage=%#v", plan.TemporaryUsage)
	}
}

func TestPlanRenameStaysInSourceNamespace(t *testing.T) {
	plan, err := New(plannerClient(plannerObjects("2Gi")...), nil).PlanRename(context.Background(), RenameOptions{
		SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationNamespace: "archive", DestinationPVC: "renamed", SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready || !hasFailedCheck(plan, "rename") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanMoveDefaultsDestinationNameAndRecordsMoveOperation(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "archive"}})
	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		Operation: domain.OperationMove, SessionID: "move", SourceNamespace: "app", SourcePVC: "data", DestinationNamespace: "archive", SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}
	if plan.Kind != "MovePlan" || plan.SessionSpec.Operation() != domain.OperationMove || plan.Volumes[0].DestinationPVC.Name != "data" || plan.Volumes[0].DestinationPVC.Namespace != "archive" {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestPlanMoveRequiresExistingDestinationNamespace(t *testing.T) {
	plan, err := New(plannerClient(plannerObjects("2Gi")...), nil).PlanRename(context.Background(), RenameOptions{
		Operation: domain.OperationMove, SessionID: "move", SourceNamespace: "app", SourcePVC: "data", DestinationNamespace: "missing", SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready || !hasFailedCheck(plan, "destination-namespace") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanRenameSameNamespaceRejectsControllerOwnedPVC(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok {
			pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: types.UID("sts-uid")}}
		}
	}
	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationPVC: "renamed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready || !hasFailedCheck(plan, "pvc-ownership") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanRenameRejectsExistingDestination(t *testing.T) {
	objects := append(plannerObjects("2Gi"), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "renamed", UID: types.UID("existing-uid")},
	})
	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationPVC: "renamed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready || !hasFailedCheck(plan, "destination-pvc") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanRenameChecksMutationRBAC(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
		review := action.(clienttesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).DeepCopy()
		if review.Spec.ResourceAttributes.Verb == "delete" && review.Spec.ResourceAttributes.Resource == "persistentvolumeclaims" {
			review.Status.Allowed = false
			review.Status.Reason = "PVC delete denied"
			return true, review, nil
		}
		review.Status.Allowed = true
		return true, review, nil
	})
	plan, err := New(client, nil).PlanRename(context.Background(), RenameOptions{
		SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationPVC: "renamed", SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready || !hasFailedCheck(plan, "rbac") || !strings.Contains(plan.Checks[len(plan.Checks)-1].Message, "delete app/persistentvolumeclaims") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func hasFailedCheck(plan *domain.MigrationPlan, name string) bool {
	for _, check := range plan.Checks {
		if check.Name == name && !check.Passed {
			return true
		}
	}
	return false
}
