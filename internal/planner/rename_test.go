package planner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	coretyped "k8s.io/client-go/kubernetes/typed/core/v1"
	clienttesting "k8s.io/client-go/testing"
)

// RenameOptions and PlanRename are test-only adapters retained while the
// shared identity planning matrix exercises both operations. Production code
// exposes only PlanRenamePVC and PlanMovePVC, each with a fixed operation.
type RenameOptions struct {
	Operation            domain.Operation
	SessionID            string
	SourceNamespace      string
	SourcePVC            string
	DestinationNamespace string
	DestinationPVC       string
	SessionNamespace     string
}

func (p *Planner) PlanRename(
	ctx context.Context,
	options RenameOptions,
) (*domain.MigrationPlan, error) {
	if options.Operation == domain.OperationMove {
		return p.PlanMovePVC(ctx, MovePlanOptions{
			SessionID: options.SessionID, SourceNamespace: options.SourceNamespace,
			SourcePVC: options.SourcePVC, DestinationNamespace: options.DestinationNamespace,
			DestinationPVC: options.DestinationPVC, SessionNamespace: options.SessionNamespace,
		})
	}

	return p.planPVCIdentity(ctx, pvcIdentityPlanOptions{
		Operation: domain.OperationRename, SessionID: options.SessionID,
		SourceNamespace: options.SourceNamespace, SourcePVC: options.SourcePVC,
		DestinationNamespace: options.DestinationNamespace, DestinationPVC: options.DestinationPVC,
		SessionNamespace: options.SessionNamespace,
	})
}

func TestPlanRenameValidatesRequiredAndDistinctIdentities(t *testing.T) {
	planner := New(plannerClient(), nil)

	tests := []struct {
		name    string
		options RenameOptions
		want    string
	}{
		{
			name:    "missing names",
			options: RenameOptions{SessionID: "rename"},
			want:    "source and destination PVC names are required",
		},
		{
			name: "same identity",
			options: RenameOptions{
				SessionID:            "rename",
				SourceNamespace:      "app",
				SourcePVC:            "data",
				DestinationNamespace: "app",
				DestinationPVC:       "data",
			},
			want: "identities must differ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planner.PlanRename(context.Background(), tt.options)
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready || len(plan.Checks) != 1 ||
				!strings.Contains(plan.Checks[0].Message, tt.want) {
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
			"volume.kubernetes.io/storage-resizer":             "drop",
			"kubectl.kubernetes.io/last-applied-configuration": "drop",
		}
	}

	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		SessionID:            "rename",
		SourceNamespace:      "app",
		SourcePVC:            "data",
		DestinationNamespace: "app",
		DestinationPVC:       "renamed",
		SessionNamespace:     "system",
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

	if plan.Volumes[0].SourceCapacity != "2Gi" || plan.Volumes[0].Capacity != "2Gi" ||
		plan.SessionSpec.Volumes[0].SourceCapacity != "2Gi" {
		t.Fatalf("rename capacities=%#v session=%#v", plan.Volumes[0], plan.SessionSpec.Volumes[0])
	}

	metadata := plan.SessionSpec.Volumes[0].SourcePVCMetadata
	if metadata.Labels["application"] != "database" ||
		metadata.Annotations["application.example/setting"] != "keep" {
		t.Fatalf("preserved metadata=%#v", metadata)
	}

	for _, key := range []string{"volume.kubernetes.io/selected-node", "pv.kubernetes.io/bind-completed", "volume.kubernetes.io/storage-resizer", "kubectl.kubernetes.io/last-applied-configuration", kube.SessionKey} {
		if _, exists := metadata.Annotations[key]; exists {
			t.Fatalf("transient annotation %q was preserved", key)
		}
	}
}

func TestPlanRenameFailsWhenSourceStorageClassCannotBeRead(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	client.PrependReactor(
		"get",
		"storageclasses",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("storage class access denied")
		},
	)

	plan, err := New(client, nil).PlanRename(context.Background(), RenameOptions{
		SessionID:            "rename-storage-class-error",
		SourceNamespace:      "app",
		SourcePVC:            "data",
		DestinationNamespace: "app",
		DestinationPVC:       "renamed",
		SessionNamespace:     "system",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "source-storage-class") ||
		!hasFailedCheckContaining(plan, "source-storage-class", "storage class access denied") {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestPlanRenameAccountsForSessionObjectsInTheirNamespace(t *testing.T) {
	t.Run("destination namespace", func(t *testing.T) {
		objects := append(plannerObjects("2Gi"), &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "session-objects"},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceConfigMaps:                               resource.MustParse("0"),
				corev1.ResourceName("count/leases.coordination.k8s.io"): resource.MustParse("0"),
			}},
		})

		plan, err := New(plannerClient(objects...), nil).PlanRename(
			context.Background(),
			RenameOptions{
				SessionID:            "rename-session-quota",
				SourceNamespace:      "app",
				SourcePVC:            "data",
				DestinationNamespace: "app",
				DestinationPVC:       "renamed",
				SessionNamespace:     "app",
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		if plan.TemporaryUsage.ConfigMaps != 1 || plan.TemporaryUsage.Leases != 1 ||
			!hasFailedCheckContaining(plan, "resource-quota", "app/session-objects") {
			t.Fatalf("plan=%#v", plan)
		}
	})

	t.Run("separate session namespace", func(t *testing.T) {
		objects := append(
			plannerObjects("2Gi"),
			&corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "destination-objects"},
				Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
					corev1.ResourceConfigMaps: resource.MustParse("0"),
				}},
			},
			&corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "session-objects"},
				Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
					corev1.ResourceName("count/leases.coordination.k8s.io"): resource.MustParse(
						"0",
					),
				}},
			},
		)

		plan, err := New(plannerClient(objects...), nil).PlanRename(
			context.Background(),
			RenameOptions{
				SessionID:            "rename-split-session-quota",
				SourceNamespace:      "app",
				SourcePVC:            "data",
				DestinationNamespace: "app",
				DestinationPVC:       "renamed",
				SessionNamespace:     "system",
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		if plan.TemporaryUsage.ConfigMaps != 0 || plan.TemporaryUsage.Leases != 0 ||
			!hasFailedCheckContaining(plan, "resource-quota", "system/session-objects") ||
			hasFailedCheckContaining(plan, "resource-quota", "app/destination-objects") {
			t.Fatalf("plan=%#v", plan)
		}
	})
}

func TestPlanRenameDoesNotApplyToolPodLimitRange(t *testing.T) {
	objects := append(plannerObjects("2Gi"), &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "tool-pod-minimum"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Min:  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m")},
		}}},
	})

	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		SessionID:            "rename-no-tool-pod",
		SourceNamespace:      "app",
		SourcePVC:            "data",
		DestinationNamespace: "app",
		DestinationPVC:       "renamed",
		SessionNamespace:     "system",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanRenameRejectsCustomPVCFinalizer(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok {
			pvc.Finalizers = []string{kube.PVCProtectionFinalizer, "storage.example/protect"}
		}
	}

	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		SessionID:        "rename-finalizer",
		SourceNamespace:  "app",
		SourcePVC:        "data",
		DestinationPVC:   "renamed",
		SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "pvc-finalizers") {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestPlanRenameRequiresOfflinePVC(t *testing.T) {
	objects := append(plannerObjects("2Gi"), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "consumer"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
			}}},
		},
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

func TestPlanRenameFailsOnEmptyPodList(t *testing.T) {
	base := plannerClient(plannerObjects("2Gi")...)
	for _, operation := range []domain.Operation{domain.OperationRename, domain.OperationMove} {
		t.Run(string(operation), func(t *testing.T) {
			options := RenameOptions{
				Operation:            operation,
				SessionID:            "empty-pods-" + strings.ToLower(string(operation)),
				SourceNamespace:      "app",
				SourcePVC:            "data",
				DestinationNamespace: "app",
				DestinationPVC:       "renamed",
				SessionNamespace:     "system",
			}
			if operation == domain.OperationMove {
				options.DestinationNamespace = "system"
			}

			plan, err := New(
				&nilPodListClient{Interface: base},
				nil,
			).PlanRename(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready || !hasFailedCheck(plan, "pvc-consumers") {
				t.Fatalf("empty PodList must fail closed: checks=%#v", plan.Checks)
			}
		})
	}
}

type nilPodListClient struct {
	kubernetes.Interface
}

func (c *nilPodListClient) CoreV1() coretyped.CoreV1Interface {
	return &nilPodListCore{CoreV1Interface: c.Interface.CoreV1()}
}

type nilPodListCore struct {
	coretyped.CoreV1Interface
}

func (c *nilPodListCore) Pods(namespace string) coretyped.PodInterface {
	return &nilPodListPods{PodInterface: c.CoreV1Interface.Pods(namespace)}
}

type nilPodListPods struct {
	coretyped.PodInterface
}

func (c *nilPodListPods) List(context.Context, metav1.ListOptions) (*corev1.PodList, error) {
	return nil, nil
}

func TestPlanMoveCrossNamespaceRejectsOwnersAndAccountsForStorage(t *testing.T) {
	objects := plannerObjects("2Gi")

	objects = append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "archive"}})
	for _, object := range objects {
		if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok {
			pvc.OwnerReferences = []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: types.UID("sts-uid")},
			}
		}
	}

	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		Operation:            domain.OperationMove,
		SessionID:            "move",
		SourceNamespace:      "app",
		SourcePVC:            "data",
		DestinationNamespace: "archive",
		SessionNamespace:     "system",
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
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).PlanRename(context.Background(), RenameOptions{
		SessionID:            "rename",
		SourceNamespace:      "app",
		SourcePVC:            "data",
		DestinationNamespace: "archive",
		DestinationPVC:       "renamed",
		SessionNamespace:     "system",
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
		Operation:            domain.OperationMove,
		SessionID:            "move",
		SourceNamespace:      "app",
		SourcePVC:            "data",
		DestinationNamespace: "archive",
		SessionNamespace:     "system",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}

	if plan.Kind != "MovePlan" || plan.SessionSpec.Operation() != domain.OperationMove ||
		plan.Volumes[0].DestinationPVC.Name != "data" ||
		plan.Volumes[0].DestinationPVC.Namespace != "archive" {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestPlanMoveRequiresExistingDestinationNamespace(t *testing.T) {
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).PlanRename(context.Background(), RenameOptions{
		Operation:            domain.OperationMove,
		SessionID:            "move",
		SourceNamespace:      "app",
		SourcePVC:            "data",
		DestinationNamespace: "missing",
		SessionNamespace:     "system",
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
			pvc.OwnerReferences = []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: types.UID("sts-uid")},
			}
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
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "renamed",
			UID:       types.UID("existing-uid"),
		},
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

func TestPlanRenameRejectsSourcePVClaimRefDrift(t *testing.T) {
	objects := plannerObjects("2Gi")
	testutil.MustType[*corev1.PersistentVolume](t, objects[6]).Spec.ClaimRef.Name = "other"

	plan, err := New(plannerClient(objects...), nil).PlanRename(context.Background(), RenameOptions{
		SessionID:        "rename-binding-drift",
		SourceNamespace:  "app",
		SourcePVC:        "data",
		DestinationPVC:   "renamed",
		SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "source-binding") {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestPlanRenameChecksMutationRBAC(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
				t,
				action,
			).DeepCopy()
			if review.Spec.ResourceAttributes.Verb == "delete" &&
				review.Spec.ResourceAttributes.Resource == "persistentvolumeclaims" {
				review.Status.Allowed = false
				review.Status.Reason = "PVC delete denied"
				return true, review, nil
			}

			review.Status.Allowed = true

			return true, review, nil
		},
	)

	plan, err := New(client, nil).PlanRename(context.Background(), RenameOptions{
		SessionID:        "rename",
		SourceNamespace:  "app",
		SourcePVC:        "data",
		DestinationPVC:   "renamed",
		SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready ||
		!hasFailedCheckContaining(plan, "rbac", "delete app/persistentvolumeclaims") {
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
