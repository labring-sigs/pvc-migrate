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

// RenameOptions describes the shared identity planning test matrix.
type RenameOptions struct {
	Operation            domain.Operation
	SessionID            string
	SourceNamespace      string
	SourcePVC            string
	DestinationNamespace string
	DestinationPVC       string
	SessionNamespace     string
}

func (p *Planner) planIdentityForTest(
	ctx context.Context,
	options RenameOptions,
) (*identityTestPlan, error) {
	if options.SourceNamespace == "" {
		options.SourceNamespace = "default"
	}

	if options.SessionNamespace == "" {
		options.SessionNamespace = "system"
	}

	if options.Operation == domain.OperationMove {
		object := &v1alpha1.Move{
			ObjectMeta: metav1.ObjectMeta{Name: options.SessionID},
			Spec: v1alpha1.MoveSpec{
				SourceNamespace:      v1alpha1.NamespaceName(options.SourceNamespace),
				DestinationNamespace: v1alpha1.NamespaceName(options.DestinationNamespace),
				SessionNamespace:     v1alpha1.NamespaceName(options.SessionNamespace),
				SourcePVC:            v1alpha1.LocalResourceReference{Name: options.SourcePVC},
				DestinationPVC: &v1alpha1.LocalResourceReference{
					Name: options.DestinationPVC,
				},
			},
		}

		if options.DestinationPVC == "" {
			object.Spec.DestinationPVC = nil
		}

		plan, err := p.PlanMove(ctx, object, options.SessionNamespace)
		if err != nil {
			return nil, err
		}

		return &identityTestPlan{
			PVCIdentityReport: *plan,
			move:              object,
		}, nil
	}

	object := &v1alpha1.Rename{
		ObjectMeta: metav1.ObjectMeta{Name: options.SessionID, Namespace: options.SourceNamespace},
		Spec: v1alpha1.RenameSpec{
			SourcePVC:      v1alpha1.LocalResourceReference{Name: options.SourcePVC},
			DestinationPVC: v1alpha1.LocalResourceReference{Name: options.DestinationPVC},
		},
	}

	plan, err := p.PlanRename(ctx, object, options.SessionNamespace)
	if err != nil {
		return nil, err
	}

	return &identityTestPlan{PVCIdentityReport: *plan, rename: object}, nil
}

type identityTestPlan struct {
	domain.PVCIdentityReport
	move   *v1alpha1.Move
	rename *v1alpha1.Rename
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
			plan, err := planner.planIdentityForTest(context.Background(), tt.options)
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

	plan, err := New(
		plannerClient(objects...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
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
		plan.rename.Status.Plan.SourceTemplate.Spec.Resources.Requests.Storage().String() != "2Gi" {
		t.Fatalf(
			"rename capacities=%#v session=%#v",
			plan.Volumes[0],
			plan.rename.Status.Plan,
		)
	}

	metadata := plan.rename.Status.Plan.SourceTemplate.Metadata
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

	plan, err := New(client, nil).planIdentityForTest(context.Background(), RenameOptions{
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

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "source-storage-class",
	) ||
		!hasFailedCheckContaining(
			plan.Checks, "source-storage-class", "storage class access denied",
		) {
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

		plan, err := New(plannerClient(objects...), nil).planIdentityForTest(
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
			!hasFailedCheckContaining(
				plan.Checks, "resource-quota", "app/session-objects",
			) {
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

		plan, err := New(plannerClient(objects...), nil).planIdentityForTest(
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
			!hasFailedCheckContaining(
				plan.Checks, "resource-quota", "system/session-objects",
			) ||
			hasFailedCheckContaining(
				plan.Checks, "resource-quota", "app/destination-objects",
			) {
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

	plan, err := New(
		plannerClient(objects...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
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

	plan, err := New(
		plannerClient(objects...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
		SessionID:        "rename-finalizer",
		SourceNamespace:  "app",
		SourcePVC:        "data",
		DestinationPVC:   "renamed",
		SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "pvc-finalizers",
	) {
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

	plan, err := New(
		plannerClient(objects...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
		SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationPVC: "renamed",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "pvc-consumers",
	) {
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
			).planIdentityForTest(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready || !hasFailedCheck(
				plan.Checks, "pvc-consumers",
			) {
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

	plan, err := New(
		plannerClient(objects...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
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

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "pvc-ownership",
	) {
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
	).planIdentityForTest(context.Background(), RenameOptions{
		SessionID:        "rename",
		SourceNamespace:  "app",
		SourcePVC:        "data",
		DestinationPVC:   "renamed",
		SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || plan.SourceNamespace != "app" || plan.DestinationNamespace != "app" ||
		plan.Volumes[0].DestinationPVC.Namespace != "app" {
		t.Fatalf("rename must retain the source namespace: %#v", plan)
	}
}

func TestPlanMoveDefaultsDestinationNameAndRecordsMoveOperation(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "archive"}})

	plan, err := New(
		plannerClient(objects...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
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

	if plan.Kind != "MovePlan" || plan.move.Status.Plan == nil ||
		plan.Volumes[0].DestinationPVC.Name != "data" ||
		plan.Volumes[0].DestinationPVC.Namespace != "archive" {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestPlanMoveAllowsSameNamespaceWithDifferentIdentity(t *testing.T) {
	object := &v1alpha1.Move{
		ObjectMeta: metav1.ObjectMeta{Name: "move"},
		Spec: v1alpha1.MoveSpec{
			SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "system",
			SourcePVC:      v1alpha1.LocalResourceReference{Name: "data"},
			DestinationPVC: &v1alpha1.LocalResourceReference{Name: "renamed"},
		},
	}

	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).PlanMove(t.Context(), object, "system")
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || object.Status.Plan == nil {
		t.Fatalf("same-namespace Move plan=%#v", plan)
	}

	if plan.TemporaryUsage.StorageRequests != "0" || plan.TemporaryUsage.PVCs != 0 {
		t.Fatalf("same-namespace Move temporary usage=%#v", plan.TemporaryUsage)
	}
}

func TestPlanMoveRequiresExistingDestinationNamespace(t *testing.T) {
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
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

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "destination-namespace",
	) {
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

	plan, err := New(
		plannerClient(objects...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
		SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationPVC: "renamed",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "pvc-ownership",
	) {
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

	plan, err := New(
		plannerClient(objects...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
		SessionID: "rename", SourceNamespace: "app", SourcePVC: "data", DestinationPVC: "renamed",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "destination-pvc",
	) {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanRenameRejectsSourcePVClaimRefDrift(t *testing.T) {
	objects := plannerObjects("2Gi")
	testutil.MustType[*corev1.PersistentVolume](t, objects[6]).Spec.ClaimRef.Name = "other"

	plan, err := New(
		plannerClient(objects...),
		nil,
	).planIdentityForTest(context.Background(), RenameOptions{
		SessionID:        "rename-binding-drift",
		SourceNamespace:  "app",
		SourcePVC:        "data",
		DestinationPVC:   "renamed",
		SessionNamespace: "system",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(
		plan.Checks, "source-binding",
	) {
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

	plan, err := New(client, nil).planIdentityForTest(context.Background(), RenameOptions{
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
		!hasFailedCheckContaining(
			plan.Checks, "rbac", "delete app/persistentvolumeclaims",
		) {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}
