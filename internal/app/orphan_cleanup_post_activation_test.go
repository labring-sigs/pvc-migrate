package app

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func orphanPVC(
	name, namespace, sessionID, rollbackPV, boundPV string,
) *corev1.PersistentVolumeClaim {
	annotations := map[string]string{kube.SessionKey: sessionID}
	if rollbackPV != "" {
		annotations[kube.RollbackPVAnnotation] = rollbackPV
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			UID:         types.UID("pvc-uid"),
			Annotations: annotations,
			Labels: map[string]string{
				kube.SessionKey:     sessionID,
				kube.ManagedByLabel: kube.ManagedByValue,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: boundPV},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	return pvc
}

func orphanPV(name, paired string) *corev1.PersistentVolume {
	annotations := map[string]string{
		kube.SessionKey:               "orphan-session",
		kube.ManagedByLabel:           kube.ManagedByValue,
		kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
	}
	if paired != "" {
		annotations[kube.PairedPVAnnotation] = paired
	}

	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			UID:         types.UID(name + "-uid"),
			Annotations: annotations,
			Labels: map[string]string{
				kube.SessionKey:        "orphan-session",
				kube.ResourceRoleLabel: kube.ResourceRoleActive,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef: &corev1.ObjectReference{
				Namespace: "tenants",
				Name:      "data",
				UID:       types.UID("pvc-uid"),
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
}

func orphanRollbackPV(name, paired string) *corev1.PersistentVolume {
	pv := orphanPV(name, paired)
	pv.Labels[kube.ResourceRoleLabel] = kube.ResourceRoleRollback
	pv.Status.Phase = corev1.VolumeReleased

	return pv
}

// TestPlanOrphanCleanupPostActivation pins the direction decision of the most
// destructive admin recovery: after a cutover whose session record was lost,
// the plan must name the PVC's currently bound PV as active and the recorded
// partner as the rollback volume.
func TestPlanOrphanCleanupPostActivation(t *testing.T) {
	pvc := orphanPVC("data", "tenants", "orphan-session", "rollback-pv", "active-pv")
	active := orphanPV("active-pv", "rollback-pv")
	rollback := orphanRollbackPV("rollback-pv", "active-pv")

	client := fake.NewClientset(pvc, active, rollback)
	cleaner := NewOrphanCleaner(client, nil, nil, orphanNoRecordsFinder{}, nil)

	plan, err := cleaner.PlanOrphanCleanup(context.Background(), OrphanCleanupOptions{
		SessionID:        "orphan-session",
		SessionNamespace: "sessions",
		SourceNamespace:  "tenants",
		SourcePVC:        "data",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Mode != domain.OrphanCleanupPostActivation {
		t.Fatalf("mode=%s, want PostActivation", plan.Mode)
	}

	if plan.PostActivation == nil ||
		plan.PostActivation.ActivePV.Name != "active-pv" ||
		plan.PostActivation.RollbackPV.Name != "rollback-pv" {
		t.Fatalf("post-activation resources=%+v", plan.PostActivation)
	}

	if !plan.Ready {
		t.Fatalf("plan not ready: %+v", plan.Checks)
	}
}

// TestPlanOrphanCleanupPostActivationRejectsBrokenPairing keeps the guard:
// when the active PV no longer points at the PVC's recorded rollback PV, the
// plan must fail instead of guessing which side to release.
func TestPlanOrphanCleanupPostActivationRejectsBrokenPairing(t *testing.T) {
	pvc := orphanPVC("data", "tenants", "orphan-session", "other-pv", "active-pv")
	active := orphanPV("active-pv", "rollback-pv")
	rollback := orphanPV("other-pv", "")

	client := fake.NewClientset(pvc, active, rollback)
	cleaner := NewOrphanCleaner(client, nil, nil, orphanNoRecordsFinder{}, nil)

	plan, err := cleaner.PlanOrphanCleanup(context.Background(), OrphanCleanupOptions{
		SessionID:       "orphan-session",
		SourceNamespace: "tenants",
		SourcePVC:       "data",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready {
		t.Fatalf("broken pairing must not be ready: %+v", plan.Checks)
	}
}

// TestCleanupOrphanPostActivationDeletesRollbackSide pins the destructive
// direction: cleanup removes the rollback PV and the session Lease while the
// active PVC and its bound PV stay untouched.
func TestCleanupOrphanPostActivationDeletesRollbackSide(t *testing.T) {
	pvc := orphanPVC("data", "tenants", "orphan-session", "rollback-pv", "active-pv")
	active := orphanPV("active-pv", "rollback-pv")
	rollback := orphanRollbackPV("rollback-pv", "active-pv")

	client := fake.NewClientset(pvc, active, rollback)

	var deletedPVs []string
	client.PrependReactor(
		"delete",
		"persistentvolumes",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			deleteAction, ok := action.(clienttesting.DeleteAction)
			if ok {
				deletedPVs = append(deletedPVs, deleteAction.GetName())
			}

			return false, nil, nil
		},
	)

	cleaner := NewOrphanCleaner(
		client,
		&fakeSessionLocker{},
		orphanNoopLeaseCleaner{},
		orphanNoRecordsFinder{},
		nil,
	)

	result, err := cleaner.CleanupOrphan(context.Background(), OrphanCleanupOptions{
		SessionID:        "orphan-session",
		SessionNamespace: "sessions",
		SourceNamespace:  "tenants",
		SourcePVC:        "data",
	})
	if err != nil {
		t.Fatal(err)
	}

	if result != nil && !result.Ready {
		t.Fatalf("cleanup plan not ready: %+v", result.Checks)
	}

	if len(deletedPVs) != 1 || deletedPVs[0] != "rollback-pv" {
		t.Fatalf("deleted PVs=%v, want only rollback-pv", deletedPVs)
	}

	if _, err := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "active-pv", metav1.GetOptions{}); err != nil {
		t.Fatalf("active PV must survive cleanup: %v", err)
	}

	surviving, err := client.CoreV1().
		PersistentVolumeClaims("tenants").
		Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if surviving.Spec.VolumeName != "active-pv" {
		t.Fatalf("active PVC binding changed: %s", surviving.Spec.VolumeName)
	}

	if surviving.Annotations[kube.SessionKey] != "" {
		t.Fatalf("active PVC ownership not released: %+v", surviving.Annotations)
	}
}

type orphanNoRecordsFinder struct{}

func (orphanNoRecordsFinder) Find(context.Context, string, ...string) (*kube.WorkflowOwner, error) {
	return nil, nil
}

type orphanNoopLeaseCleaner struct{}

func (orphanNoopLeaseCleaner) DeleteSessionLease(context.Context, string, string) error {
	return nil
}

var _ = apierrors.NewNotFound
