package app

import (
	"context"
	"errors"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestOrphanCleanupReleasesSessionLeaseOnCallbackFailure(t *testing.T) {
	lock := &fakeSessionLock{}
	cleaner := NewOrphanCleaner(nil, &fakeSessionLocker{lock: lock}, nil, nil, nil)
	cause := errors.New("cleanup failed")

	err := cleaner.withSessionIDLock(
		t.Context(),
		"sessions",
		"orphan",
		func(context.Context) error {
			return cause
		},
	)
	if !errors.Is(err, cause) {
		t.Fatalf("cleanup error = %v, want callback error", err)
	}

	if !lock.released {
		t.Fatal("orphan cleanup did not release its session Lease")
	}
}

func TestOrphanCleanupDestinationPVCStopsAfterFenceLoss(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "destination", Name: "data", UID: "pvc", ResourceVersion: "1",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        "orphan",
			kube.ResourceRoleLabel: kube.ResourceRoleDestination,
		},
		Annotations: map[string]string{
			kube.SessionKey:             "orphan",
			kube.SourcePVCUIDAnnotation: "source-pvc",
		},
	}}
	client := fake.NewClientset(pvc)
	lock := &fakeSessionLock{}
	lost := errors.New("orphan Lease lost after PVC read")
	client.PrependReactor(
		"get",
		"persistentvolumeclaims",
		func(ktesting.Action) (bool, runtime.Object, error) {
			lock.err = lost
			return false, nil, nil
		},
	)

	cleaner := NewOrphanCleaner(client, nil, nil, nil, nil)
	ctx := withHeldSessionLock(t.Context(), heldSessionLock{lock: lock})

	err := cleaner.deleteOrphanDestinationPVC(ctx, "orphan", "source-pvc", kube.PVCReference(pvc))
	if !errors.Is(err, lost) {
		t.Fatalf("cleanup error = %v, want Lease loss", err)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("Lease loss allowed PVC mutation: %s", action.GetVerb())
		}
	}
}

func TestOrphanCleanupPVStopsAfterFenceLoss(t *testing.T) {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "active-pv", UID: "pv", ResourceVersion: "1",
			Labels: map[string]string{
				kube.SessionKey:        "orphan",
				kube.ResourceRoleLabel: kube.ResourceRoleActive,
			},
			Annotations: map[string]string{
				kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimRetain),
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		},
	}
	client := fake.NewClientset(pv)
	lock := &fakeSessionLock{}
	lost := errors.New("orphan Lease lost after PV read")
	client.PrependReactor(
		"get",
		"persistentvolumes",
		func(ktesting.Action) (bool, runtime.Object, error) {
			lock.err = lost
			return false, nil, nil
		},
	)

	cleaner := NewOrphanCleaner(client, nil, nil, nil, nil)
	ctx := withHeldSessionLock(t.Context(), heldSessionLock{lock: lock})

	err := cleaner.finalizeOrphanPV(ctx, "orphan", kube.PVReference(pv), kube.ResourceRoleActive)
	if !errors.Is(err, lost) {
		t.Fatalf("cleanup error = %v, want Lease loss", err)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("Lease loss allowed PV mutation: %s", action.GetVerb())
		}
	}
}
