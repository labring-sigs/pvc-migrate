package app

import (
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestDeleteManagedPVCRechecksFenceAfterIdentityRead(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "destination", Namespace: "app", UID: "pvc", ResourceVersion: "1",
		Labels: map[string]string{kube.SessionKey: "reservation"},
	}}
	client := fake.NewClientset(pvc)
	lost := errors.New("lease lost after read")
	lock := &fakeSessionLock{}
	ctx := withHeldSessionLock(t.Context(), heldSessionLock{lock: lock})
	client.PrependReactor(
		"get",
		"persistentvolumeclaims",
		func(ktesting.Action) (bool, runtime.Object, error) {
			lock.err = lost
			return false, nil, nil
		},
	)

	if err := deleteManagedPVC(
		ctx,
		client,
		"reservation",
		kube.PVCReference(pvc),
	); !errors.Is(
		err,
		lost,
	) {
		t.Fatal(err)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("lost fence allowed resource mutation: %v", action)
		}
	}
}

func TestDeleteReclaimedPVFencesPolicyUpdateAndDeletion(t *testing.T) {
	for _, phase := range []string{"before-policy-update", "before-delete"} {
		t.Run(phase, func(t *testing.T) {
			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "destination-pv",
					UID:             "pv",
					ResourceVersion: "1",
					Labels: map[string]string{
						kube.SessionKey:        "reservation",
						kube.ResourceRoleLabel: kube.ResourceRoleDestination,
					},
					Annotations: map[string]string{
						kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
					},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				},
				Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased},
			}
			client := fake.NewClientset(pv)
			lost := errors.New("lease lost")
			lock := &fakeSessionLock{}
			ctx := withHeldSessionLock(t.Context(), heldSessionLock{lock: lock})
			gets := 0
			client.PrependReactor(
				"get",
				"persistentvolumes",
				func(ktesting.Action) (bool, runtime.Object, error) {
					gets++
					if phase == "before-policy-update" && gets == 2 {
						lock.err = lost
					}

					return false, nil, nil
				},
			)
			client.PrependReactor(
				"update",
				"persistentvolumes",
				func(ktesting.Action) (bool, runtime.Object, error) {
					if phase == "before-delete" {
						lock.err = lost
					}
					return false, nil, nil
				},
			)

			err := deleteReclaimedPV(
				ctx,
				client,
				nil,
				"reservation",
				v1alpha1.ObjectReference{
					Name: pv.Name,
					UID:  pv.UID,
				},
				kube.ResourceRoleDestination,
				corev1.PersistentVolumeReclaimDelete,
				nil,
			)
			if !errors.Is(err, lost) {
				t.Fatal(err)
			}

			updates := 0
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" {
					t.Fatal("PV deleted after fence loss")
				}

				if action.GetVerb() == "update" {
					updates++
				}
			}

			if phase == "before-policy-update" && updates != 0 {
				t.Fatal("PV policy changed after fence loss")
			}

			if phase == "before-delete" && updates != 1 {
				t.Fatalf("updates=%d; expected policy update before fence loss", updates)
			}
		})
	}
}
