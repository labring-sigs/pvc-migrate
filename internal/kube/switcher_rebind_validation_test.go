package kube

import (
	"fmt"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestVerifyPVCRebindRequiresPersistedStageForRecovery(t *testing.T) {
	for _, recovering := range []bool{false, true} {
		for _, created := range []bool{false, true} {
			t.Run(fmt.Sprintf("recovering=%v/created=%v", recovering, created), func(t *testing.T) {
				from := v1alpha1.ObjectReference{Namespace: "source", Name: "data", UID: "original"}
				to := v1alpha1.ObjectReference{Namespace: "target", Name: "data"}
				pvRef := v1alpha1.ObjectReference{Name: "pv", UID: "pv-uid"}
				pv := &corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "pv",
						UID:    "pv-uid",
						Labels: map[string]string{SessionKey: "workflow"},
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
						ClaimRef: &corev1.ObjectReference{
							Namespace: from.Namespace,
							Name:      from.Name,
							UID:       from.UID,
						},
					},
				}

				objects := []runtime.Object{pv}
				if created {
					pvc := &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: to.Namespace, Name: to.Name, UID: "new",
							Annotations: map[string]string{SessionKey: "workflow"},
						},
						Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: pv.Name},
						Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
					}
					pv.Spec.ClaimRef = &corev1.ObjectReference{
						Namespace: to.Namespace,
						Name:      to.Name,
						UID:       pvc.UID,
					}
					objects = append(objects, pvc)
				}

				client := fake.NewClientset(objects...)

				err := NewSwitcher(
					client,
				).VerifyPVCRebind(t.Context(), "workflow", from, to, pvRef, recovering)
				if (err == nil) != recovering {
					t.Fatalf("unexpected recovery validation: %v", err)
				}

				for _, action := range client.Actions() {
					if action.GetVerb() != "get" && action.GetVerb() != "list" {
						t.Fatalf("validation mutated storage: %v", action)
					}
				}
			})
		}
	}
}
