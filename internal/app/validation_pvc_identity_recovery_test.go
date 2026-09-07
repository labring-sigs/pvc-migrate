package app

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRebindResumeValidatesInterruptedStorage(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhasePlanned, domain.PhaseRenaming, domain.PhaseMoving, domain.PhaseRollingBack} {
		for _, created := range []bool{false, true} {
			for _, failed := range []bool{false, true} {
				session := appTestSession()

				session.Status.Phase = phase
				if failed {
					session.Status.Phase = domain.PhaseFailed
					session.Status.ResumeFrom = phase
				}

				from := domain.ObjectReference{Namespace: "source", Name: "data", UID: "original"}

				to := domain.ObjectReference{Namespace: "target", Name: "data"}
				if phase == domain.PhaseRollingBack {
					to.UID = "old-original"
				}

				volume := domain.VolumeSpec{
					SourcePV: domain.ObjectReference{Name: "pv", UID: "pv-uid"},
				}
				pv := &corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "pv",
						UID:    "pv-uid",
						Labels: map[string]string{kube.SessionKey: session.ID},
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
							Namespace:   to.Namespace,
							Name:        to.Name,
							UID:         "new",
							Annotations: map[string]string{kube.SessionKey: session.ID},
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
				service := &Service{client: client, switcher: kube.NewSwitcher(client)}

				err := service.validateRebindTransition(t.Context(), session, &volume, from, to)
				if (err == nil) != (phase != domain.PhasePlanned) {
					t.Fatalf("phase=%s created=%v failed=%v error=%v", phase, created, failed, err)
				}
			}
		}
	}
}
