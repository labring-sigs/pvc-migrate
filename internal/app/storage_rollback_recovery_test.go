package app

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRollbackRecoveryValidatesUnrecordedRestoredPVC(t *testing.T) {
	for _, scenario := range []string{"restored", "foreign-pvc", "pv-uid", "claim-uid"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			createActiveDestinationStorage(t, fixture, session)

			persisted := session.Status.Volumes[0].Activation
			if err := fixture.switcher.RollbackVolume(
				t.Context(),
				session,
				&session.Spec.Volumes[0],
				&session.Status.Volumes[0],
				nil,
			); err != nil {
				t.Fatal(err)
			}

			session.Status.Volumes[0].Activation = persisted
			session.Status.Phase = domain.PhaseRollingBack
			volume := session.Spec.Volumes[0]

			pvc, err := fixture.client.CoreV1().
				PersistentVolumeClaims(volume.SourcePVC.Namespace).
				Get(t.Context(), volume.SourcePVC.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			pv, err := fixture.client.CoreV1().
				PersistentVolumes().
				Get(t.Context(), volume.SourcePV.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if scenario == "foreign-pvc" {
				pvc.UID = "foreign"
				pvc.Annotations = nil
				pv.Spec.ClaimRef.UID = pvc.UID
			}

			if scenario == "pv-uid" {
				pv.UID = "foreign"
			}

			if scenario == "claim-uid" {
				pv.Spec.ClaimRef.UID = "foreign"
			}

			if _, err := fixture.client.CoreV1().
				PersistentVolumeClaims(pvc.Namespace).
				Update(t.Context(), pvc, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			if _, err := fixture.client.CoreV1().
				PersistentVolumes().
				Update(t.Context(), pv, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			err = fixture.service.validateRollbackRecoveryStorage(
				t.Context(),
				session,
				domain.PhaseCompleted,
			)
			if scenario == "restored" {
				if err != nil {
					t.Fatal(err)
				}
			} else if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("error=%v", err)
			}

			if session.Status.Volumes[0].Activation != persisted || fixture.store.updates != 0 {
				t.Fatal("validation changed checkpoint")
			}
		})
	}
}

func TestRollbackRecoveryValidatesDeletedActivePVC(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		createActiveDestinationStorage(t, fixture, session)
		fixture.service.switcher = kube.NewSwitcher(fixture.client)

		volume := session.Spec.Volumes[0]
		if _, err := fixture.client.CoreV1().
			PersistentVolumes().
			Create(t.Context(), &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: volume.SourcePV.Name, UID: volume.SourcePV.UID},
				Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
					Namespace: volume.SourcePVC.Namespace,
					Name:      volume.SourcePVC.Name,
					UID:       volume.SourcePVC.UID,
				}},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		if err := fixture.client.CoreV1().
			PersistentVolumeClaims(volume.SourcePVC.Namespace).
			Delete(t.Context(), volume.SourcePVC.Name, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}

		for _, ref := range []domain.ObjectReference{volume.SourcePV, volume.DestinationPV} {
			pv, err := fixture.client.CoreV1().
				PersistentVolumes().
				Get(t.Context(), ref.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain

			pv.Labels = map[string]string{kube.SessionKey: session.ID}
			if reserved && ref.Name == volume.SourcePV.Name {
				pv.Spec.ClaimRef.UID = ""
			}

			if _, err := fixture.client.CoreV1().
				PersistentVolumes().
				Update(t.Context(), pv, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		}

		if err := fixture.service.validateRollbackRecoveryStorage(
			t.Context(),
			session,
			domain.PhaseCompleted,
		); err != nil {
			t.Fatal(err)
		}
	}
}
