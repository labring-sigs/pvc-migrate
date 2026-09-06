package app

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestActivationStorageRecoversMissingClaimsAndFencesEveryVolume(t *testing.T) {
	ctx := context.Background()
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseActivating
	createSourceStorage(t, fixture, session)

	fixture.service.switcher = kube.NewSwitcher(fixture.client)
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		volume.DestinationPVC.UID = volume.SourcePVC.UID + "-destination"
		volume.DestinationPV = domain.ObjectReference{
			Name: volume.SourcePV.Name + "-destination",
			UID:  volume.SourcePV.UID + "-destination",
		}

		_, err := fixture.client.CoreV1().PersistentVolumes().Create(ctx, &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   volume.DestinationPV.Name,
				UID:    volume.DestinationPV.UID,
				Labels: map[string]string{kube.SessionKey: session.ID},
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				ClaimRef: &corev1.ObjectReference{
					Namespace: volume.DestinationPVC.Namespace,
					Name:      volume.DestinationPVC.Name,
					UID:       volume.DestinationPVC.UID,
				},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := fixture.service.validateActivationStorage(ctx, session); err != nil {
		t.Fatal(err)
	}

	pv, err := fixture.client.CoreV1().
		PersistentVolumes().
		Get(ctx, session.Spec.Volumes[1].DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.ClaimRef.UID = "foreign"
	if _, err := fixture.client.CoreV1().
		PersistentVolumes().
		Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.service.validateActivationStorage(
		ctx,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("error=%v", err)
	}

	if fixture.store.updates != 0 {
		t.Fatal("validation mutated session")
	}
}
