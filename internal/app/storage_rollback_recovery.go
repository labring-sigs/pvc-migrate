package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A successful API mutation may precede the status checkpoint. Recover from
// live storage identities without changing the persisted session during validation.
func (s *Service) validateUnrecordedRollbackStorage(
	ctx context.Context,
	session *domain.Session,
	index int,
) (bool, error) {
	volume := &session.Spec.Volumes[index]

	pvc, err := s.client.CoreV1().PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		active := session.Status.Volumes[index].Activation.ActivePVC
		if active.Name == "" || active.UID == "" {
			return false, nil
		}
		// Rollback reverses the retained pair: the deleted claim was on the
		// destination PV, and the original PV may already be reserved again.
		reverse := *volume
		reverse.SourcePVC = active
		reverse.SourcePV = volume.DestinationPV
		reverse.DestinationPVC = volume.SourcePVC
		reverse.DestinationPV = volume.SourcePV

		return true, s.switcher.VerifyActivationRecovery(
			ctx,
			session.ID,
			[]*domain.VolumeSpec{&reverse},
		)
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			"read rollback PVC",
			err,
		)
	}

	if pvc.Spec.VolumeName != volume.SourcePV.Name {
		return false, nil
	}

	ref := domain.ObjectReference{
		APIVersion:      corev1.SchemeGroupVersion.String(),
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}

	return true, s.verifyRollbackStorageVolumeWithRef(ctx, session, index, ref)
}
