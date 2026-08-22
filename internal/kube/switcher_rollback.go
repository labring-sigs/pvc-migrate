package kube

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Switcher) RollbackVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if err := validateRollbackVolume(session, volume, status); err != nil {
		return err
	}

	if err := s.ensureNoConsumers(
		ctx,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
	); err != nil {
		return err
	}

	current, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err == nil && current.Spec.VolumeName == volume.SourcePV.Name {
		original := current.UID == volume.SourcePVC.UID

		recovered := current.Annotations[SessionKey] == session.ID
		if !original && !recovered {
			return domain.NewError(
				domain.ErrorConflict,
				"rollback volume",
				fmt.Sprintf(
					"PVC %s/%s is not the original or session-owned source PVC",
					current.Namespace,
					current.Name,
				),
			)
		}

		return s.completeRollback(ctx, session, volume, status, current, progress)
	}

	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "rollback volume", "read active PVC", err)
	}

	if err == nil {
		if err := s.removeActiveDestination(ctx, session, volume, current); err != nil {
			return err
		}
	}

	if err := s.ensureRetain(ctx, volume.SourcePV, session.ID, ResourceRoleSource); err != nil {
		return err
	}

	if err := s.ensureRetain(
		ctx,
		volume.DestinationPV,
		session.ID,
		ResourceRoleDestination,
	); err != nil {
		return err
	}

	sourceClass := ""
	if volume.SourcePVCSpec.StorageClassName != nil {
		sourceClass = *volume.SourcePVCSpec.StorageClassName
	}

	if err := s.validateActivePVC(ctx, session, volume, volume.SourcePV, sourceClass); err != nil {
		return err
	}

	if err := s.reservePV(
		ctx,
		volume.SourcePV,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
		session.ID,
	); err != nil {
		return err
	}

	recreated, err := s.createActivePVC(ctx, session, volume, volume.SourcePV, sourceClass)
	if err != nil {
		return err
	}

	return s.completeRollback(ctx, session, volume, status, recreated, progress)
}

func validateRollbackVolume(
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
) error {
	if session == nil || volume == nil || status == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"rollback volume",
			"session, volume, and volume status are required",
		)
	}

	if volume.SourcePVC.Namespace == "" || volume.SourcePVC.Name == "" ||
		volume.SourcePVC.UID == "" || volume.SourcePV.Name == "" || volume.SourcePV.UID == "" ||
		volume.DestinationPV.Name == "" || volume.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback volume",
			"source PVC and source/destination PV identities are required",
		)
	}

	return nil
}

func (s *Switcher) removeActiveDestination(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	current *corev1.PersistentVolumeClaim,
) error {
	if current.Spec.VolumeName != volume.DestinationPV.Name ||
		current.Annotations[SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"rollback volume",
			fmt.Sprintf(
				"PVC %s/%s is not the session's active destination",
				current.Namespace,
				current.Name,
			),
		)
	}

	if err := s.verifyBinding(ctx, current, volume.DestinationPV); err != nil {
		return err
	}
	// The storage provisioner may restore the destination PV's original reclaim
	// policy after activation, so retain it before deleting the active PVC.
	if err := s.ensureRetain(
		ctx,
		volume.DestinationPV,
		session.ID,
		ResourceRoleDestination,
	); err != nil {
		return err
	}

	ref := domain.ObjectReference{
		Namespace: current.Namespace, Name: current.Name, UID: current.UID,
		ResourceVersion: current.ResourceVersion,
	}
	if err := s.deletePVC(ctx, ref); err != nil {
		return err
	}

	return s.ensureDetached(ctx, volume.DestinationPV.Name)
}
