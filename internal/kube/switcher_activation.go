package kube

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Switcher) ActivateVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if err := validateActivationInputs(session, volume, status); err != nil {
		return err
	}

	temporaryPVC, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.DestinationPVC.Namespace).
		Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"activate volume",
			"read temporary destination PVC",
			err,
		)
	}

	if err == nil {
		if err := s.validateTemporaryActivationPVC(ctx, temporaryPVC, volume, status); err != nil {
			return err
		}
	}

	return s.activateVolumeResources(ctx, session, volume, status, progress)
}

func validateActivationInputs(
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
) error {
	if session == nil || volume == nil || status == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"activate volume",
			"session, volume, and volume status are required",
		)
	}

	if volume.SourcePVC.Namespace == "" ||
		volume.SourcePVC.Name == "" ||
		volume.SourcePVC.UID == "" ||
		volume.SourcePV.Name == "" ||
		volume.SourcePV.UID == "" ||
		volume.DestinationPVC.Namespace == "" ||
		volume.DestinationPVC.Name == "" ||
		volume.DestinationPVC.UID == "" ||
		volume.DestinationPV.Name == "" ||
		volume.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate volume",
			"source and destination PVC/PV identities are required",
		)
	}

	if status.Sync.FinalCompletedAt == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate volume",
			fmt.Sprintf("PVC %s has no completed final sync", volume.SourcePVC.Name),
		)
	}

	return nil
}

func (s *Switcher) validateTemporaryActivationPVC(
	ctx context.Context,
	temporaryPVC *corev1.PersistentVolumeClaim,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
) error {
	if status.Activation.TemporaryPVCDeleted {
		return domain.NewError(
			domain.ErrorConflict,
			"activate volume",
			fmt.Sprintf(
				"temporary destination PVC %s/%s reappeared after its deletion checkpoint",
				temporaryPVC.Namespace,
				temporaryPVC.Name,
			),
		)
	}

	if temporaryPVC.UID != volume.DestinationPVC.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"activate volume",
			fmt.Sprintf(
				"temporary destination PVC %s/%s UID changed",
				temporaryPVC.Namespace,
				temporaryPVC.Name,
			),
		)
	}

	return s.verifyBinding(ctx, temporaryPVC, volume.DestinationPV)
}

func (s *Switcher) activateVolumeResources(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if err := s.ensureNoConsumers(
		ctx,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
	); err != nil {
		return err
	}

	if err := s.ensureNoConsumers(
		ctx,
		volume.DestinationPVC.Namespace,
		volume.DestinationPVC.Name,
	); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.SourcePV.Name); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.DestinationPV.Name); err != nil {
		return err
	}

	if active, err := s.activePVC(ctx, session, volume); err != nil {
		return err
	} else if active != nil {
		return s.completeActivation(ctx, session, volume, status, active, progress)
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

	if err := s.deleteTemporaryActivationPVC(ctx, volume, status, progress); err != nil {
		return err
	}

	if err := s.deleteSourceActivationPVC(ctx, volume, status, progress); err != nil {
		return err
	}

	if err := s.reserveActivationDestination(ctx, session, volume, status, progress); err != nil {
		return err
	}

	created, err := s.createActivePVC(
		ctx,
		session,
		volume,
		volume.DestinationPV,
		volume.StorageClass,
	)
	if err != nil {
		return err
	}

	return s.completeActivation(ctx, session, volume, status, created, progress)
}

func (s *Switcher) deleteTemporaryActivationPVC(
	ctx context.Context,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if status.Activation.TemporaryPVCDeleted {
		return nil
	}

	if err := s.deletePVC(ctx, volume.DestinationPVC); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.DestinationPV.Name); err != nil {
		return err
	}

	status.Activation.TemporaryPVCDeleted = true

	return callProgress(progress)
}

func (s *Switcher) deleteSourceActivationPVC(
	ctx context.Context,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if status.Activation.SourcePVCDeleted {
		return nil
	}

	if err := s.deletePVC(ctx, volume.SourcePVC); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.SourcePV.Name); err != nil {
		return err
	}

	status.Activation.SourcePVCDeleted = true

	return callProgress(progress)
}

func (s *Switcher) reserveActivationDestination(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress ProgressFunc,
) error {
	if status.Activation.DestinationReserved {
		return nil
	}

	if err := s.validateActivePVC(
		ctx,
		session,
		volume,
		volume.DestinationPV,
		volume.StorageClass,
	); err != nil {
		return err
	}

	if err := s.reservePV(
		ctx,
		volume.DestinationPV,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
		session.ID,
	); err != nil {
		return err
	}

	status.Activation.DestinationReserved = true

	return callProgress(progress)
}
