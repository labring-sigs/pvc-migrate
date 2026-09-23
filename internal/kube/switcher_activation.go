package kube

import (
	"context"
	"errors"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ActivatePVC replaces the source claim with the prepared destination binding.
// The operation must verify final synchronization before entering this resource step.
// activateNamespace is the namespace the activated claim lands in; cross-namespace
// migrations move it off the source namespace while keeping the claim name.
func (s *Switcher) ActivatePVC(
	ctx context.Context,
	sessionID string,
	activateNamespace string,
	volume PVCTransferBindings,
	desired *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
	progress ProgressFunc,
) error {
	if sessionID == "" || activateNamespace == "" || status == nil || desired == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"activate PVC",
			"workflow ID, activate namespace, desired PVC and activation checkpoint are required",
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
			"activate PVC",
			"source and destination PVC/PV identities are required",
		)
	}

	if desired.Namespace != activateNamespace ||
		desired.Name != volume.SourcePVC.Name ||
		desired.Spec.VolumeName != volume.DestinationPV.Name ||
		desired.Labels[SessionKey] != sessionID ||
		desired.Annotations[SessionKey] != sessionID ||
		desired.Labels[ManagedByLabel] != ManagedByValue {
		return domain.NewError(
			domain.ErrorConflict,
			"activate PVC",
			"desired PVC differs from the recorded cutover identity or ownership",
		)
	}

	capacity := desired.Spec.Resources.Requests[corev1.ResourceStorage]
	if capacity.Sign() <= 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"activate PVC",
			"desired PVC capacity must be positive",
		)
	}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	temporaryPVC, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.DestinationPVC.Namespace).
		Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"activate PVC",
			"read temporary destination PVC",
			err,
		)
	}

	if err == nil {
		if err := s.validateTemporaryActivationPVC(ctx, temporaryPVC, volume, status); err != nil {
			return err
		}
	}

	return s.activateVolumeResources(
		ctx,
		sessionID,
		activateNamespace,
		volume,
		desired.DeepCopy(),
		status,
		progress,
	)
}

func (s *Switcher) validateTemporaryActivationPVC(
	ctx context.Context,
	temporaryPVC *corev1.PersistentVolumeClaim,
	volume PVCTransferBindings,
	status *v1alpha1.ClusterVolumeActivationStatus,
) error {
	if status.TemporaryPVCDeleted {
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
	sessionID string,
	activateNamespace string,
	volume PVCTransferBindings,
	desired *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
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

	if active, err := s.activePVC(
		ctx,
		sessionID,
		activateNamespace,
		volume.SourcePVC,
		volume.SourcePV,
		volume.DestinationPV,
	); err != nil {
		return err
	} else if active != nil {
		return s.completeActivation(ctx, sessionID, volume, desired, status, active, progress)
	}

	if err := s.ensureRetain(ctx, volume.SourcePV, sessionID, ResourceRoleSource); err != nil {
		return err
	}

	if err := s.ensureRetain(
		ctx,
		volume.DestinationPV,
		sessionID,
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

	if err := s.reserveActivationDestination(
		ctx,
		sessionID,
		volume,
		desired,
		status,
		progress,
	); err != nil {
		return err
	}

	created, err := s.createBoundPVC(ctx, sessionID, desired)
	if err != nil {
		return err
	}

	return s.completeActivation(ctx, sessionID, volume, desired, status, created, progress)
}

func (s *Switcher) deleteTemporaryActivationPVC(
	ctx context.Context,
	volume PVCTransferBindings,
	status *v1alpha1.ClusterVolumeActivationStatus,
	progress ProgressFunc,
) error {
	if status.TemporaryPVCDeleted {
		return nil
	}

	if err := s.deletePVC(ctx, volume.DestinationPVC); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.DestinationPV.Name); err != nil {
		return err
	}

	before := status.DeepCopy()
	status.TemporaryPVCDeleted = true

	return saveActivationCheckpoint(ctx, status, before, progress)
}

func (s *Switcher) deleteSourceActivationPVC(
	ctx context.Context,
	volume PVCTransferBindings,
	status *v1alpha1.ClusterVolumeActivationStatus,
	progress ProgressFunc,
) error {
	if status.SourcePVCDeleted {
		return nil
	}

	if err := s.deletePVC(ctx, volume.SourcePVC); err != nil {
		return err
	}

	if err := s.ensureDetached(ctx, volume.SourcePV.Name); err != nil {
		return err
	}

	before := status.DeepCopy()
	status.SourcePVCDeleted = true

	return saveActivationCheckpoint(ctx, status, before, progress)
}

func (s *Switcher) reserveActivationDestination(
	ctx context.Context,
	sessionID string,
	volume PVCTransferBindings,
	desired *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
	progress ProgressFunc,
) error {
	if status.DestinationReserved {
		return nil
	}

	if err := s.validateBoundPVC(ctx, desired); err != nil {
		return err
	}

	if err := s.reservePV(
		ctx,
		volume.DestinationPV,
		desired.Namespace,
		desired.Name,
		sessionID,
	); err != nil {
		return err
	}

	before := status.DeepCopy()
	status.DestinationReserved = true

	return saveActivationCheckpoint(ctx, status, before, progress)
}

func saveActivationCheckpoint(
	ctx context.Context,
	status, previous *v1alpha1.ClusterVolumeActivationStatus,
	progress ProgressFunc,
) error {
	err := errors.Join(ctx.Err(), LeaseFenceError(ctx))
	if err == nil {
		err = callProgress(progress)
	}

	if err != nil {
		*status = *previous
	}

	return err
}
