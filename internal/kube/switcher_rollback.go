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

// RollbackPVC restores the recorded source binding using an operation-owned manifest.
func (s *Switcher) RollbackPVC(
	ctx context.Context,
	sessionID string,
	volume PVCTransferBindings,
	desired *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
	progress ProgressFunc,
) error {
	if err := validateRollbackPVC(sessionID, volume, desired, status); err != nil {
		return err
	}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	manifest := desired.DeepCopy()

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

		recovered := current.Annotations[SessionKey] == sessionID
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

		return s.completeRollback(ctx, sessionID, volume, status, current, progress)
	}

	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "rollback volume", "read active PVC", err)
	}

	if err == nil {
		if err := s.removeActiveDestination(ctx, sessionID, volume, current); err != nil {
			return err
		}
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

	if err := s.validateBoundPVC(ctx, manifest); err != nil {
		return err
	}

	if err := s.reservePV(
		ctx,
		volume.SourcePV,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
		sessionID,
	); err != nil {
		return err
	}

	recreated, err := s.createBoundPVC(ctx, sessionID, manifest)
	if err != nil {
		return err
	}

	return s.completeRollback(ctx, sessionID, volume, status, recreated, progress)
}

func (s *Switcher) removeActiveDestination(
	ctx context.Context,
	sessionID string,
	volume PVCTransferBindings,
	current *corev1.PersistentVolumeClaim,
) error {
	if current.Spec.VolumeName != volume.DestinationPV.Name ||
		current.Annotations[SessionKey] != sessionID {
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
		sessionID,
		ResourceRoleDestination,
	); err != nil {
		return err
	}

	ref := v1alpha1.ObjectReference{
		Namespace: current.Namespace, Name: current.Name, UID: current.UID,
		ResourceVersion: current.ResourceVersion,
	}
	if err := s.deletePVC(ctx, ref); err != nil {
		return err
	}

	return s.ensureDetached(ctx, volume.DestinationPV.Name)
}

func validateRollbackPVC(sessionID string, volume PVCTransferBindings,
	desired *corev1.PersistentVolumeClaim, status *v1alpha1.ClusterVolumeActivationStatus,
) error {
	if sessionID == "" || desired == nil || status == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"rollback PVC",
			"workflow ID, desired PVC and activation checkpoint are required",
		)
	}

	if volume.SourcePVC.Namespace == "" ||
		volume.SourcePVC.Name == "" ||
		volume.SourcePVC.UID == "" ||
		volume.SourcePV.Name == "" ||
		volume.SourcePV.UID == "" ||
		volume.DestinationPV.Name == "" ||
		volume.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback PVC",
			"source PVC and source/destination PV identities are required",
		)
	}

	if desired.Namespace != volume.SourcePVC.Namespace ||
		desired.Name != volume.SourcePVC.Name ||
		desired.Spec.VolumeName != volume.SourcePV.Name ||
		desired.Labels[SessionKey] != sessionID ||
		desired.Annotations[SessionKey] != sessionID ||
		desired.Labels[ManagedByLabel] != ManagedByValue {
		return domain.NewError(
			domain.ErrorConflict,
			"rollback PVC",
			"desired PVC differs from the recorded rollback identity or ownership",
		)
	}

	capacity := desired.Spec.Resources.Requests[corev1.ResourceStorage]
	if capacity.Sign() <= 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"rollback PVC",
			"desired PVC capacity must be positive",
		)
	}

	return nil
}
