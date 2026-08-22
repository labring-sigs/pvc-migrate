package kube

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Switcher) RenamePVC(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	progress ProgressFunc,
) (*corev1.PersistentVolumeClaim, error) {
	if err := validateRenamePVCRequest(session, volume); err != nil {
		return nil, err
	}

	if err := s.ensureNoConsumers(
		ctx,
		volume.SourcePVC.Namespace,
		volume.SourcePVC.Name,
	); err != nil {
		return nil, err
	}

	source, sourceErr := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if sourceErr != nil && !apierrors.IsNotFound(sourceErr) {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"rename PVC",
			"read source PVC",
			sourceErr,
		)
	}

	if sourceErr == nil {
		if source.UID != volume.SourcePVC.UID {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"rename PVC",
				fmt.Sprintf("source PVC %s/%s UID changed", source.Namespace, source.Name),
			)
		}

		if err := s.verifyBinding(ctx, source, volume.SourcePV); err != nil {
			return nil, err
		}
	}

	if existing, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.DestinationPVC.Namespace).
		Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{}); err == nil {
		if sourceErr == nil &&
			(source.Namespace != existing.Namespace || source.Name != existing.Name) {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"rename PVC",
				fmt.Sprintf(
					"both source PVC %s/%s and destination PVC %s/%s exist",
					source.Namespace,
					source.Name,
					existing.Namespace,
					existing.Name,
				),
			)
		}

		if existing.Spec.VolumeName == volume.SourcePV.Name &&
			existing.Annotations[SessionKey] == session.ID {
			if err := s.ensureNoConsumers(ctx, existing.Namespace, existing.Name); err != nil {
				return nil, err
			}

			if err := s.verifyBinding(ctx, existing, volume.SourcePV); err != nil {
				return nil, err
			}

			if err := s.ensureRetain(
				ctx,
				volume.SourcePV,
				session.ID,
				ResourceRoleActive,
			); err != nil {
				return nil, err
			}

			return existing, nil
		}

		return nil, domain.NewError(
			domain.ErrorConflict,
			"rename PVC",
			fmt.Sprintf("destination PVC %s/%s already exists", existing.Namespace, existing.Name),
		)
	} else if !apierrors.IsNotFound(
		err,
	) {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"rename PVC",
			"read destination PVC",
			err,
		)
	}

	if err := s.ensureRetain(ctx, volume.SourcePV, session.ID, ResourceRoleRename); err != nil {
		return nil, err
	}

	if err := s.deletePVC(ctx, volume.SourcePVC); err != nil {
		return nil, err
	}

	if err := s.ensureDetached(ctx, volume.SourcePV.Name); err != nil {
		return nil, err
	}

	if err := callProgress(progress); err != nil {
		return nil, err
	}

	cloned := *volume
	cloned.SourcePVC.Namespace = volume.DestinationPVC.Namespace
	cloned.SourcePVC.Name = volume.DestinationPVC.Name

	sourceClass := ""
	if volume.SourcePVCSpec.StorageClassName != nil {
		sourceClass = *volume.SourcePVCSpec.StorageClassName
	}

	if err := s.validateActivePVC(ctx, session, &cloned, volume.SourcePV, sourceClass); err != nil {
		return nil, err
	}

	if err := s.reservePV(
		ctx,
		volume.SourcePV,
		volume.DestinationPVC.Namespace,
		volume.DestinationPVC.Name,
		session.ID,
	); err != nil {
		return nil, err
	}

	created, err := s.createActivePVC(ctx, session, &cloned, volume.SourcePV, sourceClass)
	if err != nil {
		return nil, err
	}

	if err := s.verifyBinding(ctx, created, volume.SourcePV); err != nil {
		return nil, err
	}

	if err := s.ensureRetain(ctx, volume.SourcePV, session.ID, ResourceRoleActive); err != nil {
		return nil, err
	}

	return created, nil
}

func validateRenamePVCRequest(session *domain.Session, volume *domain.VolumeSpec) error {
	if session == nil || volume == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"rename PVC",
			"session and volume are required",
		)
	}

	if volume.SourcePVC.Namespace == "" || volume.SourcePVC.Name == "" ||
		volume.SourcePVC.UID == "" ||
		volume.SourcePV.Name == "" ||
		volume.SourcePV.UID == "" ||
		volume.DestinationPVC.Namespace == "" ||
		volume.DestinationPVC.Name == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rename PVC",
			"source PVC/PV identity and destination PVC name are required",
		)
	}

	return nil
}
