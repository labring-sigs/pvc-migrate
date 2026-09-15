package kube

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Switcher) RenamePVC(
	ctx context.Context,
	sessionID string,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	destination *corev1.PersistentVolumeClaim,
	progress ProgressFunc,
) (*corev1.PersistentVolumeClaim, error) {
	if err := validateRenamePVCRequest(sessionID, sourcePVC, sourcePV, destination); err != nil {
		return nil, err
	}

	if err := s.ensureNoConsumers(
		ctx,
		sourcePVC.Namespace,
		sourcePVC.Name,
	); err != nil {
		return nil, err
	}

	source, sourceErr := s.client.CoreV1().
		PersistentVolumeClaims(sourcePVC.Namespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if sourceErr != nil && !apierrors.IsNotFound(sourceErr) {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"rename PVC",
			"read source PVC",
			sourceErr,
		)
	}

	if sourceErr == nil {
		if source.UID != sourcePVC.UID {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"rename PVC",
				fmt.Sprintf("source PVC %s/%s UID changed", source.Namespace, source.Name),
			)
		}

		if err := s.verifyBinding(ctx, source, sourcePV); err != nil {
			return nil, err
		}
	}

	if existing, err := s.client.CoreV1().
		PersistentVolumeClaims(destination.Namespace).
		Get(ctx, destination.Name, metav1.GetOptions{}); err == nil {
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

		if existing.Spec.VolumeName == sourcePV.Name &&
			existing.Annotations[SessionKey] == sessionID {
			if err := s.ensureNoConsumers(ctx, existing.Namespace, existing.Name); err != nil {
				return nil, err
			}

			if err := s.verifyBinding(ctx, existing, sourcePV); err != nil {
				return nil, err
			}

			if err := s.ensureRetain(
				ctx,
				sourcePV,
				sessionID,
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

	if err := s.ensureRetain(ctx, sourcePV, sessionID, ResourceRoleRename); err != nil {
		return nil, err
	}

	if err := s.deletePVC(ctx, sourcePVC); err != nil {
		return nil, err
	}

	if err := s.ensureDetached(ctx, sourcePV.Name); err != nil {
		return nil, err
	}

	if err := callProgress(progress); err != nil {
		return nil, err
	}

	destination = destination.DeepCopy()

	destination.Spec.VolumeName = sourcePV.Name
	if err := s.validateBoundPVC(ctx, destination); err != nil {
		return nil, err
	}

	if err := s.reservePV(
		ctx,
		sourcePV,
		destination.Namespace,
		destination.Name,
		sessionID,
	); err != nil {
		return nil, err
	}

	created, err := s.createBoundPVC(ctx, sessionID, destination)
	if err != nil {
		return nil, err
	}

	if err := s.verifyBinding(ctx, created, sourcePV); err != nil {
		return nil, err
	}

	if err := s.ensureRetain(ctx, sourcePV, sessionID, ResourceRoleActive); err != nil {
		return nil, err
	}

	return created, nil
}

func validateRenamePVCRequest(
	sessionID string,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	destination *corev1.PersistentVolumeClaim,
) error {
	if sessionID == "" || destination == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"rename PVC",
			"session and volume are required",
		)
	}

	if sourcePVC.Namespace == "" || sourcePVC.Name == "" ||
		sourcePVC.UID == "" ||
		sourcePV.Name == "" ||
		sourcePV.UID == "" ||
		destination.Namespace == "" ||
		destination.Name == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rename PVC",
			"source PVC/PV identity and destination PVC name are required",
		)
	}

	if destination.Spec.VolumeName != sourcePV.Name ||
		destination.Labels[ManagedByLabel] != ManagedByValue ||
		destination.Labels[SessionKey] != sessionID ||
		destination.Annotations[SessionKey] != sessionID {
		return domain.NewError(domain.ErrorValidation, "rename PVC",
			"destination manifest must retain the source PV and identify its owning session")
	}

	return nil
}
