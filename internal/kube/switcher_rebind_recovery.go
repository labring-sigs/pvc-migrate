package kube

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VerifyPVCRebindRecovery fences the gap between deleting a claim and recording
// its replacement. The retained PV must still identify this exact operation.
func (s *Switcher) VerifyPVCRebindRecovery(
	ctx context.Context,
	sessionID string,
	from, to, pvRef domain.ObjectReference,
) error {
	if sessionID == "" || from.Namespace == "" || from.Name == "" || from.UID == "" ||
		to.Namespace == "" || to.Name == "" || pvRef.Name == "" || pvRef.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify PVC rebind recovery",
			"session and storage identities are required",
		)
	}

	if _, err := s.client.CoreV1().
		PersistentVolumeClaims(from.Namespace).
		Get(ctx, from.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"verify PVC rebind recovery",
				"read source PVC",
				err,
			)
		}

		return domain.NewError(
			domain.ErrorConflict,
			"verify PVC rebind recovery",
			"source PVC reappeared during recovery",
		)
	}

	target, err := s.client.CoreV1().
		PersistentVolumeClaims(to.Namespace).
		Get(ctx, to.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if err := s.verifyRetainedActivationPV(ctx, sessionID, from, pvRef, to); err != nil {
			return err
		}
	case err != nil:
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify PVC rebind recovery",
			"read destination PVC",
			err,
		)
	default:
		if target.DeletionTimestamp != nil || target.Annotations[SessionKey] != sessionID ||
			(to.UID != "" && target.UID != to.UID) {
			return domain.NewError(
				domain.ErrorConflict,
				"verify PVC rebind recovery",
				"destination PVC identity or ownership changed",
			)
		}

		pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, pvRef.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"verify PVC rebind recovery",
				"read retained PV",
				err,
			)
		}

		if pv.UID != pvRef.UID || pv.DeletionTimestamp != nil ||
			pv.Labels[SessionKey] != sessionID ||
			pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			return domain.NewError(domain.ErrorConflict, "verify PVC rebind recovery",
				fmt.Sprintf("PV %s identity, ownership, or retention changed", pvRef.Name))
		}

		to.UID = target.UID
		if err := s.verifyPVCAndPVIdentity(
			ctx,
			to,
			pvRef,
			ResourceRoleActive,
			sessionID,
			nil,
		); err != nil {
			return err
		}
	}

	for _, ref := range []domain.ObjectReference{from, to} {
		if err := s.ensureNoConsumers(ctx, ref.Namespace, ref.Name); err != nil {
			return err
		}
	}

	return s.ensureDetached(ctx, pvRef.Name)
}
