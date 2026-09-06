package kube

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Switcher) verifyRetainedActivationPV(
	ctx context.Context,
	sessionID string,
	pvcRef, pvRef, reservation domain.ObjectReference,
) error {
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, pvRef.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify activation recovery",
			"read PV "+pvRef.Name,
			err,
		)
	}

	if pv.UID != pvRef.UID || pv.DeletionTimestamp != nil ||
		pv.Labels[SessionKey] != sessionID ||
		pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		return domain.NewError(
			domain.ErrorConflict,
			"verify activation recovery",
			fmt.Sprintf(
				"PV %s identity, ownership, or retention changed after PVC deletion",
				pvRef.Name,
			),
		)
	}

	claim := pv.Spec.ClaimRef
	original := claim != nil && claim.Namespace == pvcRef.Namespace &&
		claim.Name == pvcRef.Name && claim.UID == pvcRef.UID

	reserved := claim != nil && reservation.Name != "" &&
		claim.Namespace == reservation.Namespace &&
		claim.Name == reservation.Name &&
		claim.UID == ""
	if !original && !reserved {
		return domain.NewError(domain.ErrorConflict, "verify activation recovery",
			fmt.Sprintf("PV %s claimRef changed after PVC deletion", pvRef.Name))
	}

	return nil
}
