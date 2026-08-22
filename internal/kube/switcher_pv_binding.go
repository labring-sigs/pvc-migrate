package kube

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

func (s *Switcher) deletePVC(ctx context.Context, ref domain.ObjectReference) error {
	if ref.Namespace == "" || ref.Name == "" || ref.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"delete PVC",
			"PVC namespace, name, and UID are required",
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"delete PVC",
			fmt.Sprintf("read %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	if pvc.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"delete PVC",
			fmt.Sprintf(
				"PVC %s/%s UID changed from %s to %s",
				ref.Namespace,
				ref.Name,
				ref.UID,
				pvc.UID,
			),
		)
	}

	if err := validatePVCDeletionFinalizers(pvc, "delete PVC"); err != nil {
		return err
	}

	preconditions := &metav1.Preconditions{UID: &pvc.UID, ResourceVersion: &pvc.ResourceVersion}
	if err := s.client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Delete(ctx, ref.Name, metav1.DeleteOptions{Preconditions: preconditions}); err != nil &&
		!apierrors.IsNotFound(err) {
		if apierrors.IsConflict(err) {
			return domain.WrapError(
				domain.ErrorConflict,
				"delete PVC",
				fmt.Sprintf("PVC %s/%s changed concurrently", ref.Namespace, ref.Name),
				err,
			)
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"delete PVC",
			fmt.Sprintf("delete %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	return s.waitFor(
		ctx,
		fmt.Sprintf("PVC %s/%s deletion", ref.Namespace, ref.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := s.client.CoreV1().
				PersistentVolumeClaims(ref.Namespace).
				Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}

			if getErr != nil {
				return false, getErr
			}

			if current.UID != pvc.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"delete PVC",
					fmt.Sprintf("PVC %s/%s name was reused", ref.Namespace, ref.Name),
				)
			}

			return false, nil
		},
	)
}

func (s *Switcher) reservePV(
	ctx context.Context,
	ref domain.ObjectReference,
	namespace, claim, sessionID string,
) error {
	if ref.Name == "" || ref.UID == "" || namespace == "" || claim == "" || sessionID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"reserve PV",
			"PV name/UID, claim namespace/name, and session ID are required",
		)
	}

	if err := s.waitForPVReservation(ctx, ref, namespace, claim); err != nil {
		return err
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return s.updatePVReservation(ctx, ref, namespace, claim, sessionID)
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve PV",
			fmt.Sprintf("update PV %s claimRef", ref.Name),
			err,
		)
	}

	return nil
}

func (s *Switcher) waitForPVReservation(
	ctx context.Context,
	ref domain.ObjectReference,
	namespace, claim string,
) error {
	return s.waitFor(
		ctx,
		fmt.Sprintf("PV %s release before reservation", ref.Name),
		func(waitCtx context.Context) (bool, error) {
			return s.pvReadyForReservation(waitCtx, ref, namespace, claim)
		},
	)
}

func (s *Switcher) pvReadyForReservation(
	ctx context.Context,
	ref domain.ObjectReference,
	namespace, claim string,
) (bool, error) {
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}

	if pv.UID != ref.UID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"reserve PV",
			fmt.Sprintf("PV %s UID changed", ref.Name),
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != namespace ||
		pv.Spec.ClaimRef.Name != claim {
		return pv.Status.Phase == corev1.VolumeReleased ||
			pv.Status.Phase == corev1.VolumeAvailable, nil
	}

	current, getErr := s.client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(ctx, claim, metav1.GetOptions{})
	if getErr == nil {
		if pv.Spec.ClaimRef.UID != "" && current.UID == pv.Spec.ClaimRef.UID &&
			current.Spec.VolumeName == pv.Name {
			return true, nil
		}

		return false, domain.NewError(
			domain.ErrorConflict,
			"reserve PV",
			fmt.Sprintf("PV %s claimRef points to a different PVC identity", pv.Name),
		)
	}

	if !apierrors.IsNotFound(getErr) {
		return false, getErr
	}
	// The recorded claim is stale after its PVC was deleted. The mutation below
	// can safely clear it before creating the new claim.
	return true, nil
}

func (s *Switcher) updatePVReservation(
	ctx context.Context,
	ref domain.ObjectReference,
	namespace, claim, sessionID string,
) error {
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if pv.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve PV",
			fmt.Sprintf("PV %s UID changed", ref.Name),
		)
	}

	if owner := pv.Labels[SessionKey]; owner != "" && owner != sessionID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve PV",
			fmt.Sprintf("PV %s belongs to session %s", ref.Name, owner),
		)
	}

	if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == namespace &&
		pv.Spec.ClaimRef.Name == claim {
		current, getErr := s.client.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, claim, metav1.GetOptions{})
		if getErr == nil {
			if pv.Spec.ClaimRef.UID != "" && current.UID == pv.Spec.ClaimRef.UID &&
				current.Spec.VolumeName == pv.Name {
				return nil
			}

			return domain.NewError(
				domain.ErrorConflict,
				"reserve PV",
				fmt.Sprintf("PV %s claimRef points to a different PVC identity", pv.Name),
			)
		}

		if getErr != nil && !apierrors.IsNotFound(getErr) {
			return getErr
		}

		if pv.Spec.ClaimRef.UID == "" {
			return nil
		}
	}

	pv.Spec.ClaimRef = &corev1.ObjectReference{
		APIVersion: domain.CoreAPIVersion,
		Kind:       domain.KindPersistentVolumeClaim,
		Namespace:  namespace,
		Name:       claim,
	}
	_, err = s.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

	return err
}

func (s *Switcher) ensureRetain(
	ctx context.Context,
	ref domain.ObjectReference,
	sessionID, role string,
) error {
	if ref.Name == "" || ref.UID == "" || sessionID == "" || role == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"retain PV",
			"PV name/UID, session ID, and role are required",
		)
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if pv.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"retain PV",
				fmt.Sprintf("PV %s UID changed", ref.Name),
			)
		}

		if pv.Labels == nil {
			pv.Labels = map[string]string{}
		}

		if pv.Annotations == nil {
			pv.Annotations = map[string]string{}
		}

		if owner := pv.Labels[SessionKey]; owner != "" && owner != sessionID {
			return domain.NewError(
				domain.ErrorConflict,
				"retain PV",
				fmt.Sprintf("PV %s belongs to session %s", ref.Name, owner),
			)
		}

		changed := markPVSession(pv.Labels, sessionID, role)
		if pv.Annotations[OriginalPolicyAnnotation] == "" {
			pv.Annotations[OriginalPolicyAnnotation] = string(pv.Spec.PersistentVolumeReclaimPolicy)
			changed = true
		}

		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
			changed = true
		}

		if !changed {
			return nil
		}

		_, err = s.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

		return err
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}
		return domain.WrapError(domain.ErrorKubernetes, "retain PV", "update PV "+ref.Name, err)
	}

	return nil
}

func (s *Switcher) verifyBinding(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	ref domain.ObjectReference,
) error {
	if pvc == nil || pvc.Namespace == "" || pvc.Name == "" || pvc.UID == "" || ref.Name == "" ||
		ref.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"verify binding",
			"PVC and PV identities are required",
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName != ref.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"verify binding",
			fmt.Sprintf("PVC %s/%s is not Bound to %s", pvc.Namespace, pvc.Name, ref.Name),
		)
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify binding", "read PV "+ref.Name, err)
	}

	if pv.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify binding",
			fmt.Sprintf("PV %s UID changed", ref.Name),
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"verify binding",
			fmt.Sprintf("PV %s claimRef does not match PVC UID %s", ref.Name, pvc.UID),
		)
	}

	return nil
}

// verifyPVCAndPVIdentity re-reads both objects before a copy and validates the
// persisted identities and the two-sided Kubernetes binding relationship.
func (s *Switcher) verifyPVCAndPVIdentity(
	ctx context.Context,
	pvcRef, pvRef domain.ObjectReference,
	role, sessionID string,
) error {
	if pvcRef.Namespace == "" || pvcRef.Name == "" || pvcRef.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify PVC offline",
			role+" PVC reference is incomplete",
		)
	}

	if pvRef.Name == "" || pvRef.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify PVC offline",
			role+" PV reference is incomplete",
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(pvcRef.Namespace).
		Get(ctx, pvcRef.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify PVC offline",
			fmt.Sprintf("read %s PVC %s/%s", role, pvcRef.Namespace, pvcRef.Name),
			err,
		)
	}

	if pvc.UID != pvcRef.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			fmt.Sprintf(
				"%s PVC %s/%s UID changed from %s to %s",
				role,
				pvc.Namespace,
				pvc.Name,
				pvcRef.UID,
				pvc.UID,
			),
		)
	}

	if err := validatePVCDeletionFinalizers(pvc, "verify PVC offline"); err != nil {
		return err
	}

	if sessionID != "" {
		for _, owner := range []string{pvc.Annotations[SessionKey], pvc.Labels[SessionKey]} {
			if owner != "" && owner != sessionID {
				return domain.NewError(
					domain.ErrorConflict,
					"verify PVC offline",
					fmt.Sprintf(
						"%s PVC %s/%s belongs to session %s",
						role,
						pvc.Namespace,
						pvc.Name,
						owner,
					),
				)
			}
		}
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify PVC offline",
			fmt.Sprintf("%s PVC %s/%s is %s", role, pvc.Namespace, pvc.Name, pvc.Status.Phase),
		)
	}

	if pvc.Spec.VolumeName != pvRef.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			fmt.Sprintf(
				"%s PVC %s/%s points to PV %s, expected %s",
				role,
				pvc.Namespace,
				pvc.Name,
				pvc.Spec.VolumeName,
				pvRef.Name,
			),
		)
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, pvRef.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify PVC offline",
			fmt.Sprintf("read %s PV %s", role, pvRef.Name),
			err,
		)
	}

	if pv.UID != pvRef.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			fmt.Sprintf("%s PV %s UID changed from %s to %s", role, pv.Name, pvRef.UID, pv.UID),
		)
	}

	if sessionID != "" {
		if owner := pv.Labels[SessionKey]; owner != "" && owner != sessionID {
			return domain.NewError(
				domain.ErrorConflict,
				"verify PVC offline",
				fmt.Sprintf("%s PV %s belongs to session %s", role, pv.Name, owner),
			)
		}
	}

	claimRef := pv.Spec.ClaimRef
	if claimRef == nil || claimRef.Namespace != pvc.Namespace || claimRef.Name != pvc.Name ||
		claimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			fmt.Sprintf(
				"%s PV %s claimRef does not match PVC %s/%s UID %s",
				role,
				pv.Name,
				pvc.Namespace,
				pvc.Name,
				pvc.UID,
			),
		)
	}

	return nil
}

func (s *Switcher) markPVPair(
	ctx context.Context,
	sessionID string,
	volume *domain.VolumeSpec,
	rolledBack bool,
) error {
	if sessionID == "" || volume == nil || volume.SourcePV.Name == "" ||
		volume.SourcePV.UID == "" ||
		volume.DestinationPV.Name == "" ||
		volume.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"mark PV pair",
			"session ID and source/destination PV identities are required",
		)
	}

	active := volume.DestinationPV

	rollback := volume.SourcePV
	if rolledBack {
		active, rollback = rollback, active
	}

	for _, item := range []struct {
		ref   domain.ObjectReference
		role  string
		other string
	}{
		{ref: active, role: ResourceRoleActive, other: rollback.Name},
		{ref: rollback, role: ResourceRoleRollback, other: active.Name},
	} {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			pv, err := s.client.CoreV1().
				PersistentVolumes().
				Get(ctx, item.ref.Name, metav1.GetOptions{})
			if err != nil {
				// A rollback PV is disposable once the source PV is active again.
				// Provisioners with a Delete reclaim policy can remove it while the
				// active PVC is being deleted; keep validating and marking the active
				// PV without turning a completed rollback into a failure.
				if rolledBack && item.role == ResourceRoleRollback && apierrors.IsNotFound(err) {
					return nil
				}
				return err
			}

			if pv.UID != item.ref.UID {
				return domain.NewError(
					domain.ErrorConflict,
					"mark PV pair",
					fmt.Sprintf("PV %s UID changed", item.ref.Name),
				)
			}

			if owner := pv.Labels[SessionKey]; owner != "" && owner != sessionID {
				return domain.NewError(
					domain.ErrorConflict,
					"mark PV pair",
					fmt.Sprintf("PV %s belongs to session %s", item.ref.Name, owner),
				)
			}

			if pv.Labels == nil {
				pv.Labels = map[string]string{}
			}

			if pv.Annotations == nil {
				pv.Annotations = map[string]string{}
			}

			changed := markPVSession(pv.Labels, sessionID, item.role)
			if pv.Annotations[PairedPVAnnotation] != item.other {
				pv.Annotations[PairedPVAnnotation] = item.other
				changed = true
			}

			if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
				pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
				changed = true
			}

			if !changed {
				return nil
			}

			_, err = s.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

			return err
		})
		if err != nil {
			if domain.CategoryOf(err) == domain.ErrorConflict {
				return err
			}

			return domain.WrapError(
				domain.ErrorKubernetes,
				"mark PV pair",
				"update PV "+item.ref.Name,
				err,
			)
		}
	}

	return nil
}
