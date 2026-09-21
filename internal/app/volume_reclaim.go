package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// Resource deletion checks the recorded identity and ownership at every retry.
// The owning operation decides which resources and policies may be reclaimed.
func deleteManagedPVC(
	ctx context.Context,
	client kubernetes.Interface,
	sessionID string,
	ref v1alpha1.ObjectReference,
) error {
	if err := checkpointFenceError(ctx); err != nil {
		return err
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("read PVC %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	if pvc.UID != ref.UID || pvc.Labels[kube.SessionKey] != sessionID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("PVC %s/%s identity or session ownership changed", ref.Namespace, ref.Name),
		)
	}

	uid := pvc.UID

	resourceVersion := pvc.ResourceVersion

	if err := checkpointFenceError(ctx); err != nil {
		return err
	}

	if err := client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Delete(ctx, ref.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}}); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("delete PVC %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}
	if err := checkpointFenceError(ctx); err != nil {
		return err
	}

	return nil
}

func deleteReclaimedPV(
	ctx context.Context,
	client kubernetes.Interface,
	logger *slog.Logger,
	sessionID string,
	ref v1alpha1.ObjectReference,
	expectedRole string,
	policy corev1.PersistentVolumeReclaimPolicy,
	uncheckpointedClaim *v1alpha1.ObjectReference,
) error {
	if err := checkpointFenceError(ctx); err != nil {
		return err
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup PV",
			"read PV "+ref.Name,
			err,
		)
	}

	if !cleanupPVIdentityMatches(pv, ref, sessionID, expectedRole, uncheckpointedClaim) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup PV",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}
	// PVC deletion and PV release are asynchronous Kubernetes controller updates.
	if pv.Status.Phase == corev1.VolumeBound {
		pv, err = waitForPVRelease(
			ctx, client, logger,
			pv,
			ref,
			sessionID,
			expectedRole,
			uncheckpointedClaim,
		)
		if err != nil || pv == nil {
			return err
		}
	}

	if pv.Status.Phase != corev1.VolumeReleased && pv.Status.Phase != corev1.VolumeAvailable {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup PV",
			fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase),
		)
	}

	pv, err = restoreReclaimedPVPolicy(
		ctx, client,
		ref,
		sessionID,
		expectedRole,
		policy,
		uncheckpointedClaim,
	)
	if err != nil || pv == nil {
		return err
	}

	uid, resourceVersion := pv.UID, pv.ResourceVersion

	if err := checkpointFenceError(ctx); err != nil {
		return err
	}

	if err := client.CoreV1().
		PersistentVolumes().
		Delete(ctx, pv.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}}); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup PV",
			"delete PV "+pv.Name,
			err,
		)
	}
	if err := checkpointFenceError(ctx); err != nil {
		return err
	}

	return waitForPVDeletion(ctx, client, ref)
}

func restoreReclaimedPVPolicy(
	ctx context.Context,
	client kubernetes.Interface,
	ref v1alpha1.ObjectReference,
	sessionID string,
	expectedRole string,
	policy corev1.PersistentVolumeReclaimPolicy,
	uncheckpointedClaim *v1alpha1.ObjectReference,
) (*corev1.PersistentVolume, error) {
	if !validReclaimPolicy(policy) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"cleanup PV",
			fmt.Sprintf("PV %s has no valid original reclaim policy", ref.Name),
		)
	}

	var prepared *corev1.PersistentVolume

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		current, err := client.CoreV1().
			PersistentVolumes().
			Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			prepared = nil

			return nil
		}

		if err != nil {
			return err
		}

		if !cleanupPVIdentityMatches(current, ref, sessionID, expectedRole, uncheckpointedClaim) ||
			(current.Status.Phase != corev1.VolumeReleased && current.Status.Phase != corev1.VolumeAvailable) ||
			!reclaimPVPolicyMatches(current, policy, uncheckpointedClaim) {
			return domain.NewError(
				domain.ErrorConflict,
				"cleanup PV",
				fmt.Sprintf(
					"PV %s identity, ownership, state, or reclaim policy changed",
					ref.Name,
				),
			)
		}

		if current.Spec.PersistentVolumeReclaimPolicy == policy {
			prepared = current

			return nil
		}

		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		current.Spec.PersistentVolumeReclaimPolicy = policy
		prepared, err = client.CoreV1().PersistentVolumes().Update(
			ctx,
			current,
			metav1.UpdateOptions{},
		)

		return errors.Join(err, checkpointFenceError(ctx))
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return nil, err
		}

		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup PV",
			"restore reclaim policy for PV "+ref.Name,
			err,
		)
	}

	return prepared, nil
}

func reclaimPVPolicyMatches(
	pv *corev1.PersistentVolume,
	policy corev1.PersistentVolumeReclaimPolicy,
	uncheckpointedClaim *v1alpha1.ObjectReference,
) bool {
	if pv == nil || !validReclaimPolicy(policy) {
		return false
	}

	if uncheckpointedClaim != nil && uncheckpointedDestinationPVMatches(pv, *uncheckpointedClaim) {
		return policy == corev1.PersistentVolumeReclaimDelete
	}

	return pv.Annotations[kube.OriginalPolicyAnnotation] == string(policy) &&
		(pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain ||
			pv.Spec.PersistentVolumeReclaimPolicy == policy)
}

func waitForPVDeletion(
	ctx context.Context,
	client kubernetes.Interface,
	ref v1alpha1.ObjectReference,
) error {
	err := kube.WaitFor(
		ctx,
		time.Second,
		fmt.Sprintf("PV %s deletion", ref.Name),
		func(waitCtx context.Context) (bool, error) {
			current, err := client.CoreV1().PersistentVolumes().Get(
				waitCtx,
				ref.Name,
				metav1.GetOptions{},
			)
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			if err != nil {
				return false, err
			}

			if current.UID != ref.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"cleanup PV",
					fmt.Sprintf("PV %s was replaced while waiting for deletion", ref.Name),
				)
			}

			return false, nil
		},
	)
	if err != nil && domain.CategoryOf(err) == domain.ErrorInternal {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup PV",
			"wait for PV "+ref.Name+" deletion",
			err,
		)
	}

	return err
}

func waitForPVRelease(
	ctx context.Context,
	client kubernetes.Interface,
	logger *slog.Logger,
	pv *corev1.PersistentVolume,
	ref v1alpha1.ObjectReference,
	sessionID, expectedRole string,
	uncheckpointedClaim *v1alpha1.ObjectReference,
) (*corev1.PersistentVolume, error) {
	if err := validateDeletingPVClaim(ctx, client, pv); err != nil {
		return nil, err
	}

	if logger != nil {
		logger.Info("waiting for PV release", "session", sessionID, "pv", pv.Name)
	}

	err := kube.WaitFor(
		ctx,
		time.Second,
		fmt.Sprintf("PV %s release", pv.Name),
		func(waitCtx context.Context) (bool, error) {
			return reclaimedPVReleased(
				waitCtx, client,
				ref,
				sessionID,
				expectedRole,
				uncheckpointedClaim,
			)
		},
	)
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorInternal {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				"cleanup PV",
				fmt.Sprintf("wait for PV %s release", ref.Name),
				err,
			)
		}

		return nil, err
	}

	current, err := client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup PV",
			fmt.Sprintf("read PV %s after release", ref.Name),
			err,
		)
	}

	if !cleanupPVIdentityMatches(current, ref, sessionID, expectedRole, uncheckpointedClaim) {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"cleanup PV",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	return current, nil
}

func validateDeletingPVClaim(
	ctx context.Context,
	client kubernetes.Interface,
	pv *corev1.PersistentVolume,
) error {
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace == "" ||
		pv.Spec.ClaimRef.Name == "" ||
		pv.Spec.ClaimRef.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup PV",
			fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase),
		)
	}

	claim, err := client.CoreV1().
		PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).
		Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup PV",
			fmt.Sprintf("read PVC %s/%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name),
			err,
		)
	}

	if claim.UID != pv.Spec.ClaimRef.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup PV",
			fmt.Sprintf("PV %s ClaimRef UID changed", pv.Name),
		)
	}

	if claim.DeletionTimestamp == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup PV",
			fmt.Sprintf(
				"PV %s is still claimed by PVC %s/%s",
				pv.Name,
				pv.Spec.ClaimRef.Namespace,
				pv.Spec.ClaimRef.Name,
			),
		)
	}

	return nil
}

func reclaimedPVReleased(
	ctx context.Context,
	client kubernetes.Interface,
	ref v1alpha1.ObjectReference,
	sessionID, expectedRole string,
	uncheckpointedClaim *v1alpha1.ObjectReference,
) (bool, error) {
	current, err := client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, err
	}

	if !cleanupPVIdentityMatches(current, ref, sessionID, expectedRole, uncheckpointedClaim) {
		return false, domain.NewError(
			domain.ErrorConflict,
			"cleanup PV",
			fmt.Sprintf(
				"PV %s identity, ownership, or role changed while waiting for release",
				ref.Name,
			),
		)
	}

	if current.Status.Phase == corev1.VolumeReleased ||
		current.Status.Phase == corev1.VolumeAvailable {
		return true, nil
	}

	if current.Status.Phase != corev1.VolumeBound {
		return false, domain.NewError(
			domain.ErrorPrecondition,
			"cleanup PV",
			fmt.Sprintf(
				"PV %s phase %s must be Released or Available",
				current.Name,
				current.Status.Phase,
			),
		)
	}

	if current.Spec.ClaimRef == nil || current.Spec.ClaimRef.Namespace == "" ||
		current.Spec.ClaimRef.Name == "" ||
		current.Spec.ClaimRef.UID == "" {
		return false, domain.NewError(
			domain.ErrorPrecondition,
			"cleanup PV",
			fmt.Sprintf("PV %s phase %s has no ClaimRef", current.Name, current.Status.Phase),
		)
	}

	claim, claimErr := client.CoreV1().
		PersistentVolumeClaims(current.Spec.ClaimRef.Namespace).
		Get(ctx, current.Spec.ClaimRef.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(claimErr) {
		return false, nil
	}

	if claimErr != nil {
		return false, claimErr
	}

	if claim.UID != current.Spec.ClaimRef.UID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"cleanup PV",
			fmt.Sprintf("PV %s ClaimRef UID changed while waiting for release", ref.Name),
		)
	}

	if claim.DeletionTimestamp == nil {
		return false, domain.NewError(
			domain.ErrorPrecondition,
			"cleanup PV",
			fmt.Sprintf(
				"PV %s is still claimed by PVC %s/%s",
				current.Name,
				current.Spec.ClaimRef.Namespace,
				current.Spec.ClaimRef.Name,
			),
		)
	}

	return false, nil
}

func cleanupPVIdentityMatches(
	pv *corev1.PersistentVolume,
	ref v1alpha1.ObjectReference,
	sessionID, expectedRole string,
	uncheckpointedClaim *v1alpha1.ObjectReference,
) bool {
	if pv == nil || pv.UID != ref.UID {
		return false
	}

	if pv.Labels[kube.SessionKey] == sessionID &&
		pv.Labels[kube.ResourceRoleLabel] == expectedRole {
		return true
	}

	return uncheckpointedClaim != nil &&
		uncheckpointedDestinationPVMatches(pv, *uncheckpointedClaim)
}

func uncheckpointedDestinationPVMatches(
	pv *corev1.PersistentVolume,
	claim v1alpha1.ObjectReference,
) bool {
	if pv == nil || claim.Namespace == "" || claim.Name == "" || claim.UID == "" ||
		pv.Spec.ClaimRef == nil {
		return false
	}

	if pv.Labels[kube.ManagedByLabel] != "" || pv.Labels[kube.SessionKey] != "" ||
		pv.Labels[kube.ResourceRoleLabel] != "" ||
		pv.Annotations[kube.SessionKey] != "" {
		return false
	}

	if pv.Annotations[kube.OriginalPolicyAnnotation] != "" ||
		pv.Annotations[kube.PairedPVAnnotation] != "" ||
		pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		return false
	}

	return pv.Spec.ClaimRef.Namespace == claim.Namespace && pv.Spec.ClaimRef.Name == claim.Name &&
		pv.Spec.ClaimRef.UID == claim.UID
}

type reclaimVolume struct {
	uncheckpointed *v1alpha1.ObjectReference
	skipMissingPV  bool
	role           string
	pvc            v1alpha1.ObjectReference
	pv             v1alpha1.ObjectReference
	policy         corev1.PersistentVolumeReclaimPolicy
	metadata       v1alpha1.PVCMetadata
	delete         bool
}

func skipMissingReclaimPV(
	ctx context.Context,
	client kubernetes.Interface,
	v reclaimVolume,
) (bool, error) {
	if !v.skipMissingPV {
		return false, nil
	}

	_, err := client.CoreV1().PersistentVolumes().Get(ctx, v.pv.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	return false, err
}

func protectRetainedPVs(
	ctx context.Context,
	client kubernetes.Interface,
	volumes []reclaimVolume,
) error {
	for i := range volumes {
		v := &volumes[i]
		if v.delete || v.pv.Name == "" || v.policy == "" {
			continue
		}
		// A released PV must stay Retain after ownership is removed, even when
		// its original StorageClass policy was Delete and its PVC UID is recorded.
		if v.pvc.UID == "" {
			v.policy = corev1.PersistentVolumeReclaimRetain
			continue
		}

		pvc, err := client.CoreV1().
			PersistentVolumeClaims(v.pvc.Namespace).
			Get(ctx, v.pvc.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || (err == nil && pvc.DeletionTimestamp != nil) {
			v.policy = corev1.PersistentVolumeReclaimRetain
		} else if err != nil {
			return err
		}
	}

	return nil
}

func validateReclaimVolume(
	ctx context.Context,
	client kubernetes.Interface,
	sessionID string,
	v reclaimVolume,
	finalize bool,
) error {
	if skip, err := skipMissingReclaimPV(ctx, client, v); skip || err != nil {
		return err
	}

	if !v.delete && !finalize {
		return nil
	}

	if err := validateReclaimPVC(ctx, client, sessionID, v); err != nil {
		return err
	}

	return validateReclaimPV(ctx, client, sessionID, v)
}

func validateReclaimPVC(
	ctx context.Context,
	client kubernetes.Interface,
	sessionID string,
	v reclaimVolume,
) error {
	if v.pvc.UID != "" {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(v.pvc.Namespace).
			Get(ctx, v.pvc.Name, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}

		if err == nil {
			if pvc.UID != v.pvc.UID ||
				(pvc.Labels[kube.SessionKey] != "" && pvc.Labels[kube.SessionKey] != sessionID) ||
				(pvc.Annotations[kube.SessionKey] != "" && pvc.Annotations[kube.SessionKey] != sessionID) {
				return domain.NewError(
					domain.ErrorConflict,
					"cleanup",
					"PVC identity or ownership changed",
				)
			}

			if v.delete {
				if pvc.Labels[kube.SessionKey] != sessionID {
					return domain.NewError(
						domain.ErrorConflict,
						"cleanup",
						"PVC is no longer owned by this workflow",
					)
				}

				current, err := inspectPVCUnusedWithOperations(
					ctx, client,
					v.pvc,
					sessionID,
				)
				if err != nil {
					return err
				}

				if current != nil && current.UID != v.pvc.UID {
					return domain.NewError(
						domain.ErrorConflict,
						"cleanup",
						"PVC identity changed during consumer checks",
					)
				}
			}
		}
	}

	return nil
}

func validateReclaimPV(
	ctx context.Context,
	client kubernetes.Interface,
	sessionID string,

	v reclaimVolume,
) error {
	if v.pv.Name == "" {
		return nil
	}

	if v.policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			"PV "+v.pv.Name+" has no recorded reclaim policy",
		)
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, v.pv.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	uncheckpointed := v.uncheckpointed != nil &&
		uncheckpointedDestinationPVMatches(pv, *v.uncheckpointed) &&
		pv.UID == v.pv.UID
	if err := validateFinalizablePV(pv, v.pv, sessionID, v.policy); err != nil && !uncheckpointed {
		return err
	}

	if v.role != "" && pv.Labels[kube.SessionKey] == sessionID &&
		pv.Labels[kube.ResourceRoleLabel] != v.role {
		return domain.NewError(domain.ErrorConflict, "cleanup", "PV "+v.pv.Name+" role changed")
	}

	if v.delete && pv.Labels[kube.SessionKey] != sessionID && !uncheckpointed {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			"PV is no longer owned by this workflow",
		)
	}

	if pv.Spec.ClaimRef != nil && pv.Status.Phase == corev1.VolumeBound {
		if v.pvc.UID == "" || pv.Spec.ClaimRef.UID != v.pvc.UID ||
			pv.Spec.ClaimRef.Name != v.pvc.Name ||
			pv.Spec.ClaimRef.Namespace != v.pvc.Namespace {
			return domain.NewError(
				domain.ErrorConflict,
				"cleanup",
				"PV binding changed; refusing to reclaim another claim's storage",
			)
		}
	}

	return nil
}

func reclaimStorageVolume(
	ctx context.Context,
	client kubernetes.Interface,
	logger *slog.Logger,
	sessionID string,
	v reclaimVolume,
	finalize bool,
) error {
	if skip, err := skipMissingReclaimPV(ctx, client, v); skip || err != nil {
		return err
	}

	if !v.delete && !finalize {
		return nil
	}

	if err := validateReclaimVolume(ctx, client, sessionID, v, finalize); err != nil {
		return err
	}

	if v.delete {
		if v.pvc.UID != "" {
			if err := deleteManagedPVC(ctx, client, sessionID, v.pvc); err != nil {
				return err
			}
		}

		if v.pv.Name == "" {
			return nil
		}

		_, err := client.CoreV1().PersistentVolumes().Get(ctx, v.pv.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return err
		}

		return deleteReclaimedPV(
			ctx, client, logger,
			sessionID,
			v.pv,
			v.role,
			v.policy,
			v.uncheckpointed,
		)
	}

	if v.pvc.UID != "" {
		if err := kube.FinalizePVC(ctx, client, v.pvc, sessionID, v.metadata); err != nil {
			return err
		}
	}

	if v.pv.Name == "" {
		return nil
	}

	_, err := client.CoreV1().PersistentVolumes().Get(ctx, v.pv.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	return finalizeActivePV(ctx, client, sessionID, v.pv, v.policy)
}
