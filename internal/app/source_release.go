package app

import (
	"context"
	"errors"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

func releaseSourceOwnership(
	ctx context.Context,
	client kubernetes.Interface,
	workflowID string,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	originalPolicy corev1.PersistentVolumeReclaimPolicy,
) error {
	if sourcePVC.Name == "" || sourcePV.Name == "" {
		return nil
	}

	if err := validateSourceOwnershipRelease(
		ctx,
		client,
		workflowID,
		sourcePVC,
		sourcePV,
		originalPolicy,
	); err != nil {
		return err
	}

	if err := kube.ReleasePVC(ctx, client, sourcePVC, workflowID); err != nil {
		return err
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, sourcePV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"read source PV "+sourcePV.Name,
			err,
		)
	}

	if pv.UID != sourcePV.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PV %s identity changed", pv.Name),
		)
	}

	if pv.Labels[kube.SessionKey] != workflowID {
		return nil
	}

	if role := pv.Labels[kube.ResourceRoleLabel]; role != kube.ResourceRoleSource {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PV %s has unexpected session role %q", pv.Name, role),
		)
	}

	retained := []reclaimVolume{{pvc: sourcePVC, pv: sourcePV, policy: originalPolicy}}
	if err := protectRetainedPVs(ctx, client, retained); err != nil {
		return err
	}

	return finalizeActivePV(ctx, client, workflowID, sourcePV, retained[0].policy)
}

// validateSourceOwnershipRelease permits source storage that was never acquired
// or has already been released. Owned storage must retain its recorded binding.
func validateSourceOwnershipRelease(
	ctx context.Context,
	client kubernetes.Interface,
	workflowID string,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	originalPolicy corev1.PersistentVolumeReclaimPolicy,
) error {
	if sourcePVC.Name == "" || sourcePV.Name == "" {
		return nil
	}

	pvc, pvcErr := client.CoreV1().
		PersistentVolumeClaims(sourcePVC.Namespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if pvcErr != nil && !apierrors.IsNotFound(pvcErr) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("read source PVC %s/%s", sourcePVC.Namespace, sourcePVC.Name),
			pvcErr,
		)
	}

	if pvcErr == nil && pvc.UID != sourcePVC.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PVC %s/%s identity changed", pvc.Namespace, pvc.Name),
		)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, sourcePV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"read source PV "+sourcePV.Name,
			err,
		)
	}

	if pv.UID != sourcePV.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PV %s identity changed", pv.Name),
		)
	}

	if pv.Labels[kube.SessionKey] != workflowID {
		return nil
	}

	if pvcErr == nil {
		for _, owner := range []string{pvc.Labels[kube.SessionKey], pvc.Annotations[kube.SessionKey]} {
			if owner != "" && owner != workflowID {
				return domain.NewError(
					domain.ErrorConflict,
					"cleanup",
					"source PVC ownership changed",
				)
			}
		}

		if pvc.Spec.VolumeName != "" && pvc.Spec.VolumeName != sourcePV.Name {
			return domain.NewError(domain.ErrorConflict, "cleanup", "source PVC binding changed")
		}
	}

	if pv.Status.Phase == corev1.VolumeBound &&
		(pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != sourcePVC.UID ||
			pv.Spec.ClaimRef.Name != sourcePVC.Name || pv.Spec.ClaimRef.Namespace != sourcePVC.Namespace) {
		return domain.NewError(domain.ErrorConflict, "cleanup", "source PV binding changed")
	}

	if role := pv.Labels[kube.ResourceRoleLabel]; role != kube.ResourceRoleSource {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PV %s has unexpected session role %q", pv.Name, role),
		)
	}

	if !validReclaimPolicy(originalPolicy) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			fmt.Sprintf("source PV %s has no recorded reclaim policy", pv.Name),
		)
	}

	return nil
}

func finalizeActivePV(
	ctx context.Context,
	client kubernetes.Interface,
	sessionID string,
	ref v1alpha1.ObjectReference,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize active PV",
			fmt.Sprintf("PV %s has no recorded reclaim policy", ref.Name),
		)
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		pv, err := client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		role := pv.Labels[kube.ResourceRoleLabel]
		if pv.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"finalize active PV",
				fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
			)
		}

		if pv.Labels[kube.SessionKey] == "" && role == "" &&
			pv.Spec.PersistentVolumeReclaimPolicy == policy &&
			pv.Annotations[kube.OriginalPolicyAnnotation] == "" {
			return nil
		}

		if pv.Labels[kube.SessionKey] != sessionID ||
			(role != kube.ResourceRoleActive && role != kube.ResourceRoleSource && role != kube.ResourceRoleRename && role != kube.ResourceRoleDestination && role != kube.ResourceRoleRollback) {
			return domain.NewError(
				domain.ErrorConflict,
				"finalize active PV",
				fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
			)
		}

		pv.Spec.PersistentVolumeReclaimPolicy = policy
		delete(pv.Labels, kube.SessionKey)
		delete(pv.Labels, kube.ResourceRoleLabel)

		if pv.Labels[kube.ManagedByLabel] == kube.ManagedByValue {
			delete(pv.Labels, kube.ManagedByLabel)
		}

		delete(pv.Annotations, kube.OriginalPolicyAnnotation)
		delete(pv.Annotations, kube.PairedPVAnnotation)

		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		_, err = client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

		return errors.Join(err, checkpointFenceError(ctx))
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"finalize active PV",
			"update PV "+ref.Name,
			err,
		)
	}

	return nil
}
