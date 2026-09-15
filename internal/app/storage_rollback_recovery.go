package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// A successful API mutation may precede the status checkpoint. Recover from
// live storage identities without changing the persisted workflow during
// validation.
func validateUnrecordedRollbackStorage(
	ctx context.Context,
	client kubernetes.Interface,
	switcher volumeSwitcher,
	sessionID string,
	sourcePVC, sourcePV, destinationPV v1alpha1.ObjectReference,
	active *v1alpha1.ObjectReference,
) (bool, error) {
	pvc, err := client.CoreV1().PersistentVolumeClaims(sourcePVC.Namespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if active == nil || active.Name == "" || active.UID == "" {
			return false, nil
		}
		// Rollback reverses the retained pair: the deleted claim was on the
		// destination PV, and the original PV may already be reserved again.
		reverse := kube.PVCTransferBindings{
			SourcePVC:      *active,
			SourcePV:       destinationPV,
			DestinationPVC: sourcePVC,
			DestinationPV:  sourcePV,
		}

		return true, switcher.VerifyActivationRecovery(
			ctx,
			sessionID,
			[]kube.PVCTransferBindings{reverse},
		)
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			"read rollback PVC",
			err,
		)
	}

	if pvc.Spec.VolumeName != sourcePV.Name {
		return false, nil
	}

	ref := v1alpha1.ObjectReference{
		APIVersion:      corev1.SchemeGroupVersion.String(),
		Kind:            "PersistentVolumeClaim",
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}

	return true, verifyRollbackStorageVolume(
		ctx,
		client,
		sessionID,
		sourcePVC,
		sourcePV,
		&ref,
	)
}
