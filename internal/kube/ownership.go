package kube

import (
	"context"
	"fmt"
	"maps"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

func markPVSession(labels map[string]string, sessionID, role string) (changed bool) {
	for _, label := range []struct{ key, value string }{
		{ManagedByLabel, ManagedByValue},
		{SessionKey, sessionID},
		{ResourceRoleLabel, role},
	} {
		if labels[label.key] != label.value {
			labels[label.key] = label.value
			changed = true
		}
	}

	return changed
}

func AcquirePVC(
	ctx context.Context,
	client kubernetes.Interface,
	ref domain.ObjectReference,
	sessionID string,
) error {
	if err := validatePVCOwnershipInput(ref, sessionID, "acquire PVC"); err != nil {
		return err
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if pvc.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"acquire PVC",
				fmt.Sprintf("PVC %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		owner := pvc.Annotations[SessionKey]
		if owner != "" && owner != sessionID {
			return domain.NewError(
				domain.ErrorConflict,
				"acquire PVC",
				fmt.Sprintf("PVC %s/%s belongs to session %s", ref.Namespace, ref.Name, owner),
			)
		}

		if owner == sessionID {
			return nil
		}

		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}

		pvc.Annotations[SessionKey] = sessionID
		_, err = client.CoreV1().
			PersistentVolumeClaims(ref.Namespace).
			Update(ctx, pvc, metav1.UpdateOptions{})

		return err
	})
	if err == nil {
		return nil
	}

	if domain.CategoryOf(err) == domain.ErrorConflict {
		return err
	}

	if apierrors.IsConflict(err) {
		return domain.WrapError(
			domain.ErrorConflict,
			"acquire PVC",
			"PVC changed concurrently",
			err,
		)
	}

	return domain.WrapError(
		domain.ErrorKubernetes,
		"acquire PVC",
		fmt.Sprintf("annotate %s/%s", ref.Namespace, ref.Name),
		err,
	)
}

func ReleasePVC(
	ctx context.Context,
	client kubernetes.Interface,
	ref domain.ObjectReference,
	sessionID string,
) error {
	if err := validatePVCOwnershipInput(ref, sessionID, "release PVC"); err != nil {
		return err
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return err
		}

		if pvc.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"release PVC",
				fmt.Sprintf("PVC %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		if pvc.Annotations[SessionKey] != sessionID {
			return nil
		}

		delete(pvc.Annotations, SessionKey)
		_, err = client.CoreV1().
			PersistentVolumeClaims(ref.Namespace).
			Update(ctx, pvc, metav1.UpdateOptions{})

		return err
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"release PVC",
			fmt.Sprintf("update %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	return nil
}

func FinalizePVC(
	ctx context.Context,
	client kubernetes.Interface,
	ref domain.ObjectReference,
	sessionID string,
	original domain.PVCMetadata,
) error {
	if err := validatePVCOwnershipInput(ref, sessionID, "finalize PVC"); err != nil {
		return err
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return err
		}

		if pvc.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"finalize PVC",
				fmt.Sprintf("PVC %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		for _, owner := range []string{pvc.Annotations[SessionKey], pvc.Labels[SessionKey]} {
			if owner != "" && owner != sessionID {
				return domain.NewError(
					domain.ErrorConflict,
					"finalize PVC",
					fmt.Sprintf("PVC %s/%s belongs to session %s", ref.Namespace, ref.Name, owner),
				)
			}
		}

		if pvc.Labels == nil {
			pvc.Labels = map[string]string{}
		}

		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}

		delete(pvc.Labels, ManagedByLabel)
		delete(pvc.Labels, SessionKey)
		delete(pvc.Labels, ResourceRoleLabel)

		maps.Copy(pvc.Labels, original.Labels)

		delete(pvc.Annotations, SessionKey)
		delete(pvc.Annotations, RollbackPVAnnotation)
		delete(pvc.Annotations, SourcePVAnnotation)
		delete(pvc.Annotations, SourcePVCUIDAnnotation)

		maps.Copy(pvc.Annotations, original.Annotations)

		pvc.OwnerReferences = append([]metav1.OwnerReference(nil), original.OwnerReferences...)
		_, err = client.CoreV1().
			PersistentVolumeClaims(ref.Namespace).
			Update(ctx, pvc, metav1.UpdateOptions{})

		return err
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"finalize PVC",
			fmt.Sprintf("update %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	return nil
}

func validatePVCOwnershipInput(ref domain.ObjectReference, sessionID, operation string) error {
	if ref.Namespace == "" || ref.Name == "" || ref.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			operation,
			"PVC namespace, name, and UID are required",
		)
	}

	if sessionID == "" {
		return domain.NewError(domain.ErrorValidation, operation, "session ID is required")
	}

	return nil
}
