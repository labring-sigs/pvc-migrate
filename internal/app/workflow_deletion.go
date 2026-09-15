package app

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// workflowDeletionInProgress reports whether the current execution finalizes a
// deleted workflow. Deletion is the last convergence pass, so preconditions
// that would restore live state are relaxed when that state is already gone.
func workflowDeletionInProgress(ctx context.Context) bool {
	return ctx.Value(workflowDeletionContextKey{}) == true
}

// sourcePVCDeleted reports whether one recorded source PVC no longer exists.
func sourcePVCDeleted(
	ctx context.Context,
	client kubernetes.Interface,
	sourcePVC v1alpha1.ObjectReference,
) (bool, error) {
	_, err := client.CoreV1().
		PersistentVolumeClaims(sourcePVC.Namespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			verifySourceStoragePhase,
			fmt.Sprintf("read source PVC %s/%s", sourcePVC.Namespace, sourcePVC.Name),
			err,
		)
	}

	return false, nil
}

// deletedPlannedSourcePVC reports whether any planned source volume lost its
// PVC. A deletion pass that would re-verify or resume onto it can only fail:
// abort converges without the resume and cleanup releases the storage instead.
func deletedPlannedSourcePVC(
	ctx context.Context,
	client kubernetes.Interface,
	sourceNamespace string,
	volumes []v1alpha1.VolumeSpec,
) (bool, error) {
	for _, volume := range volumes {
		if volume.SourcePVC.Name == "" {
			continue
		}

		deleted, err := sourcePVCDeleted(
			ctx,
			client,
			qualifiedResourceReference(volume.SourcePVC, sourceNamespace),
		)
		if err != nil || deleted {
			return deleted, err
		}
	}

	return false, nil
}
