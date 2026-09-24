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

// sourceTermination names a planned source PVC whose deletion was requested
// but has not settled. The claim still carries its identity, yet the
// deletionTimestamp is immutable, so the deletion can no longer be cancelled.
type sourceTermination struct {
	PVC   v1alpha1.ObjectReference
	Since metav1.Time
}

// plannedSourceScan aggregates the source-loss signals of one probe pass.
type plannedSourceScan struct {
	Deleted     bool
	Terminating *sourceTermination
}

// probeSourcePVC reads one source PVC once and classifies its deletion state:
// gone, terminating, or intact.
func probeSourcePVC(
	ctx context.Context,
	client kubernetes.Interface,
	sourcePVC v1alpha1.ObjectReference,
) (plannedSourceScan, error) {
	pvc, err := client.CoreV1().
		PersistentVolumeClaims(sourcePVC.Namespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return plannedSourceScan{Deleted: true}, nil
	}

	if err != nil {
		return plannedSourceScan{}, domain.WrapError(
			domain.ErrorKubernetes,
			verifySourceStoragePhase,
			fmt.Sprintf("read source PVC %s/%s", sourcePVC.Namespace, sourcePVC.Name),
			err,
		)
	}

	if pvc.DeletionTimestamp != nil {
		return plannedSourceScan{Terminating: &sourceTermination{
			PVC:   sourcePVC,
			Since: *pvc.DeletionTimestamp,
		}}, nil
	}

	return plannedSourceScan{}, nil
}

// sourcePVCDeleted reports whether one recorded source PVC no longer exists.
func sourcePVCDeleted(
	ctx context.Context,
	client kubernetes.Interface,
	sourcePVC v1alpha1.ObjectReference,
) (bool, error) {
	scan, err := probeSourcePVC(ctx, client, sourcePVC)
	return scan.Deleted, err
}

// scanPlannedSourcePVCs probes every planned source volume and reports the
// first loss signal; a deleted PVC wins over a terminating one.
func scanPlannedSourcePVCs(
	ctx context.Context,
	client kubernetes.Interface,
	sourceNamespace string,
	volumes []v1alpha1.VolumeSpec,
) (plannedSourceScan, error) {
	for _, volume := range volumes {
		if volume.SourcePVC.Name == "" {
			continue
		}

		scan, err := probeSourcePVC(
			ctx,
			client,
			qualifiedResourceReference(volume.SourcePVC, sourceNamespace),
		)
		if err != nil || scan.Deleted || scan.Terminating != nil {
			return scan, err
		}
	}

	return plannedSourceScan{}, nil
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
	scan, err := scanPlannedSourcePVCs(ctx, client, sourceNamespace, volumes)
	return scan.Deleted, err
}

// deletionSourceMissing reports whether a planned volume's source PVC is gone
// while finalizing a deleted workflow. Only a deletion pass may skip the
// validations that re-verify the live source identity.
func deletionSourceMissing(
	ctx context.Context,
	client kubernetes.Interface,
	sourceNamespace string,
	volume v1alpha1.VolumeSpec,
) (bool, error) {
	if !workflowDeletionInProgress(ctx) || volume.SourcePVC.Name == "" {
		return false, nil
	}

	return sourcePVCDeleted(
		ctx,
		client,
		qualifiedResourceReference(volume.SourcePVC, sourceNamespace),
	)
}
