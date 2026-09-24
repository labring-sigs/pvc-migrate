package app

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
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

// scanPlannedSourcePVCs probes every planned source volume and aggregates the
// loss signals: a deleted PVC always wins over a terminating one, so the scan
// keeps going after a terminating hit to look for a full deletion.
func scanPlannedSourcePVCs(
	ctx context.Context,
	client kubernetes.Interface,
	sourceNamespace string,
	volumes []v1alpha1.VolumeSpec,
) (plannedSourceScan, error) {
	scan := plannedSourceScan{}
	for _, volume := range volumes {
		if volume.SourcePVC.Name == "" {
			continue
		}

		probe, err := probeSourcePVC(
			ctx,
			client,
			qualifiedResourceReference(volume.SourcePVC, sourceNamespace),
		)
		if err != nil {
			return plannedSourceScan{}, err
		}

		if probe.Deleted {
			scan.Deleted = true
			return scan, nil
		}

		if probe.Terminating != nil && scan.Terminating == nil {
			scan.Terminating = probe.Terminating
		}
	}

	return scan, nil
}

// deletionSourceMissing reports whether a planned volume's source storage can
// no longer be fully verified while finalizing a deleted workflow: the PVC
// gone or terminating, or the PV gone or terminating. Only a deletion pass
// may skip the validations that re-verify the live source identity — a
// half-deleted source pair must not wedge the finalizer either.
func deletionSourceMissing(
	ctx context.Context,
	client kubernetes.Interface,
	sourceNamespace string,
	volume v1alpha1.VolumeSpec,
) (bool, error) {
	if !workflowDeletionInProgress(ctx) || volume.SourcePVC.Name == "" {
		return false, nil
	}

	probe, err := probeSourcePVC(
		ctx,
		client,
		qualifiedResourceReference(volume.SourcePVC, sourceNamespace),
	)
	if err != nil || probe.Deleted || probe.Terminating != nil {
		return probe.Deleted || probe.Terminating != nil, err
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			verifySourceStoragePhase,
			"read source PV "+volume.SourcePV.Name,
			err,
		)
	}

	return pv.DeletionTimestamp != nil, nil
}

// deletionDestinationSettling reports whether the staged destination pair is
// gone or terminating while finalizing a deleted workflow, so the deletion
// pass skips re-validating storage it is about to release anyway.
func deletionDestinationSettling(
	ctx context.Context,
	client kubernetes.Interface,
	binding kube.PVCTransferBindings,
) (bool, error) {
	if !workflowDeletionInProgress(ctx) {
		return false, nil
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(binding.DestinationPVC.Namespace).
		Get(ctx, binding.DestinationPVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			verifySourceStoragePhase,
			fmt.Sprintf(
				"read destination PVC %s/%s",
				binding.DestinationPVC.Namespace,
				binding.DestinationPVC.Name,
			),
			err,
		)
	}

	if pvc.DeletionTimestamp != nil {
		return true, nil
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, binding.DestinationPV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			verifySourceStoragePhase,
			"read destination PV "+binding.DestinationPV.Name,
			err,
		)
	}

	return pv.DeletionTimestamp != nil, nil
}

// deletionSourcePairSettling reports whether any planned volume's source
// pair is going away while finalizing a deleted workflow, so the deletion
// pass skips the source re-verification entirely.
func deletionSourcePairSettling(
	ctx context.Context,
	client kubernetes.Interface,
	sourceNamespace string,
	volumes []v1alpha1.VolumeSpec,
) (bool, error) {
	for _, volume := range volumes {
		settling, err := deletionSourceMissing(ctx, client, sourceNamespace, volume)
		if err != nil || settling {
			return settling, err
		}
	}

	return false, nil
}

// deletionValidationSkip reports whether the deletion pass should skip the
// reserved-volume re-validation because either side of the volume pair is
// going away. Non-deletion callers get false and validate strictly.
func deletionValidationSkip(
	ctx context.Context,
	client kubernetes.Interface,
	sourceNamespace string,
	volume v1alpha1.VolumeSpec,
	binding kube.PVCTransferBindings,
) (bool, error) {
	skip, err := deletionSourceMissing(ctx, client, sourceNamespace, volume)
	if err != nil || skip {
		return skip, err
	}

	return deletionDestinationSettling(ctx, client, binding)
}
