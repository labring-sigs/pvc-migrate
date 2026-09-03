package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// validateDeclarativeSourceVolumes verifies the planner snapshot before the
// controller starts a workflow. A user-authored CR can contain arbitrary
// cleanup metadata and PVC spec fields, so those fields must match the live
// source objects while they still exist. Later reconciles skip this check once
// the workflow has left Planned because migration stages may intentionally
// delete or replace the source PVC.
func (r *WorkflowReconciler) validateDeclarativeSourceVolumes(
	ctx context.Context,
	session *domain.Session,
) error {
	if session == nil || session.Status.Phase != domain.PhasePlanned ||
		len(session.Spec.Volumes) == 0 {
		return nil
	}

	if r == nil || r.kubeClient == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"controller volume snapshot",
			"Kubernetes client is not configured",
		)
	}

	resource, ok := domain.ControllerResourceForSession(session)
	if !ok {
		return domain.NewError(
			domain.ErrorValidation,
			"controller volume snapshot",
			"workflow resource type is not supported",
		)
	}

	expectedNamespace := session.Spec.SessionNamespace
	if resource.Cluster {
		expectedNamespace = session.Spec.SourceNamespace
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		if volume.SourcePVC.Namespace != expectedNamespace {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller volume snapshot",
				fmt.Sprintf(
					"source PVC %s/%s is outside the workflow namespace",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
			)
		}

		pvc, err := r.kubeClient.CoreV1().PersistentVolumeClaims(volume.SourcePVC.Namespace).
			Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller volume snapshot",
				fmt.Sprintf(
					"source PVC %s/%s no longer exists before execution",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
			)
		}

		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"controller volume snapshot",
				fmt.Sprintf(
					"read source PVC %s/%s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
				err,
			)
		}

		if pvc.UID != volume.SourcePVC.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"controller volume snapshot",
				fmt.Sprintf("source PVC %s/%s UID changed", pvc.Namespace, pvc.Name),
			)
		}

		pv, err := r.kubeClient.CoreV1().PersistentVolumes().
			Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"controller volume snapshot",
				fmt.Sprintf("source PV %s no longer exists before execution", volume.SourcePV.Name),
			)
		}

		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"controller volume snapshot",
				"read source PV "+volume.SourcePV.Name,
				err,
			)
		}

		if err := validateSourcePVIdentity(volume, pvc, pv); err != nil {
			return fmt.Errorf("volume %d: %w", index, err)
		}

		if err := validateSourcePVCSnapshot(
			volume,
			pvc,
			pv,
			!session.Spec.Operation().RebindsPVC(),
		); err != nil {
			return fmt.Errorf("volume %d: %w", index, err)
		}
	}

	return nil
}

func validateSourcePVIdentity(
	volume *domain.VolumeSpec,
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
) error {
	if pv.UID != volume.SourcePV.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"controller volume snapshot",
			fmt.Sprintf("source PV %s UID changed", pv.Name),
		)
	}

	if pvc.Spec.VolumeName != pv.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"controller volume snapshot",
			fmt.Sprintf(
				"source PVC %s/%s is bound to PV %s, expected %s",
				pvc.Namespace,
				pvc.Name,
				pvc.Spec.VolumeName,
				pv.Name,
			),
		)
	}

	claim := pv.Spec.ClaimRef
	if claim == nil || claim.Namespace != pvc.Namespace || claim.Name != pvc.Name ||
		claim.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"controller volume snapshot",
			fmt.Sprintf(
				"source PV %s claimRef does not identify PVC %s/%s",
				pv.Name,
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	return nil
}

func validateSourcePVCSnapshot(
	volume *domain.VolumeSpec,
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
	requireTransferLayout bool,
) error {
	if !apiequality.Semantic.DeepEqual(volume.SourcePVCSpec, pvc.Spec) {
		return domain.NewError(
			domain.ErrorConflict,
			"controller volume snapshot",
			fmt.Sprintf("source PVC %s/%s spec changed since planning", pvc.Namespace, pvc.Name),
		)
	}

	if !maps.Equal(volume.SourcePVCMetadata.Labels, pvc.Labels) ||
		!maps.Equal(
			volume.SourcePVCMetadata.Annotations,
			kube.PVCAnnotationsForRecreation(pvc.Annotations),
		) ||
		!apiequality.Semantic.DeepEqual(
			volume.SourcePVCMetadata.OwnerReferences,
			pvc.OwnerReferences,
		) {
		return domain.NewError(
			domain.ErrorConflict,
			"controller volume snapshot",
			fmt.Sprintf(
				"source PVC %s/%s metadata changed since planning",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != volume.SourceReclaimPolicy {
		return domain.NewError(
			domain.ErrorConflict,
			"controller volume snapshot",
			fmt.Sprintf("source PV %s reclaim policy changed since planning", pv.Name),
		)
	}

	if !requireTransferLayout {
		return nil
	}

	if volume.SourceCapacity == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"controller volume snapshot",
			fmt.Sprintf("source PV %s capacity snapshot is required", pv.Name),
		)
	}

	wantCapacity, err := resource.ParseQuantity(volume.SourceCapacity)
	if err != nil || wantCapacity.Sign() <= 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"controller volume snapshot",
			fmt.Sprintf(
				"source PV %s capacity snapshot %q is invalid",
				pv.Name,
				volume.SourceCapacity,
			),
		)
	}

	gotCapacity, ok := pv.Spec.Capacity[corev1.ResourceStorage]
	if !ok || gotCapacity.Cmp(wantCapacity) != 0 {
		return domain.NewError(
			domain.ErrorConflict,
			"controller volume snapshot",
			fmt.Sprintf("source PV %s capacity changed since planning", pv.Name),
		)
	}

	pvcVolumeMode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		pvcVolumeMode = *pvc.Spec.VolumeMode
	}

	if !slices.Equal(volume.AccessModes, pvc.Spec.AccessModes) ||
		volume.VolumeMode != pvcVolumeMode {
		return domain.NewError(
			domain.ErrorConflict,
			"controller volume snapshot",
			fmt.Sprintf(
				"source PVC %s/%s access mode or volume mode changed since planning",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	return nil
}
