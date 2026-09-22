package app

import (
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func migrationPVCManifest(
	workflowID string,
	sourcePVC, sourcePV, destinationPV v1alpha1.ObjectReference,
	sourceSpec corev1.PersistentVolumeClaimSpec,
	metadata v1alpha1.PVCMetadata,
	storageClass, capacity string,
) (*corev1.PersistentVolumeClaim, error) {
	quantity, err := resource.ParseQuantity(capacity)
	if err != nil || quantity.Sign() <= 0 {
		return nil, domain.NewError(domain.ErrorValidation, "active PVC",
			fmt.Sprintf("destination capacity %q must be a positive quantity", capacity))
	}

	pvc := kube.BoundPVCManifest(workflowID, sourcePVC, destinationPV.Name, sourceSpec, metadata)

	pvc.Spec.StorageClassName = &storageClass
	if pvc.Spec.Resources.Requests == nil {
		pvc.Spec.Resources.Requests = corev1.ResourceList{}
	}

	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = quantity
	pvc.Annotations[kube.RollbackPVAnnotation] = sourcePV.Name

	return pvc, nil
}
