package app

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func migrationActivationAdmission(
	namespace string,
	volume v1alpha1.VolumeSpec,
	sourceExists bool,
) (kube.PVCAdmissionChange, error) {
	quantity, err := resource.ParseQuantity(volume.Capacity)
	if err != nil || quantity.Sign() <= 0 {
		return kube.PVCAdmissionChange{}, domain.NewError(
			domain.ErrorValidation,
			"migration activation",
			"destination capacity must be positive",
		)
	}

	sourceClass := ""
	if volume.SourcePVCSpec.StorageClassName != nil {
		sourceClass = *volume.SourcePVCSpec.StorageClassName
	}

	attributes := kube.RequestedVolumeAttributesClassNames(volume.SourcePVCSpec)

	return kube.PVCAdmissionChange{
		Namespace:                           namespace,
		Name:                                volume.SourcePVC.Name,
		RequestedStorage:                    quantity,
		RequestedStorageClass:               volume.StorageClass,
		Existing:                            sourceExists,
		ExistingUID:                         volume.SourcePVC.UID,
		ExistingStorage:                     volume.SourcePVCSpec.Resources.Requests[corev1.ResourceStorage],
		ExistingStorageClass:                sourceClass,
		RequestedVolumeAttributesClassNames: attributes,
		ExistingVolumeAttributesClassNames:  attributes,
	}, nil
}
