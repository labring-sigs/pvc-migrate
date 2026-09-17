package domain

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func ValidateReclaimPolicies(source, destination v1alpha1.PVReclaimPolicy) error {
	for _, field := range []struct {
		name  string
		value v1alpha1.PVReclaimPolicy
	}{
		{"sourcePVReclaimPolicy", source}, {"destinationPVCReclaimPolicy", destination},
	} {
		retain := corev1.PersistentVolumeReclaimRetain
		dele := corev1.PersistentVolumeReclaimDelete

		if field.value != "" && field.value != retain && field.value != dele {
			return NewError(
				ErrorValidation,
				"reclaim policy",
				field.name+" must be Retain or Delete",
			)
		}
	}

	return nil
}
