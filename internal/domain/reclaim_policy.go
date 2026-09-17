package domain

import (
	corev1 "k8s.io/api/core/v1"
)

func ValidateReclaimPolicies(source, destination string) error {
	for _, field := range []struct{ name, value string }{
		{"sourcePVReclaimPolicy", source}, {"destinationPVCReclaimPolicy", destination},
	} {
		retain := string(corev1.PersistentVolumeReclaimRetain)
		dele := string(corev1.PersistentVolumeReclaimDelete)

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
