package domain

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

// ValidateUnusedStoragePolicy accepts only the empty value (treated as Keep)
// or the explicit Keep/Delete spellings.
func ValidateUnusedStoragePolicy(policy v1alpha1.UnusedStoragePolicy) error {
	if policy != "" && policy != v1alpha1.UnusedStorageKeep && policy != v1alpha1.UnusedStorageDelete {
		return NewError(
			ErrorValidation,
			"unused storage policy",
			"unusedStoragePolicy must be Keep or Delete",
		)
	}

	return nil
}

// DeletesUnusedStorage reports whether unused storage identities should be
// deleted at terminal states. The empty value means Keep.
func DeletesUnusedStorage(policy v1alpha1.UnusedStoragePolicy) bool {
	return policy == v1alpha1.UnusedStorageDelete
}
