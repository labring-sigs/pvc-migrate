package app

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// Only the storage primitive uses qualified identities. Durable Migration
// checkpoints remain local references and reject out-of-namespace results.
func qualifiedActivationCheckpoint(
	status v1alpha1.VolumeActivationStatus,
	namespace string,
) v1alpha1.ClusterVolumeActivationStatus {
	return v1alpha1.ClusterVolumeActivationStatus{
		TemporaryPVCDeleted: status.TemporaryPVCDeleted,
		SourcePVCDeleted:    status.SourcePVCDeleted,
		DestinationReserved: status.DestinationReserved,
		ActivePVC:           qualifiedOptionalReference(status.ActivePVC, namespace),
		ActivatedAt:         status.ActivatedAt.DeepCopy(),
		RolledBackAt:        status.RolledBackAt.DeepCopy(),
	}
}

func localActivationCheckpoint(
	status v1alpha1.ClusterVolumeActivationStatus,
	namespace string,
) (v1alpha1.VolumeActivationStatus, error) {
	local := v1alpha1.VolumeActivationStatus{
		TemporaryPVCDeleted: status.TemporaryPVCDeleted,
		SourcePVCDeleted:    status.SourcePVCDeleted,
		DestinationReserved: status.DestinationReserved,
		ActivatedAt:         status.ActivatedAt.DeepCopy(),
		RolledBackAt:        status.RolledBackAt.DeepCopy(),
	}
	if status.ActivePVC != nil {
		if status.ActivePVC.Namespace != namespace {
			return local, domain.NewError(
				domain.ErrorConflict,
				"migration checkpoint",
				"active PVC is outside the workflow namespace",
			)
		}

		local.ActivePVC = localResourceReference(*status.ActivePVC)
	}

	return local, nil
}

func qualifiedOptionalReference(
	ref *v1alpha1.LocalResourceReference,
	namespace string,
) *v1alpha1.ObjectReference {
	if ref == nil {
		return nil
	}

	qualified := qualifiedResourceReference(*ref, namespace)

	return &qualified
}
