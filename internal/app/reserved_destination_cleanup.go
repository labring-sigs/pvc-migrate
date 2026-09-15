package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// prepareReservedDestination recovers storage identities without mutating live
// resources. The caller must persist the recovered checkpoint before cleanup.
func prepareReservedDestination(
	ctx context.Context,
	client kubernetes.Interface,
	id string,
	source, destination v1alpha1.ObjectReference,
	sourcePVUID types.UID,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
	deleteDestination bool,
) (v1alpha1.ClusterVolumeReservationStatus, reclaimVolume, error) {
	recovered, err := recoverReservationVolume(
		ctx,
		client,
		id,
		source,
		destination,
		checkpoint,
		true,
	)
	if err != nil {
		return checkpoint, reclaimVolume{}, err
	}

	if recovered.DestinationPV != nil && recovered.DestinationPV.UID == sourcePVUID {
		return checkpoint, reclaimVolume{}, domain.NewError(
			domain.ErrorConflict, "destination cleanup", "destination PV aliases source storage",
		)
	}

	volume := reclaimVolume{
		role:   kube.ResourceRoleDestination,
		policy: recovered.DestinationPolicy,
		delete: deleteDestination,
	}
	if recovered.DestinationPVC != nil {
		volume.pvc = *recovered.DestinationPVC
		if !recovered.Reserved {
			volume.uncheckpointed = recovered.DestinationPVC.DeepCopy()
		}
	}

	if recovered.DestinationPV != nil {
		volume.pv = *recovered.DestinationPV
	}

	return recovered, volume, nil
}
