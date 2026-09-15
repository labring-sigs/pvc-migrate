package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// prepareMigrationReclaimVolume resolves storage ownership for one terminal
// migration volume. The caller owns operation policy and checkpoint persistence.
func prepareMigrationReclaimVolume(
	ctx context.Context,
	client kubernetes.Interface,
	owner string,
	phase v1alpha1.WorkflowPhase,
	sourceNamespace, temporaryNamespace string,
	planned v1alpha1.VolumeSpec,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
	activePVC *v1alpha1.ObjectReference,
	sourcePolicy, destinationPolicy string,
) (v1alpha1.ClusterVolumeReservationStatus, []reclaimVolume, error) {
	source := qualifiedResourceReference(planned.SourcePVC, sourceNamespace)

	destination := reclaimVolume{
		role:   kube.ResourceRoleDestination,
		policy: checkpoint.DestinationPolicy,
		delete: destinationPolicy == "Delete",
	}
	if checkpoint.DestinationPVC != nil {
		destination.pvc = *checkpoint.DestinationPVC
	}

	if checkpoint.DestinationPV != nil {
		destination.pv = *checkpoint.DestinationPV
	}

	recovered := *checkpoint.DeepCopy()
	if phase == domain.PhaseAborted {
		var err error

		recovered, destination, err = prepareReservedDestination(
			ctx, client, owner, source,
			qualifiedResourceReference(planned.DestinationPVC, temporaryNamespace),
			planned.SourcePV.UID, checkpoint, destinationPolicy == "Delete",
		)
		if err != nil {
			return checkpoint, nil, err
		}
	}

	src := reclaimVolume{
		pv:            qualifiedResourceReference(planned.SourcePV, ""),
		policy:        planned.SourceReclaimPolicy,
		metadata:      planned.SourcePVCMetadata,
		skipMissingPV: phase == domain.PhaseAborted,
	}
	if phase == domain.PhaseCompleted {
		src.role = kube.ResourceRoleRollback

		src.delete = sourcePolicy == "Delete"
		if !src.delete {
			src.policy = corev1.PersistentVolumeReclaimRetain
		}

		destination.role = kube.ResourceRoleActive

		destination.metadata = planned.SourcePVCMetadata
		if activePVC != nil {
			destination.pvc = *activePVC
		}
	} else {
		src.pvc = source
		if activePVC != nil {
			src.pvc = *activePVC
		}

		if phase == domain.PhaseRolledBack {
			destination.role = kube.ResourceRoleRollback
		}
	}

	var volumes []reclaimVolume
	if phase == domain.PhaseAborted && !checkpoint.Reserved && activePVC == nil {
		if err := validateSourceOwnershipRelease(
			ctx,
			client,
			owner,
			source,
			src.pv,
			planned.SourceReclaimPolicy,
		); err != nil {
			return checkpoint, nil, err
		}
	} else {
		volumes = append(volumes, src)
	}

	if destination.pvc.Name != "" || destination.pv.Name != "" {
		volumes = append(volumes, destination)
	}

	return recovered, volumes, nil
}
