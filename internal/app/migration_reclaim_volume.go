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
//
// The in-use copy is always kept: the destination after Completed, the source
// after a rollback, and — with a source-identity safety check — the source
// after an abort. deleteUnused opts into deleting the other copy.
func prepareMigrationReclaimVolume(
	ctx context.Context,
	client kubernetes.Interface,
	owner string,
	phase v1alpha1.WorkflowPhase,
	sourceNamespace, temporaryNamespace string,
	planned v1alpha1.VolumeSpec,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
	activePVC *v1alpha1.ObjectReference,
	deleteUnused bool,
) (v1alpha1.ClusterVolumeReservationStatus, []reclaimVolume, error) {
	source := qualifiedResourceReference(planned.SourcePVC, sourceNamespace)

	destination := reclaimVolume{
		role:   kube.ResourceRoleDestination,
		policy: checkpoint.DestinationPolicy,
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
			planned.SourcePV.UID, checkpoint, deleteUnused,
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

		// The migrated destination is the in-use copy; the old source PV is
		// the unused one.
		src.delete = deleteUnused
		if !src.delete {
			src.policy = corev1.PersistentVolumeReclaimRetain
		}

		destination.role = kube.ResourceRoleActive
		destination.delete = false

		destination.metadata = planned.SourcePVCMetadata
		if activePVC != nil {
			destination.pvc = *activePVC
		}
	} else {
		// Failed, aborted, and rolled-back workflows keep the source: it is
		// the copy the workload can actually run on.
		src.delete = false
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
		// The source identity is gone or unverifiable — the staged
		// destination may be the only surviving copy, so it is retained
		// whatever the policy says.
		destination.delete = false

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
