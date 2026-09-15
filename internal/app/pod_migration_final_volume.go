package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func podFinalSyncPhase(phase v1alpha1.WorkflowPhase) bool {
	return phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced
}

func (m *podMigrationResources) validatePodFinalVolume(
	ctx context.Context,
	owner, targetNode, image string,
	volume v1alpha1.VolumeSpec,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
	binding kube.PVCTransferBindings,
	skipUsageCheck bool,
) error {
	if err := kube.VerifyVolumeShrinkUsage(
		ctx,
		m.config.VolumeUsageReader,
		m.config.Transfer.Logger,
		binding.SourcePVC,
		binding.SourcePV,
		volume.SourceCapacity,
		volume.Capacity,
		domain.SourceTransferPath(volume.TransferScope),
		skipUsageCheck,
	); err != nil {
		return err
	}

	return m.validateReservedVolume(ctx, owner, targetNode, image, binding, volume, checkpoint)
}
