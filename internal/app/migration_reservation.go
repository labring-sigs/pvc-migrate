package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

// validateReservationVolume checks one storage binding without receiving the
// operation's lifecycle, workload or other volume checkpoints.
func (m *migrationResources) validateReservationVolume(
	ctx context.Context,
	request kube.ReservationRequest,
	sourceNamespace, destinationNamespace string,
	volume v1alpha1.VolumeSpec,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
	skipSourceUsageCheck bool,
) error {
	source := qualifiedResourceReference(volume.SourcePVC, sourceNamespace)

	pv := qualifiedResourceReference(volume.SourcePV, "")
	if err := kube.VerifyVolumeShrinkUsage(
		ctx, m.config.VolumeUsageReader, m.config.Transfer.Logger,
		source, pv, volume.SourceCapacity, volume.Capacity,
		domain.SourceTransferPath(volume.TransferScope), skipSourceUsageCheck,
	); err != nil {
		return err
	}

	desired, err := reservationManifest(
		qualifiedResourceReference(volume.DestinationPVC, destinationNamespace),
		volume.Capacity, volume.StorageClass, volume.VolumeMode, volume.AccessModes,
	)
	if err != nil {
		return err
	}

	return m.reserver.ValidateVolumeReservation(
		ctx, request, source, pv, volume.SourceCapacity, desired, checkpoint,
	)
}
