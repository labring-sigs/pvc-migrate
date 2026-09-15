package app

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

// reserveVolumeCheckpoint owns resource checkpoint isolation. Workflow phases,
// namespace policy and error transitions are the operation's responsibility.
func reserveVolumeCheckpoint(
	ctx context.Context,
	reserver volumeReserver,
	request kube.ReservationRequest,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	sourceCapacity string,
	desired *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeReservationStatus,
	save func(context.Context) error,
) error {
	if err := checkpointFenceError(ctx); err != nil {
		return err
	}

	checkpoint := status.DeepCopy()
	err := reserver.ReserveVolume(
		ctx,
		request,
		sourcePVC,
		sourcePV,
		sourceCapacity,
		desired,
		checkpoint,
	)
	previous := status.DeepCopy()

	*status = *checkpoint.DeepCopy()
	if saveErr := persistCheckpoint(ctx, save); saveErr != nil {
		*status = *previous
		return errors.Join(err, saveErr)
	}

	if err != nil {
		return err
	}

	if !status.Reserved || status.DestinationPVC == nil || status.DestinationPV == nil ||
		status.DestinationPVC.UID == "" || status.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"reserve volume",
			"reserver did not produce a complete checkpoint",
		)
	}

	return nil
}
