package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

type migrationRollbacker interface {
	RollbackPVC(ctx context.Context, workflowID string, bindings kube.PVCTransferBindings,
		desired *corev1.PersistentVolumeClaim, status *v1alpha1.ClusterVolumeActivationStatus, progress kube.ProgressFunc) error
}

// rollbackMigrationVolume publishes only saved checkpoints. In particular, a
// later failure transition must not persist a rejected rollback checkpoint.
func rollbackMigrationVolume(
	ctx context.Context,
	switcher migrationRollbacker,
	workflowID string,
	bindings kube.PVCTransferBindings,
	desired *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
	save func(context.Context) error,
) error {
	checkpoint := status.DeepCopy()

	return switcher.RollbackPVC(ctx, workflowID, bindings, desired, checkpoint, func() error {
		previous := status.DeepCopy()

		*status = *checkpoint.DeepCopy()
		if err := persistCheckpoint(ctx, save); err != nil {
			*status = *previous
			return err
		}

		return nil
	})
}
