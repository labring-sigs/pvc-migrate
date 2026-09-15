package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

type migrationActivator interface {
	ActivatePVC(ctx context.Context, workflowID string, bindings kube.PVCTransferBindings,
		desired *corev1.PersistentVolumeClaim, status *v1alpha1.ClusterVolumeActivationStatus, progress kube.ProgressFunc) error
}

// activateMigrationVolume checkpoints only activation state. The caller owns
// operation transitions; the switcher owns resource fencing and cutover order.
func activateMigrationVolume(
	ctx context.Context,
	client kubernetes.Interface,
	switcher migrationActivator,
	workflowID string,
	bindings kube.PVCTransferBindings,
	desired *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
	save func(context.Context) error,
) error {
	if status.ActivatedAt != nil {
		return verifyActiveStorageVolume(ctx, client, workflowID,
			bindings.SourcePVC, bindings.DestinationPV, status.ActivePVC)
	}

	checkpoint := status.DeepCopy()

	return switcher.ActivatePVC(ctx, workflowID, bindings, desired, checkpoint, func() error {
		previous := status.DeepCopy()

		*status = *checkpoint.DeepCopy()
		if err := persistCheckpoint(ctx, save); err != nil {
			*status = *previous
			return err
		}

		return nil
	})
}
