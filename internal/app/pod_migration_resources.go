package app

import (
	"context"
	"fmt"
	"slices"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const openEBSLVMSharedMountCleanupTimeout = 10 * time.Second

// PodMigrationExecutorConfig supplies services for workload migration.
// Operation inputs and recovery checkpoints remain in the CRD.
type PodMigrationExecutorConfig struct {
	Storage       MigrationExecutorConfig
	SharedVolumes kube.OpenEBSLVMSharedVolumeManager
	Workloads     workloadController
}

type podMigrationResources struct {
	migrationResources
	sharedVolumes kube.OpenEBSLVMSharedVolumeManager
	workloads     workloadController
}

func newPodMigrationResources(
	client kubernetes.Interface, engine copyengine.Engine, config PodMigrationExecutorConfig,
) podMigrationResources {
	return podMigrationResources{
		migrationResources: newMigrationResources(client, engine, config.Storage),
		sharedVolumes:      config.SharedVolumes,
		workloads:          config.Workloads,
	}
}

func (m *podMigrationResources) restoreSharedMounts(
	ctx context.Context, owner string, namespaces []string,
	mounts *[]v1alpha1.SharedMountStatus, save func(context.Context) error,
) error {
	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	// Probe Pods must stop using a volume before its temporary shared setting is restored.
	if err := kube.CleanupSessionToolProbePods(ctx, m.client, owner, namespaces); err != nil {
		return err
	}

	if err := m.validateSharedMountRestoration(ctx, owner, *mounts); err != nil {
		return err
	}

	return kube.RestoreSharedMounts(
		ctx,
		m.sharedVolumes,
		owner,
		mounts,
		save,
		m.config.Transfer.Logger,
	)
}

func (m *podMigrationResources) validateSharedMountRestoration(
	ctx context.Context, owner string, mounts []v1alpha1.SharedMountStatus,
) error {
	if len(mounts) == 0 {
		return nil
	}

	if m.sharedVolumes == nil {
		return domain.NewError(domain.ErrorInternal, "restore OpenEBS LVM shared mount",
			"OpenEBS LVMVolume manager is required to restore workflow-managed shared mounts")
	}

	for _, mount := range mounts {
		if err := m.sharedVolumes.ValidateRestoreShared(ctx, owner, mount); err != nil {
			return err
		}
	}

	return nil
}

func (m *podMigrationResources) ensureDestinationMount(
	ctx context.Context,
	accessModes []corev1.PersistentVolumeAccessMode,
	concurrentConsumers int,
	pvc, pv v1alpha1.ObjectReference,
	allowShared bool,
) error {
	if concurrentConsumers <= 1 || !slices.Contains(accessModes, corev1.ReadWriteOnce) {
		return nil
	}

	current, err := m.client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"destination shared mount",
			"read destination PV "+pv.Name,
			err,
		)
	}

	if current.UID != pv.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"destination shared mount",
			"destination PV "+pv.Name+" UID changed",
		)
	}

	if current.Spec.CSI == nil || current.Spec.CSI.Driver != kube.OpenEBSLVMCSIDriver {
		return nil
	}

	if m.sharedVolumes == nil {
		return domain.NewError(domain.ErrorInternal, "destination shared mount",
			"OpenEBS LVMVolume manager is required for a multi-consumer RWO destination")
	}

	if !allowShared {
		return domain.NewError(
			domain.ErrorPrecondition,
			"destination shared mount",
			fmt.Sprintf(
				"destination PV %s is OpenEBS LVM and requires spec.shared=yes for %d concurrent consumers; recreate the migration with openebsLvmEnableShared enabled",
				pv.Name,
				concurrentConsumers,
			),
		)
	}

	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	_, err = m.sharedVolumes.EnsureShared(ctx, pvc, pv)

	return err
}
