package app

import (
	"context"
	"reflect"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// MigrationCleanupOptions contains only policies that belong to migration.
type MigrationCleanupOptions struct {
	UnusedStoragePolicy string
	Finalize            bool
	DeleteSession       bool
}

func (m *ClusterMigrationExecutor) ValidateCleanup(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
	options MigrationCleanupOptions,
) error {
	_, _, _, err := m.prepareCleanup(ctx, object, options)
	return err
}

func (m *ClusterMigrationExecutor) Cleanup(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
	options MigrationCleanupOptions,
) error {
	if err := validateClusterMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(
		ctx,
		m.store,
		m.locker,
		m.storageNamespace,
		object,
		func(ctx context.Context) error {
			return m.cleanup(ctx, object, options)
		},
	)
}

func validateMigrationCleanupOptions(
	phase v1alpha1.WorkflowPhase,
	planned bool,
	options MigrationCleanupOptions,
) error {
	if err := domain.ValidateUnusedStoragePolicy(
		v1alpha1.UnusedStoragePolicy(options.UnusedStoragePolicy),
	); err != nil {
		return err
	}

	if options.DeleteSession && !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"migration cleanup",
			"deleting the session requires finalization",
		)
	}

	if planned && phase != domain.PhaseCompleted &&
		phase != domain.PhaseRolledBack &&
		phase != domain.PhaseAborted {
		return domain.NewError(
			domain.ErrorPrecondition,
			"migration cleanup",
			"migration is still active; abort or rollback before cleanup",
		)
	}

	return nil
}

func (m *ClusterMigrationExecutor) prepareCleanup(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
	options MigrationCleanupOptions,
) (*v1alpha1.ClusterMigration, []reclaimVolume, []v1alpha1.ObjectReference, error) {
	if err := validateClusterMigrationObject(object); err != nil {
		return nil, nil, nil, err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if err := validateMigrationCleanupOptions(
		phase,
		object.Status.Plan != nil,
		options,
	); err != nil {
		return nil, nil, nil, err
	}

	preview := object.DeepCopy()

	plan := preview.Status.Plan
	if plan == nil {
		return preview, nil, nil, nil
	}

	if len(preview.Status.Volumes) == 0 && phase == domain.PhaseAborted {
		for _, volume := range plan.Volumes {
			preview.Status.Volumes = append(
				preview.Status.Volumes,
				v1alpha1.ClusterMigrationVolumeStatus{
					ClusterVolumeReservationStatus: v1alpha1.ClusterVolumeReservationStatus{
						SourcePVCName: volume.SourcePVC.Name,
					},
				},
			)
		}
	}

	indexes := clusterMigrationVolumeIndexes(preview.Status.Volumes)

	volumes := make([]reclaimVolume, 0, len(plan.Volumes)*2)
	for _, planned := range plan.Volumes {
		index, ok := indexes[planned.SourcePVC.Name]
		if !ok {
			return nil, nil, nil, domain.NewError(
				domain.ErrorValidation,
				"migration cleanup",
				"migration volume checkpoint is missing",
			)
		}

		deleteUnused := domain.DeletesUnusedStorage(
			v1alpha1.UnusedStoragePolicy(options.UnusedStoragePolicy),
		)

		recovered, reclaimed, err := prepareMigrationReclaimVolume(
			ctx,
			m.client,
			object.Name,
			phase,
			string(plan.SourceNamespace),
			string(plan.TemporaryNamespace),
			planned,
			preview.Status.Volumes[index].ClusterVolumeReservationStatus,
			preview.Status.Volumes[index].Activation.ActivePVC,
			deleteUnused,
		)
		if err != nil {
			return nil, nil, nil, err
		}

		preview.Status.Volumes[index].ClusterVolumeReservationStatus = recovered

		volumes = append(volumes, reclaimed...)
	}

	pods, err := inventoryReservationPods(
		ctx,
		m.client,
		object.Name,
		[]string{string(plan.TemporaryNamespace)},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	if err := protectRetainedPVs(ctx, m.client, volumes); err != nil {
		return nil, nil, nil, err
	}

	for _, volume := range volumes {
		if err := validateReclaimVolume(
			ctx,
			m.client,
			object.Name,
			volume,
			options.Finalize,
		); err != nil {
			return nil, nil, nil, err
		}
	}

	return preview, volumes, pods, nil
}

func (m *ClusterMigrationExecutor) cleanup(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
	options MigrationCleanupOptions,
) error {
	preview, volumes, pods, err := m.prepareCleanup(ctx, object, options)
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(object.Status, preview.Status) {
		previous := object.Status.DeepCopy()

		object.Status = *preview.Status.DeepCopy()
		if err := persistCheckpoint(
			ctx,
			func(ctx context.Context) error { return m.store.Save(ctx, object) },
		); err != nil {
			object.Status = *previous
			return err
		}
	}

	if err := deleteReservationConsumers(ctx, m.client, pods); err != nil {
		return err
	}

	if plan := object.Status.Plan; plan != nil &&
		workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseAborted {
		indexes := clusterMigrationVolumeIndexes(object.Status.Volumes)
		for _, volume := range plan.Volumes {
			checkpoint := object.Status.Volumes[indexes[volume.SourcePVC.Name]]
			if checkpoint.Reserved || checkpoint.Activation.ActivePVC != nil {
				continue
			}

			if err := releaseSourceOwnership(
				ctx,
				m.client,
				object.Name,
				qualifiedResourceReference(volume.SourcePVC, string(plan.SourceNamespace)),
				qualifiedResourceReference(volume.SourcePV, ""),
				volume.SourceReclaimPolicy,
			); err != nil {
				return err
			}
		}
	}

	for _, volume := range volumes {
		if err := reclaimStorageVolume(
			ctx,
			m.client,
			m.config.Transfer.Logger,
			object.Name,
			volume,
			options.Finalize,
		); err != nil {
			return err
		}
	}

	if options.DeleteSession {
		if err := m.store.Delete(ctx, object); err != nil {
			return err
		}

		if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
			if err := held.lock.Delete(ctx); err != nil {
				return err
			}
		}
	}

	return nil
}
