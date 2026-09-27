package app

import (
	"context"
	"reflect"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (m *MigrationExecutor) ValidateCleanup(
	ctx context.Context,
	object *v1alpha1.Migration,
	options MigrationCleanupOptions,
) error {
	_, _, _, err := m.prepareCleanup(ctx, object, options)
	return err
}

func (m *MigrationExecutor) Cleanup(
	ctx context.Context,
	object *v1alpha1.Migration,
	options MigrationCleanupOptions,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	if options.Finalize {
		ctx = context.WithValue(ctx, workflowFinalizeContextKey{}, true)
	}

	return withStoredWorkflowLock(
		ctx,
		m.store,
		m.locker,
		object.Namespace,
		object,
		func(ctx context.Context) error {
			return m.cleanup(ctx, object, options)
		},
	)
}

func (m *MigrationExecutor) prepareCleanup(
	ctx context.Context,
	object *v1alpha1.Migration,
	options MigrationCleanupOptions,
) (*v1alpha1.Migration, []reclaimVolume, []v1alpha1.ObjectReference, error) {
	if err := validateMigrationObject(object); err != nil {
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
				v1alpha1.MigrationVolumeStatus{
					VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
						SourcePVCName: volume.SourcePVC.Name,
					},
				},
			)
		}
	}

	indexes := migrationVolumeIndexes(preview.Status.Volumes)

	// The recorded plan policy applies unless the cleanup command overrides
	// it — identical to the copy and reservation families, and to what the
	// CLI flag help promises.
	policy := plan.UnusedStoragePolicy
	if options.UnusedStoragePolicy != "" {
		policy = v1alpha1.UnusedStoragePolicy(options.UnusedStoragePolicy)
	}

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

		deleteUnused := domain.DeletesUnusedStorage(policy)

		recovered, reclaimed, err := prepareMigrationReclaimVolume(
			ctx,
			m.client,
			object.Name,
			phase,
			object.Namespace,
			object.Namespace,
			planned,
			qualifiedReservationCheckpoint(
				preview.Status.Volumes[index].VolumeReservationStatus,
				object.Namespace,
			),
			qualifiedOptionalReference(
				preview.Status.Volumes[index].Activation.ActivePVC,
				object.Namespace,
			),
			deleteUnused,
		)
		if err != nil {
			return nil, nil, nil, err
		}

		local, err := localReservationCheckpoint(recovered, object.Namespace)
		if err != nil {
			return nil, nil, nil, err
		}

		preview.Status.Volumes[index].VolumeReservationStatus = local

		volumes = append(volumes, reclaimed...)
	}

	pods, err := inventoryReservationPods(
		ctx,
		m.client,
		object.Name,
		[]string{object.Namespace},
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

func (m *MigrationExecutor) cleanup(
	ctx context.Context,
	object *v1alpha1.Migration,
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
		indexes := migrationVolumeIndexes(object.Status.Volumes)
		for _, volume := range plan.Volumes {
			checkpoint := object.Status.Volumes[indexes[volume.SourcePVC.Name]]
			if checkpoint.Reserved || checkpoint.Activation.ActivePVC != nil {
				continue
			}

			if err := releaseSourceOwnership(
				ctx,
				m.client,
				object.Name,
				qualifiedResourceReference(volume.SourcePVC, object.Namespace),
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
		if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
			if err := held.lock.Delete(ctx); err != nil {
				return err
			}
		}

		return m.store.Delete(ctx, object)
	}

	return nil
}
