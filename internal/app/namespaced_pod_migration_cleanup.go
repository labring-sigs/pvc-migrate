package app

import (
	"context"
	"reflect"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (m *PodMigrationExecutor) ValidateCleanup(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	options MigrationCleanupOptions,
) error {
	_, _, _, err := m.prepareCleanup(ctx, object, options)
	return err
}

func (m *PodMigrationExecutor) Cleanup(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	options MigrationCleanupOptions,
) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
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

func (m *PodMigrationExecutor) prepareCleanup(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	options MigrationCleanupOptions,
) (*v1alpha1.PodMigration, []reclaimVolume, []v1alpha1.ObjectReference, error) {
	if err := validatePodMigrationObject(object); err != nil {
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

	if err := m.validateSharedMountRestoration(
		ctx,
		object.Name,
		object.Status.OpenEBSLVMSharedMounts,
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
				v1alpha1.PodMigrationVolumeStatus{
					VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
						SourcePVCName: volume.SourcePVC.Name,
					},
				},
			)
		}
	}

	indexes := podMigrationVolumeIndexes(preview.Status.Volumes)

	volumes := make([]reclaimVolume, 0, len(plan.Volumes)*2)
	for _, planned := range plan.Volumes {
		index, ok := indexes[planned.SourcePVC.Name]
		if !ok {
			return nil, nil, nil, domain.NewError(
				domain.ErrorValidation,
				"pod migration cleanup",
				"migration volume checkpoint is missing",
			)
		}

		policy := migrationSourceCleanupPolicy(
			phase,
			plan.SourcePVReclaimPolicy,
			v1alpha1.PVReclaimPolicy(options.SourcePVReclaimPolicy),
		)

		destinationPolicy := plan.DestinationPVCReclaimPolicy
		if options.DestinationPVCReclaimPolicy != "" {
			destinationPolicy = v1alpha1.PVReclaimPolicy(options.DestinationPVCReclaimPolicy)
		}

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
			policy,
			destinationPolicy,
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

func (m *PodMigrationExecutor) cleanup(
	ctx context.Context,
	object *v1alpha1.PodMigration,
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

	if plan := object.Status.Plan; plan != nil {
		if err := releaseWorkloadMarkers(
			ctx, m.client, object.Name, plan.Workload, object.Namespace,
		); err != nil {
			return err
		}
	}

	if plan := object.Status.Plan; plan != nil {
		if err := m.restoreSharedMounts(ctx, object.Name, []string{object.Namespace},
			&object.Status.OpenEBSLVMSharedMounts,
			func(ctx context.Context) error { return m.store.Save(ctx, object) }); err != nil {
			return err
		}
	}

	if plan := object.Status.Plan; plan != nil &&
		workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseAborted {
		indexes := podMigrationVolumeIndexes(object.Status.Volumes)
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
