package app

import (
	"context"
	"reflect"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (r *CopyExecutor) ValidateCleanup(
	ctx context.Context,
	object *v1alpha1.Copy,
	options CopyCleanupOptions,
) error {
	_, _, _, err := r.prepareCleanup(ctx, object, options)
	return err
}

func (r *CopyExecutor) Cleanup(
	ctx context.Context,
	object *v1alpha1.Copy,
	options CopyCleanupOptions,
) error {
	if err := validateCopyObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, r.store, r.locker, object.Namespace, object,
		func(ctx context.Context) error { return r.cleanup(ctx, object, options) })
}

func (r *CopyExecutor) prepareCleanup(
	ctx context.Context,
	object *v1alpha1.Copy,
	options CopyCleanupOptions,
) (*v1alpha1.Copy, []reclaimVolume, []v1alpha1.ObjectReference, error) {
	if err := validateCopyObject(object); err != nil {
		return nil, nil, nil, err
	}

	if err := domain.ValidateUnusedStoragePolicy(
		v1alpha1.UnusedStoragePolicy(options.UnusedStoragePolicy),
	); err != nil {
		return nil, nil, nil, err
	}

	if options.DeleteSession && !options.Finalize {
		return nil, nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			"copy cleanup",
			"deleting the session requires finalization",
		)
	}

	if object.Status.Plan != nil && object.Status.Phase != domain.PhaseWarmCopied &&
		object.Status.Phase != domain.PhaseAborted {
		return nil, nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			"copy cleanup",
			"copy is still active; abort before cleanup",
		)
	}

	preview := object.DeepCopy()

	plan := preview.Status.Plan
	if plan == nil {
		return preview, nil, nil, nil
	}

	if len(preview.Status.Volumes) == 0 {
		for _, volume := range plan.Volumes {
			preview.Status.Volumes = append(
				preview.Status.Volumes,
				v1alpha1.CopyVolumeStatus{SourcePVCName: volume.SourcePVC.Name},
			)
		}
	}

	// A completed copy keeps its deliverable; only an aborted copy has an
	// unused staged destination to reclaim.
	policy := plan.UnusedStoragePolicy
	if options.UnusedStoragePolicy != "" {
		policy = v1alpha1.UnusedStoragePolicy(options.UnusedStoragePolicy)
	}

	deleteUnused := domain.DeletesUnusedStorage(policy) &&
		object.Status.Phase == domain.PhaseAborted

	indexes := copyVolumeIndexes(preview.Status.Volumes)

	volumes := make([]reclaimVolume, 0, len(plan.Volumes))
	for _, volume := range plan.Volumes {
		source := qualifiedResourceReference(volume.SourcePVC, object.Namespace)
		if err := validateSourceOwnershipRelease(
			ctx,
			r.client,
			object.Name,
			source,
			qualifiedResourceReference(
				volume.SourcePV,
				"",
			),
			volume.SourceReclaimPolicy,
		); err != nil {
			return nil, nil, nil, err
		}

		index := indexes[volume.SourcePVC.Name]

		recovered, destination, err := prepareReservedDestination(
			ctx,
			r.client,
			object.Name,
			source,
			qualifiedResourceReference(volume.DestinationPVC, object.Namespace),
			volume.SourcePV.UID,
			qualifiedReservationCheckpoint(
				preview.Status.Volumes[index].VolumeReservationStatus,
				object.Namespace,
			),
			deleteUnused,
		)
		if err != nil {
			return nil, nil, nil, err
		}

		local, scopeErr := localReservationCheckpoint(recovered, object.Namespace)
		if scopeErr != nil {
			return nil, nil, nil, scopeErr
		}

		preview.Status.Volumes[index].VolumeReservationStatus = local

		volumes = append(volumes, destination)
	}

	if err := protectRetainedPVs(ctx, r.client, volumes); err != nil {
		return nil, nil, nil, err
	}

	pods, err := inventoryReservationPods(
		ctx,
		r.client,
		object.Name,
		[]string{object.Namespace},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, volume := range volumes {
		if err := validateReclaimVolume(
			ctx,
			r.client,
			object.Name,
			volume,
			options.Finalize,
		); err != nil {
			return nil, nil, nil, err
		}
	}

	return preview, volumes, pods, nil
}

func (r *CopyExecutor) cleanup(
	ctx context.Context,
	object *v1alpha1.Copy,
	options CopyCleanupOptions,
) error {
	preview, volumes, pods, err := r.prepareCleanup(ctx, object, options)
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(object.Status, preview.Status) {
		previous := object.Status.DeepCopy()

		object.Status = *preview.Status.DeepCopy()
		if err := persistCheckpoint(
			ctx,
			func(ctx context.Context) error { return r.store.Save(ctx, object) },
		); err != nil {
			object.Status = *previous
			return err
		}
	}

	if err := deleteReservationConsumers(ctx, r.client, pods); err != nil {
		return err
	}

	if plan := object.Status.Plan; plan != nil {
		indexes := copyVolumeIndexes(object.Status.Volumes)
		for _, volume := range plan.Volumes {
			if !options.Finalize && object.Status.Volumes[indexes[volume.SourcePVC.Name]].Reserved {
				continue
			}

			if err := releaseSourceOwnership(
				ctx,
				r.client,
				object.Name,
				qualifiedResourceReference(volume.SourcePVC, object.Namespace),
				qualifiedResourceReference(
					volume.SourcePV,
					"",
				),
				volume.SourceReclaimPolicy,
			); err != nil {
				return err
			}
		}
	}

	for _, volume := range volumes {
		if err := reclaimStorageVolume(
			ctx,
			r.client,
			r.config.Transfer.Logger,
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

		return r.store.Delete(ctx, object)
	}

	return nil
}
