package app

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *MigrationExecutor) FinalSync(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error { return m.finalSync(ctx, object) })
}

func (m *MigrationExecutor) ValidateFinalSync(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	if err := validateMigrationCopyRetry(object.Status.FailureReason); err != nil {
		return err
	}

	if !migrationFinalSyncPhase(workflowResumePhase(object.Status.WorkflowStatus)) ||
		object.Status.Plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"migration final sync",
			"migration must have reserved storage before final sync",
		)
	}

	plan := object.Status.Plan
	indexes := migrationVolumeIndexes(object.Status.Volumes)

	bindings := make([]kube.PVCTransferBindings, 0, len(plan.Volumes))
	for _, volume := range plan.Volumes {
		checkpoint := object.Status.Volumes[indexes[volume.SourcePVC.Name]].VolumeReservationStatus

		binding := namespacedMigrationBindings(object.Namespace, volume, checkpoint)
		if err := kube.VerifyVolumeShrinkUsage(
			ctx,
			m.config.VolumeUsageReader,
			m.config.Transfer.Logger,
			binding.SourcePVC,
			binding.SourcePV,
			volume.SourceCapacity,
			volume.Capacity,
			domain.SourceTransferPath(
				volume.TransferScope,
			),
			plan.SkipSourceUsageCheck,
		); err != nil {
			return err
		}

		if err := m.validateReservedVolume(
			ctx,
			object.Name,
			plan.TargetNode,
			plan.ToolImage,
			binding,
			volume,
			qualifiedReservationCheckpoint(checkpoint, object.Namespace),
		); err != nil {
			return err
		}

		bindings = append(bindings, binding)
	}

	return m.switcher.VerifyVolumesOfflineForSession(ctx, object.Name, bindings)
}

func (m *MigrationExecutor) finalSync(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if !migrationFinalSyncPhase(workflowResumePhase(object.Status.WorkflowStatus)) ||
		object.Status.Plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"migration final sync",
			"migration phase cannot run final sync",
		)
	}

	if err := validateMigrationCopyRetry(object.Status.FailureReason); err != nil {
		return err
	}

	if err := m.cleanupInterruptedFinalSync(ctx, object); err != nil {
		return err
	}

	if err := m.ValidateFinalSync(ctx, object); err != nil {
		return err
	}

	plan := object.Status.Plan

	probes, err := m.finalSyncProbes(ctx, object.Name, object.Namespace, plan)
	if err != nil {
		return err
	}

	previous := object.Status.DeepCopy()
	if object.Status.Phase == domain.PhaseFinalSynced {
		for i := range object.Status.Volumes {
			object.Status.Volumes[i].Sync.FinalCompletedAt = nil
			object.Status.Volumes[i].Sync.ChecksumVerified = false
		}
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseFinalSyncing,
		"running offline migration final sync",
	); err != nil {
		object.Status = *previous
		return err
	}

	indexes := migrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Sync.FinalCompletedAt != nil {
			continue
		}

		binding := namespacedMigrationBindings(
			object.Namespace,
			volume,
			status.VolumeReservationStatus,
		)
		request := copyengine.Request{
			SessionID:   object.Name,
			ToolImage:   plan.ToolImage,
			Source:      binding.SourcePVC,
			Destination: binding.DestinationPVC,
			SourcePath: domain.SourceTransferPath(
				volume.TransferScope,
			),
			DestinationPath: domain.DestinationTransferPath(volume.TransferScope),
			IgnoreSizes: destinationCapacityIsSmaller(
				volume.SourceCapacity,
				volume.Capacity,
			),
			Strategies:            plan.Strategies,
			VerifyChecksum:        plan.VerifyChecksum,
			DeleteExtraneousFiles: plan.DeleteExtraneous,
			Mode:                  copyengine.ModeFinal,
		}

		save := func(ctx context.Context) error { return m.store.Save(ctx, object) }
		if err := m.transfer.copyWithRetry(ctx, request, plan.SourceNode, plan.TargetNode, "",
			&status.Sync.Attempts, &status.Sync.LastError, probes, save,
			func(ctx context.Context) error {
				if err := m.validateReservedVolume(
					ctx,
					object.Name,
					plan.TargetNode,
					plan.ToolImage,
					binding,
					volume,
					qualifiedReservationCheckpoint(
						status.VolumeReservationStatus,
						object.Namespace,
					),
				); err != nil {
					return err
				}

				return m.switcher.VerifyVolumeOffline(ctx, binding)
			}, func(context.Context) (bool, error) { return false, nil }); err != nil {
			return m.fail(ctx, object, err)
		}

		if err := checkpointFinalCopy(
			ctx,
			&status.Sync.FinalCompletedAt,
			&status.Sync.ChecksumVerified,
			&status.Sync.LastError,
			plan.VerifyChecksum,
			m.now(),
			save,
		); err != nil {
			return err
		}
	}

	return m.transition(
		ctx,
		object,
		domain.PhaseFinalSynced,
		"offline migration final sync completed",
	)
}

func (m *MigrationExecutor) cleanupInterruptedFinalSync(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	plan := object.Status.Plan

	indexes := migrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		index, exists := indexes[volume.SourcePVC.Name]
		if !exists {
			continue
		}

		status := object.Status.Volumes[index]
		if status.Sync.Attempts == 0 || status.Sync.FinalCompletedAt != nil {
			continue
		}

		binding := namespacedMigrationBindings(
			object.Namespace,
			volume,
			status.VolumeReservationStatus,
		)
		if err := m.validateReservedVolume(
			ctx,
			object.Name,
			plan.TargetNode,
			plan.ToolImage,
			binding,
			volume,
			qualifiedReservationCheckpoint(status.VolumeReservationStatus, object.Namespace),
		); err != nil {
			return err
		}

		request := copyengine.CleanupRequest{
			SessionID:            object.Name,
			Source:               binding.SourcePVC,
			DestinationNamespace: binding.DestinationPVC.Namespace,
			Mode:                 copyengine.ModeFinal,
			Attempt:              status.Sync.Attempts,
			Strategies:           slices.Clone(plan.Strategies),
			KubeconfigPath:       m.config.Transfer.KubeconfigPath,
			Context:              m.config.Transfer.Context,
		}
		if err := m.transfer.copier.Cleanup(ctx, request); err != nil {
			return err
		}

		if err := m.transfer.cleanupCopyToolPods(ctx, binding.SourcePVC, binding.DestinationPVC,
			copyengine.OperationID(request.AttemptIdentity)); err != nil {
			return err
		}
	}

	return nil
}

func (m *MigrationExecutor) finalSyncProbes(
	ctx context.Context,
	id, namespace string,
	plan *v1alpha1.MigrationPlan,
) ([]kube.ToolImageProbeResult, error) {
	return m.transferFinalSyncProbes(
		ctx,
		id,
		namespace,
		namespace,
		plan.SourceNode,
		plan.TargetNode,
		plan.ToolImage,
		plan.Strategies,
		plan.Volumes,
	)
}

func namespacedMigrationBindings(
	namespace string,
	volume v1alpha1.VolumeSpec,
	checkpoint v1alpha1.VolumeReservationStatus,
) kube.PVCTransferBindings {
	return plannedMigrationBindings(
		namespace,
		volume,
		qualifiedReservationCheckpoint(checkpoint, namespace),
	)
}
