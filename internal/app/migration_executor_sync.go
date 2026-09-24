package app

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *ClusterMigrationExecutor) FinalSync(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
) error {
	if err := validateClusterMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object,
		func(ctx context.Context) error { return m.finalSync(ctx, object) })
}

func migrationFinalSyncPhase(phase v1alpha1.WorkflowPhase) bool {
	return phase == domain.PhaseReserved || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced
}

func (m *ClusterMigrationExecutor) ValidateFinalSync(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
) error {
	if err := validateClusterMigrationObject(object); err != nil {
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
	if err := validateFinalSyncSources(
		ctx,
		m.client,
		string(plan.SourceNamespace),
		plan.Volumes,
		"migration final sync",
	); err != nil {
		return err
	}

	indexes := clusterMigrationVolumeIndexes(object.Status.Volumes)

	bindings := make([]kube.PVCTransferBindings, 0, len(plan.Volumes))
	for _, volume := range plan.Volumes {
		checkpoint := object.Status.Volumes[indexes[volume.SourcePVC.Name]].ClusterVolumeReservationStatus

		binding := plannedMigrationBindings(string(plan.SourceNamespace), volume, checkpoint)
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
			checkpoint,
		); err != nil {
			return err
		}

		bindings = append(bindings, binding)
	}

	return m.switcher.VerifyVolumesOfflineForSession(ctx, object.Name, bindings)
}

// plannedMigrationBindings combines immutable source identities with the
// destination identities recorded by reservation. Callers validate checkpoints
// before accessing them; the execution plan never acquires runtime references.
func plannedMigrationBindings(sourceNamespace string, volume v1alpha1.VolumeSpec,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
) kube.PVCTransferBindings {
	return kube.PVCTransferBindings{
		SourcePVC:      qualifiedResourceReference(volume.SourcePVC, sourceNamespace),
		SourcePV:       qualifiedResourceReference(volume.SourcePV, ""),
		DestinationPVC: *checkpoint.DestinationPVC,
		DestinationPV:  *checkpoint.DestinationPV,
	}
}

func (m *migrationResources) validateReservedVolume(
	ctx context.Context,
	id, targetNode, image string,
	bindings kube.PVCTransferBindings,
	volume v1alpha1.VolumeSpec,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
) error {
	desired, err := reservationManifest(
		bindings.DestinationPVC,
		volume.Capacity,
		volume.StorageClass,
		volume.VolumeMode,
		volume.AccessModes,
	)
	if err != nil {
		return err
	}

	return m.reserver.ValidateVolumeReservation(ctx, kube.ReservationRequest{
		SessionID: id, TargetNode: targetNode, ToolImage: m.transfer.toolImage(image),
	}, bindings.SourcePVC, bindings.SourcePV, volume.SourceCapacity, desired, *checkpoint.DeepCopy())
}

func (m *ClusterMigrationExecutor) finalSync(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
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

	probes, err := m.finalSyncProbes(ctx, object.Name, plan)
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

	indexes := clusterMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Sync.FinalCompletedAt != nil {
			continue
		}

		binding := plannedMigrationBindings(
			string(plan.SourceNamespace),
			volume,
			status.ClusterVolumeReservationStatus,
		)
		request := copyengine.CopyRequest{
			AttemptIdentity: copyengine.AttemptIdentity{
				SessionID: object.Name, Source: binding.SourcePVC, Mode: copyengine.ModeFinal,
			},
			Source: copyengine.CopySource{Path: domain.SourceTransferPath(volume.TransferScope)},
			Destination: copyengine.CopyDestination{
				Reference: binding.DestinationPVC,
				Path:      domain.DestinationTransferPath(volume.TransferScope),
			},
			Policy: copyengine.CopyPolicy{
				IgnoreSizes: destinationCapacityIsSmaller(
					volume.SourceCapacity,
					volume.Capacity,
				),
				Strategies:            slices.Clone(plan.Strategies),
				VerifyChecksum:        plan.VerifyChecksum,
				DeleteExtraneousFiles: plan.DeleteExtraneous,
			},
			Runtime: copyengine.CopyRuntime{ToolImage: plan.ToolImage},
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
					status.ClusterVolumeReservationStatus,
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

func (m *ClusterMigrationExecutor) cleanupInterruptedFinalSync(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
) error {
	plan := object.Status.Plan

	indexes := clusterMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		index, exists := indexes[volume.SourcePVC.Name]
		if !exists {
			continue
		}

		status := object.Status.Volumes[index]
		if status.Sync.Attempts == 0 || status.Sync.FinalCompletedAt != nil {
			continue
		}

		binding := plannedMigrationBindings(
			string(plan.SourceNamespace),
			volume,
			status.ClusterVolumeReservationStatus,
		)
		if err := m.validateReservedVolume(
			ctx,
			object.Name,
			plan.TargetNode,
			plan.ToolImage,
			binding,
			volume,
			status.ClusterVolumeReservationStatus,
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

func (m *ClusterMigrationExecutor) finalSyncProbes(
	ctx context.Context,
	id string,
	plan *v1alpha1.ClusterMigrationPlan,
) ([]kube.ToolImageProbeResult, error) {
	return m.transferFinalSyncProbes(
		ctx,
		id,
		string(plan.SourceNamespace),
		string(plan.TemporaryNamespace),
		plan.SourceNode,
		plan.TargetNode,
		plan.ToolImage,
		plan.Strategies,
		plan.Volumes,
	)
}
