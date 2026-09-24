package app

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *PodMigrationExecutor) FinalSync(ctx context.Context, object *v1alpha1.PodMigration) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error { return m.finalSync(ctx, object) })
}

func (m *PodMigrationExecutor) ValidateFinalSync(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil ||
		!podFinalSyncPhase(workflowResumePhase(object.Status.WorkflowStatus)) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pod migration final sync",
			"workload must be paused before final sync",
		)
	}

	if err := validatePodCopyRetry(object.Status.FailureReason); err != nil {
		return err
	}

	if m.workloads == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pod migration final sync",
			"workload controller is required",
		)
	}

	plan := object.Status.Plan
	if err := m.workloads.VerifyPaused(
		ctx,
		object.Name,
		object.Namespace,
		podWorkload(plan.Workload, object.Status.Workload),
		object.Status.Phase,
		object.Status.ResumeFrom,
	); err != nil {
		return err
	}

	if err := validateFinalSyncSources(
		ctx,
		m.client,
		object.Namespace,
		plan.Volumes,
		"pod migration final sync",
	); err != nil {
		return err
	}

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)

	bindings := make([]kube.PVCTransferBindings, 0, len(plan.Volumes))
	for _, volume := range plan.Volumes {
		checkpoint := qualifiedReservationCheckpoint(
			object.Status.Volumes[indexes[volume.SourcePVC.Name]].VolumeReservationStatus,
			object.Namespace,
		)

		binding := plannedMigrationBindings(object.Namespace, volume, checkpoint)
		if err := m.validatePodFinalVolume(
			ctx,
			object.Name,
			plan.TargetNode,
			plan.ToolImage,
			volume,
			checkpoint,
			binding,
			plan.SkipSourceUsageCheck,
		); err != nil {
			return err
		}

		bindings = append(bindings, binding)
	}

	return m.switcher.VerifyVolumesOfflineForSession(ctx, object.Name, bindings)
}

func (m *PodMigrationExecutor) finalSync(ctx context.Context, object *v1alpha1.PodMigration) error {
	if object.Status.Plan == nil ||
		!podFinalSyncPhase(workflowResumePhase(object.Status.WorkflowStatus)) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pod migration final sync",
			"workload must be paused before final sync",
		)
	}

	if err := validatePodCopyRetry(object.Status.FailureReason); err != nil {
		return err
	}

	plan := object.Status.Plan

	save := func(ctx context.Context) error { return m.store.Save(ctx, object) }
	if err := m.cleanupInterruptedFinalSync(ctx, object); err != nil {
		return err
	}

	if err := m.restoreSharedMounts(
		ctx,
		object.Name,
		[]string{object.Namespace},
		&object.Status.OpenEBSLVMSharedMounts,
		save,
	); err != nil {
		return err
	}

	if err := m.ValidateFinalSync(ctx, object); err != nil {
		return err
	}

	probes, err := m.transferFinalSyncProbes(
		ctx,
		object.Name,
		object.Namespace,
		object.Namespace,
		plan.SourceNode,
		plan.TargetNode,
		plan.ToolImage,
		plan.Strategies,
		plan.Volumes,
	)
	if err != nil {
		return err
	}

	return m.finalSyncWithProbes(ctx, object, probes)
}

func (m *PodMigrationExecutor) finalSyncWithProbes(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	probes []kube.ToolImageProbeResult,
) error {
	plan := object.Status.Plan

	previous := object.Status.DeepCopy()
	if workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseFinalSynced {
		for i := range object.Status.Volumes {
			object.Status.Volumes[i].Sync.FinalCompletedAt = nil
			object.Status.Volumes[i].Sync.ChecksumVerified = false
		}
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseFinalSyncing,
		"running pod migration final sync",
	); err != nil {
		object.Status = *previous
		return err
	}

	save := func(ctx context.Context) error { return m.store.Save(ctx, object) }

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Sync.FinalCompletedAt != nil {
			continue
		}

		checkpoint := qualifiedReservationCheckpoint(
			status.VolumeReservationStatus,
			object.Namespace,
		)
		binding := plannedMigrationBindings(object.Namespace, volume, checkpoint)

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
		if err := m.transfer.copyWithRetry(ctx, request, plan.SourceNode, plan.TargetNode, "",
			&status.Sync.Attempts, &status.Sync.LastError, probes, save,
			func(ctx context.Context) error {
				if err := m.workloads.VerifyPaused(
					ctx,
					object.Name,
					object.Namespace,
					podWorkload(plan.Workload, object.Status.Workload),
					object.Status.Phase,
					object.Status.ResumeFrom,
				); err != nil {
					return err
				}

				if err := m.validatePodFinalVolume(
					ctx,
					object.Name,
					plan.TargetNode,
					plan.ToolImage,
					volume,
					checkpoint,
					binding,
					plan.SkipSourceUsageCheck,
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

	return m.transition(ctx, object, domain.PhaseFinalSynced, "pod migration final sync completed")
}

func (m *PodMigrationExecutor) cleanupInterruptedFinalSync(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if workflowResumePhase(object.Status.WorkflowStatus) != domain.PhaseFinalSyncing {
		return nil
	}

	plan := object.Status.Plan

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Sync.Attempts == 0 || status.Sync.FinalCompletedAt != nil {
			continue
		}

		checkpoint := qualifiedReservationCheckpoint(
			status.VolumeReservationStatus,
			object.Namespace,
		)

		binding := plannedMigrationBindings(object.Namespace, volume, checkpoint)
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

		if err := m.cleanupCopyAttempt(ctx, copyengine.CleanupRequest{
			SessionID:            object.Name,
			Source:               binding.SourcePVC,
			DestinationNamespace: binding.DestinationPVC.Namespace,
			Mode:                 copyengine.ModeFinal,
			Attempt:              status.Sync.Attempts,
			Strategies:           plan.Strategies,
		}, binding.DestinationPVC); err != nil {
			return err
		}
	}

	return nil
}
