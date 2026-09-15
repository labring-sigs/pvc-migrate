package app

import (
	"context"
	"errors"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *PodMigrationExecutor) WarmCopy(ctx context.Context, object *v1alpha1.PodMigration) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error { return m.warmCopy(ctx, object) })
}

func (m *PodMigrationExecutor) ValidateWarmCopy(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	if err := validatePodCopyRetry(object.Status.FailureReason); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if object.Status.Plan == nil ||
		(!podWarmCopyPhase(phase) && phase != domain.PhasePlanned && phase != domain.PhaseReserving) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pod migration warm copy",
			"pod migration phase cannot warm-copy",
		)
	}

	plan := object.Status.Plan

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		checkpoint := v1alpha1.ClusterVolumeReservationStatus{SourcePVCName: volume.SourcePVC.Name}
		if index, exists := indexes[volume.SourcePVC.Name]; exists {
			checkpoint = qualifiedReservationCheckpoint(
				object.Status.Volumes[index].VolumeReservationStatus,
				object.Namespace,
			)
		}

		if err := m.validateReservationVolume(ctx, kube.ReservationRequest{
			SessionID:  object.Name,
			TargetNode: plan.TargetNode,
			ToolImage:  m.transfer.toolImage(plan.ToolImage),
		}, object.Namespace, object.Namespace, volume, checkpoint, plan.SkipSourceUsageCheck); err != nil {
			return err
		}
	}

	if err := m.validateWarmSourceMounts(ctx, object.Namespace, plan.Volumes,
		plan.OpenEBSLVMEnableShared, object.Status.OpenEBSLVMSharedMounts); err != nil {
		return err
	}

	_, err := m.warmSourceTargets(ctx, object.Namespace, plan.SourceNode, plan.Volumes)

	return err
}

func (m *PodMigrationExecutor) warmCopy(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) (result error) {
	if !podWarmCopyPhase(workflowResumePhase(object.Status.WorkflowStatus)) ||
		object.Status.Plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pod migration warm copy",
			"pod migration requires reserved storage before warm copy",
		)
	}

	if err := validatePodCopyRetry(object.Status.FailureReason); err != nil {
		return err
	}

	plan := object.Status.Plan
	save := func(ctx context.Context) error { return m.store.Save(ctx, object) }

	namespaces := []string{object.Namespace}
	if err := m.cleanupInterruptedWarmCopy(ctx, object); err != nil {
		return err
	}

	if err := m.restoreSharedMounts(
		ctx,
		object.Name,
		namespaces,
		&object.Status.OpenEBSLVMSharedMounts,
		save,
	); err != nil {
		return m.fail(ctx, object, err)
	}

	if err := m.ValidateWarmCopy(ctx, object); err != nil {
		return err
	}

	restore := true
	defer func() {
		if restore {
			if err := m.restoreSharedMountsAfterFailure(
				ctx,
				object.Name,
				namespaces,
				&object.Status.OpenEBSLVMSharedMounts,
				save,
			); err != nil {
				result = m.fail(ctx, object, errors.Join(result, err))
			}
		}
	}()

	if plan.OpenEBSLVMEnableShared {
		if err := m.prepareWarmSharedMounts(ctx, object.Name, object.Namespace, plan.Volumes,
			&object.Status.OpenEBSLVMSharedMounts, save); err != nil {
			return err
		}
	}

	sources, err := m.warmSourceTargets(ctx, object.Namespace, plan.SourceNode, plan.Volumes)
	if err != nil {
		return err
	}

	destinations := podWarmDestinationTargets(
		object.Namespace,
		plan.TargetNode,
		plan.Strategies,
		plan.Volumes,
	)

	probes, err := m.probeWarmCopy(
		ctx,
		object.Name,
		plan.ToolImage,
		plan.Strategies,
		plan.Volumes,
		sources,
		destinations,
		object.Status.OpenEBSLVMSharedMounts,
	)
	if err != nil {
		return err
	}

	previous := object.Status.DeepCopy()
	if workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseWarmCopied {
		for i := range object.Status.Volumes {
			object.Status.Volumes[i].Sync.WarmCompletedAt = nil
			object.Status.Volumes[i].Sync.LastError = ""
		}
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseWarmCopying,
		"running pod migration warm copy",
	); err != nil {
		object.Status = *previous
		return err
	}

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Sync.WarmCompletedAt != nil {
			continue
		}

		checkpoint := qualifiedReservationCheckpoint(
			status.VolumeReservationStatus,
			object.Namespace,
		)
		binding := plannedMigrationBindings(object.Namespace, volume, checkpoint)
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
			DeleteExtraneousFiles: plan.DeleteExtraneous,
			Mode:                  copyengine.ModeWarm,
		}

		recovery := ""
		if kb := plan.Workload.KubeBlocks; kb != nil {
			recovery = fmt.Sprintf(
				"update KubeBlocks Cluster %s component %s volumeClaimTemplates storage request, abort and clean up this migration, then create a new pod migration",
				kb.Cluster,
				kb.Component,
			)
		}

		if err := m.transfer.copyWithRetry(ctx, request, plan.SourceNode, plan.TargetNode, recovery,
			&status.Sync.Attempts, &status.Sync.LastError, probes, save,
			func(ctx context.Context) error {
				if _, err := m.warmSourceTargets(
					ctx,
					object.Namespace,
					plan.SourceNode,
					[]v1alpha1.VolumeSpec{volume},
				); err != nil {
					return err
				}

				return m.validateReservedVolume(
					ctx,
					object.Name,
					plan.TargetNode,
					plan.ToolImage,
					binding,
					volume,
					checkpoint,
				)
			},
			func(ctx context.Context) (bool, error) {
				return m.warmSourceWritable(
					ctx,
					object.Name,
					binding.SourcePVC,
					binding.SourcePV,
					object.Status.OpenEBSLVMSharedMounts,
				)
			}); err != nil {
			return m.fail(ctx, object, err)
		}

		if err := checkpointWarmCopy(
			ctx,
			&status.Sync.WarmCompletedAt,
			&status.Sync.LastError,
			m.now(),
			save,
		); err != nil {
			return err
		}
	}

	if err := m.restoreSharedMounts(
		ctx,
		object.Name,
		namespaces,
		&object.Status.OpenEBSLVMSharedMounts,
		save,
	); err != nil {
		return m.fail(ctx, object, err)
	}

	restore = false

	return checkpointPodMigrationWarmPass(ctx, &object.Status.WarmPassesCompleted,
		func(ctx context.Context) error {
			return m.transition(
				ctx,
				object,
				domain.PhaseWarmCopied,
				"pod migration warm copy completed",
			)
		})
}

func (m *PodMigrationExecutor) cleanupInterruptedWarmCopy(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if workflowResumePhase(object.Status.WorkflowStatus) != domain.PhaseWarmCopying {
		return nil
	}

	plan := object.Status.Plan

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Sync.Attempts == 0 || status.Sync.WarmCompletedAt != nil {
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
			Mode:                 copyengine.ModeWarm,
			Attempt:              status.Sync.Attempts,
			Strategies:           plan.Strategies,
		}, binding.DestinationPVC); err != nil {
			return err
		}
	}

	return nil
}
