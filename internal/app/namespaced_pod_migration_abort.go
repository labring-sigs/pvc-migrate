package app

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *PodMigrationExecutor) Abort(ctx context.Context, object *v1alpha1.PodMigration) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(
		ctx,
		m.store,
		m.locker,
		object.Namespace,
		object,
		func(ctx context.Context) error { return m.abort(ctx, object) },
	)
}

func (m *PodMigrationExecutor) ValidateAbort(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	if err := validatePodAbortPhase(workflowResumePhase(object.Status.WorkflowStatus)); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return nil
	}

	if err := m.validateSharedMountRestoration(
		ctx,
		object.Name,
		object.Status.OpenEBSLVMSharedMounts,
	); err != nil {
		return err
	}

	if !podAbortNeedsResume(object.Status.Phase, object.Status.ResumeFrom, object.Status.History) {
		return nil
	}

	if m.workloads == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"abort pod migration",
			"workload controller is required",
		)
	}

	plan := object.Status.Plan

	scan, err := scanPlannedSourcePVCs(ctx, m.client, object.Namespace, plan.Volumes)
	if err != nil {
		return err
	}

	if workflowDeletionInProgress(ctx) {
		// Lost or terminating source storage cannot be re-verified and gives
		// the workload nothing to resume onto — the scheduler refuses pods
		// mounting a claim that is being deleted. Deletion must converge
		// through cleanup instead of erroring or waiting forever.
		if scan.Deleted || scan.Terminating != nil {
			return nil
		}
	}

	// A live abort resumes the workload onto the source claim; a terminating
	// source would leave that resume waiting forever. Surface the loss.
	if scan.Terminating != nil {
		return sourceTerminationFailure("abort pod migration", *scan.Terminating)
	}

	for _, volume := range plan.Volumes {
		if err := verifySourceStorage(
			ctx,
			m.client,
			qualifiedResourceReference(volume.SourcePVC, object.Namespace),
			qualifiedResourceReference(volume.SourcePV, ""),
		); err != nil {
			return err
		}
	}

	return m.validateResumeNode(ctx, plan.Workload.Adapter, plan.SourceNode)
}

func (m *PodMigrationExecutor) abort(ctx context.Context, object *v1alpha1.PodMigration) error {
	if err := m.ValidateAbort(ctx, object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		if object.Status.Phase == domain.PhaseAborted {
			return nil
		}

		return m.transition(
			ctx,
			object,
			domain.PhaseAborted,
			"pod migration aborted before execution planning",
		)
	}

	plan := object.Status.Plan

	resume := podAbortNeedsResume(
		object.Status.Phase,
		object.Status.ResumeFrom,
		object.Status.History,
	)
	if resume && workflowDeletionInProgress(ctx) {
		scan, err := scanPlannedSourcePVCs(
			ctx,
			m.client,
			object.Namespace,
			plan.Volumes,
		)
		if err != nil {
			return m.fail(ctx, object, err)
		}

		// Resuming the workload onto lost or terminating source storage can
		// only fail — the scheduler refuses pods mounting a claim that is
		// being deleted; the deletion pass converges to Aborted and cleanup
		// takes over.
		if scan.Deleted || scan.Terminating != nil {
			resume = false
		}
	}

	save := func(ctx context.Context) error { return m.store.Save(ctx, object) }
	if object.Status.Phase == domain.PhaseAborted {
		return m.restoreSharedMounts(
			ctx,
			object.Name,
			[]string{object.Namespace},
			&object.Status.OpenEBSLVMSharedMounts,
			save,
		)
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseAborting,
		"stopping pod migration tools",
	); err != nil {
		return err
	}
	// Stop copy consumers before restoring source mount settings or restarting the workload.
	indexes := podMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		index, exists := indexes[volume.SourcePVC.Name]
		if !exists {
			continue
		}

		status := object.Status.Volumes[index]
		if status.Sync.Attempts == 0 {
			continue
		}

		checkpoint := qualifiedReservationCheckpoint(
			status.VolumeReservationStatus,
			object.Namespace,
		)

		binding := plannedMigrationBindings(object.Namespace, volume, checkpoint)

		skipValidation, err := deletionSourceMissing(ctx, m.client, object.Namespace, volume)
		if err != nil {
			return m.fail(ctx, object, err)
		}

		// The reserved-volume validation re-verifies the source identity,
		// which no longer exists during deletion convergence.
		if !skipValidation {
			if err := m.validateReservedVolume(
				ctx,
				object.Name,
				plan.TargetNode,
				plan.ToolImage,
				binding,
				volume,
				checkpoint,
			); err != nil {
				return m.fail(ctx, object, err)
			}
		}

		for _, mode := range []copyengine.Mode{copyengine.ModeWarm, copyengine.ModeFinal} {
			if err := m.cleanupCopyAttempt(ctx, copyengine.CleanupRequest{
				SessionID:            object.Name,
				Source:               binding.SourcePVC,
				DestinationNamespace: binding.DestinationPVC.Namespace,
				Mode:                 mode,
				Attempt:              status.Sync.Attempts,
				Strategies:           plan.Strategies,
			}, binding.DestinationPVC); err != nil {
				return m.fail(ctx, object, err)
			}
		}
	}

	if err := m.restoreSharedMounts(
		ctx,
		object.Name,
		[]string{object.Namespace},
		&object.Status.OpenEBSLVMSharedMounts,
		save,
	); err != nil {
		return m.fail(ctx, object, err)
	}

	if resume {
		if err := m.ValidateAbort(ctx, object); err != nil {
			return m.fail(ctx, object, err)
		}

		if err := kube.LeaseFenceError(ctx); err != nil {
			return err
		}

		checkpoint, resumeErr := m.workloads.Resume(
			ctx,
			object.Name,
			object.Namespace,
			podWorkload(plan.Workload, object.Status.Workload),
			plan.SourceNode,
			object.Status.Phase,
			object.Status.ResumeFrom,
			object.Status.WorkflowStatus.DeepCopy().History,
		)
		if err := m.saveWorkloadCheckpoint(ctx, object, checkpoint); err != nil {
			return errors.Join(resumeErr, err)
		}

		if resumeErr != nil {
			return m.fail(ctx, object, resumeErr)
		}

		if plan.Workload.Adapter == v1alpha1.WorkloadStandalone {
			workload := podWorkload(plan.Workload, object.Status.Workload)
			if err := verifyResumedStandalonePod(
				ctx,
				m.client,
				object.Name,
				qualifiedResourceReference(*workload.Pod, object.Namespace),
				plan.SourceNode,
			); err != nil {
				return m.fail(ctx, object, err)
			}
		}
	}

	return m.transition(
		ctx,
		object,
		domain.PhaseAborted,
		"pod migration aborted; reserved volumes are retained for cleanup",
	)
}
