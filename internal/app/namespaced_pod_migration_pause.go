package app

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *PodMigrationExecutor) Pause(ctx context.Context, object *v1alpha1.PodMigration) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error { return m.pause(ctx, object) })
}

func (m *PodMigrationExecutor) pause(ctx context.Context, object *v1alpha1.PodMigration) error {
	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if object.Status.Plan == nil || !podPausePhase(phase) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause workload",
			"pod migration phase cannot pause",
		)
	}

	if m.workloads == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause workload",
			"workload controller is required",
		)
	}

	plan := object.Status.Plan

	save := func(ctx context.Context) error { return m.store.Save(ctx, object) }
	if err := m.restoreSharedMounts(ctx, object.Name, []string{object.Namespace},
		&object.Status.OpenEBSLVMSharedMounts, save); err != nil {
		return err
	}

	workload := podWorkload(plan.Workload, object.Status.Workload)
	if phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced {
		return m.workloads.VerifyPaused(ctx, object.Name, object.Namespace, workload,
			object.Status.Phase, object.Status.ResumeFrom)
	}

	if err := m.ValidatePause(ctx, object); err != nil {
		return err
	}

	if err := m.transition(ctx, object, domain.PhasePausing, "pausing workload"); err != nil {
		return err
	}

	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	checkpoint, pauseErr := m.workloads.Pause(ctx, object.Name, object.Namespace, workload,
		object.Status.Phase, object.Status.ResumeFrom)
	// A controller can return recovery identities before a later convergence failure.
	// Persist them before recording the failure or attempting further mutations.
	if err := m.saveWorkloadCheckpoint(ctx, object, checkpoint); err != nil {
		return errors.Join(pauseErr, err)
	}

	if pauseErr != nil {
		return m.fail(ctx, object, pauseErr)
	}

	if err := m.workloads.VerifyPaused(
		ctx,
		object.Name,
		object.Namespace,
		podWorkload(
			plan.Workload,
			object.Status.Workload,
		),
		object.Status.Phase,
		object.Status.ResumeFrom,
	); err != nil {
		return m.fail(ctx, object, err)
	}

	return m.transition(ctx, object, domain.PhasePaused, "workload is safely paused")
}

func (m *PodMigrationExecutor) ValidatePause(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if object.Status.Plan == nil || !podPausePhase(phase) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause workload",
			"pod migration phase cannot pause",
		)
	}

	if m.workloads == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause workload",
			"workload controller is required",
		)
	}

	plan := object.Status.Plan
	if podFinalSyncPhase(phase) {
		return m.workloads.VerifyPaused(
			ctx,
			object.Name,
			object.Namespace,
			podWorkload(plan.Workload, object.Status.Workload),
			object.Status.Phase,
			object.Status.ResumeFrom,
		)
	}

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		checkpoint := qualifiedReservationCheckpoint(
			object.Status.Volumes[indexes[volume.SourcePVC.Name]].VolumeReservationStatus,
			object.Namespace,
		)
		if err := m.validatePodFinalVolume(
			ctx,
			object.Name,
			plan.TargetNode,
			plan.ToolImage,
			volume,
			checkpoint,
			plannedMigrationBindings(object.Namespace, volume, checkpoint),
			plan.SkipSourceUsageCheck,
		); err != nil {
			return err
		}
	}

	return nil
}
