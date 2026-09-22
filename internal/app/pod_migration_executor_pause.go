package app

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *ClusterPodMigrationExecutor) Pause(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object,
		func(ctx context.Context) error { return m.pause(ctx, object) })
}

func (m *ClusterPodMigrationExecutor) pause(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
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
	if err := m.restoreSharedMounts(
		ctx,
		object.Name,
		[]string{string(plan.SourceNamespace), string(plan.TemporaryNamespace)},
		&object.Status.OpenEBSLVMSharedMounts,
		save,
	); err != nil {
		return err
	}

	workload := clusterPodWorkload(plan.Workload, object.Status.Workload)
	if phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced {
		return m.workloads.VerifyPaused(ctx, object.Name, string(plan.SourceNamespace), workload,
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

	checkpoint, pauseErr := m.workloads.Pause(
		ctx,
		object.Name,
		string(plan.SourceNamespace),
		workload,
		object.Status.Phase,
		object.Status.ResumeFrom,
	)
	// The controller records pause-probe outcomes on the passed workload;
	// persist them so resume and restore reuse the same semantics.
	if workload.VMCluster != nil {
		if object.Status.Workload == nil {
			object.Status.Workload = &v1alpha1.ClusterPodMigrationWorkloadStatus{}
		}

		if object.Status.Workload.VMCluster == nil {
			object.Status.Workload.VMCluster = workload.VMCluster
		} else {
			object.Status.Workload.VMCluster.ComponentPausedSupported = workload.VMCluster.ComponentPausedSupported
		}
	}
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
		string(plan.SourceNamespace),
		clusterPodWorkload(
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

func (m *ClusterPodMigrationExecutor) ValidatePause(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
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
			string(plan.SourceNamespace),
			clusterPodWorkload(plan.Workload, object.Status.Workload),
			object.Status.Phase,
			object.Status.ResumeFrom,
		)
	}

	indexes := clusterPodMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		checkpoint := object.Status.Volumes[indexes[volume.SourcePVC.Name]].ClusterVolumeReservationStatus
		if err := m.validatePodFinalVolume(
			ctx,
			object.Name,
			plan.TargetNode,
			plan.ToolImage,
			volume,
			checkpoint,
			plannedMigrationBindings(string(plan.SourceNamespace), volume, checkpoint),
			plan.SkipSourceUsageCheck,
		); err != nil {
			return err
		}
	}

	return nil
}
