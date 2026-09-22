package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (m *ClusterPodMigrationExecutor) PauseAndFinalSync(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object,
		func(ctx context.Context) error { return m.pauseAndFinalSync(ctx, object) })
}

func (m *ClusterPodMigrationExecutor) pauseAndFinalSync(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if object.Status.Plan == nil || !podPausePhase(phase) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pod migration final sync",
			"pod migration phase cannot pause and final-sync",
		)
	}

	if err := validatePodCopyRetry(object.Status.FailureReason); err != nil {
		return err
	}

	if podFinalSyncPhase(phase) {
		return m.finalSync(ctx, object)
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

	// The source probe co-mounts the source LV while the workload still holds
	// it read-write, so the OpenEBS LVM shared-mount arrangement must be
	// active first: warm copy left it enabled, and a precopyPasses=0 plan
	// enables it here. Restoring spec.shared before probing would force a
	// fresh read-only mount that ext4 rejects with EBUSY.
	if plan.OpenEBSLVMEnableShared {
		if err := m.prepareWarmSharedMounts(
			ctx,
			object.Name,
			string(plan.SourceNamespace),
			plan.Volumes,
			&object.Status.OpenEBSLVMSharedMounts,
			save,
		); err != nil {
			return err
		}
	}

	// Resolve tools and destination paths while the shared-mount arrangement
	// is active, then release it before taking workload downtime.
	probes, err := m.transferFinalSyncProbes(
		ctx,
		object.Name,
		string(plan.SourceNamespace),
		string(plan.TemporaryNamespace),
		plan.SourceNode,
		plan.TargetNode,
		plan.ToolImage,
		plan.Strategies,
		plan.Volumes,
	)
	if err != nil {
		return err
	}

	if err := m.restoreSharedMounts(
		ctx,
		object.Name,
		[]string{string(plan.SourceNamespace), string(plan.TemporaryNamespace)},
		&object.Status.OpenEBSLVMSharedMounts,
		save,
	); err != nil {
		return err
	}

	if err := m.ValidatePause(ctx, object); err != nil {
		return err
	}

	if err := m.pause(ctx, object); err != nil {
		return err
	}
	// Live source usage and consumers can change until the pause converges.
	if err := m.ValidateFinalSync(ctx, object); err != nil {
		return m.fail(ctx, object, err)
	}

	return m.finalSyncWithProbes(ctx, object, probes)
}
