package app

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *ClusterPodMigrationExecutor) ResumeWorkload(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object,
		func(ctx context.Context) error { return m.resumeWorkload(ctx, object) })
}

func (m *ClusterPodMigrationExecutor) ValidateWorkloadResume(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if object.Status.Plan == nil ||
		(phase != domain.PhaseActivated && phase != domain.PhaseResuming && phase != domain.PhaseCompleted) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume workload",
			"pod migration volumes must be activated before resuming the workload",
		)
	}

	if m.workloads == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume workload",
			"workload controller is required",
		)
	}

	if err := m.verifyActiveVolumes(ctx, object); err != nil {
		return err
	}

	plan := object.Status.Plan
	if err := m.workloads.ValidateResume(
		ctx,
		object.Name,
		string(plan.SourceNamespace),
		clusterPodWorkload(plan.Workload, object.Status.Workload),
		object.Status.Phase,
		object.Status.ResumeFrom,
	); err != nil {
		return err
	}

	return m.validateResumeNode(ctx, plan.Workload.Adapter, plan.TargetNode)
}

func (m *ClusterPodMigrationExecutor) resumeWorkload(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := m.ValidateWorkloadResume(ctx, object); err != nil {
		return err
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

	if object.Status.Phase == domain.PhaseCompleted {
		return nil
	}

	if workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseCompleted {
		return m.transition(
			ctx,
			object,
			domain.PhaseCompleted,
			"pod migration completion revalidated",
		)
	}

	if err := m.transition(ctx, object, domain.PhaseResuming, "resuming workload"); err != nil {
		return err
	}

	indexes := clusterPodMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if err := m.ensureDestinationMount(
			ctx,
			volume.AccessModes,
			volume.ConcurrentConsumers,
			*status.Activation.ActivePVC,
			*status.DestinationPV,
			plan.OpenEBSLVMEnableShared,
		); err != nil {
			return m.fail(ctx, object, err)
		}
	}

	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	checkpoint, resumeErr := m.workloads.Resume(
		ctx,
		object.Name,
		string(plan.SourceNamespace),
		clusterPodWorkload(plan.Workload, object.Status.Workload),
		plan.TargetNode,
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

	if err := m.verifyActiveVolumes(ctx, object); err != nil {
		return m.fail(ctx, object, err)
	}

	if plan.Workload.Adapter == v1alpha1.WorkloadStandalone {
		workload := clusterPodWorkload(plan.Workload, object.Status.Workload)
		if err := verifyResumedStandalonePod(
			ctx,
			m.client,
			object.Name,
			qualifiedResourceReference(*workload.Pod, string(plan.SourceNamespace)),
			plan.TargetNode,
		); err != nil {
			return m.fail(ctx, object, err)
		}
	}

	return m.transition(
		ctx,
		object,
		domain.PhaseCompleted,
		"migration completed and workload is ready",
	)
}
