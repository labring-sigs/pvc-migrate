package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// sourceLossFailure builds the terminal failure recorded when the planned
// source PVC of a live workflow no longer exists.
func sourceLossFailure(operation string) error {
	return domain.NewError(
		domain.ErrorPrecondition,
		operation,
		"source PVC no longer exists; the migration cannot continue",
	)
}

// sourceLossRelevant reports whether the workflow phase still drives source
// storage. Terminal phases never re-enter execution, and a Failed workflow
// must not accumulate another identical failure record.
func sourceLossRelevant(status v1alpha1.WorkflowStatus) bool {
	switch status.Phase {
	case domain.PhaseFailed, domain.PhaseCompleted, domain.PhaseAborted,
		domain.PhaseRolledBack:
		return false
	}

	return true
}

// FailSourceDeleted converges a live workflow whose planned source PVC was
// deleted to Failed. Reserve and final-sync revalidation can only re-read the
// missing identity forever, wedging the workflow in a non-terminal phase and
// looping the reconciler. Returns nil without mutating the workflow when the
// source storage still exists or the phase is terminal.
func (m *ClusterPodMigrationExecutor) FailSourceDeleted(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	plan := object.Status.Plan
	if plan == nil || !sourceLossRelevant(object.Status.WorkflowStatus) {
		return nil
	}

	deleted, err := deletedPlannedSourcePVC(
		ctx,
		m.client,
		string(plan.SourceNamespace),
		plan.Volumes,
	)
	if err != nil || !deleted {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object,
		func(ctx context.Context) error {
			return m.fail(ctx, object, sourceLossFailure("pod migration"))
		})
}

// FailSourceDeleted mirrors the cluster-scoped convergence for namespaced
// PodMigration workflows.
func (m *PodMigrationExecutor) FailSourceDeleted(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	plan := object.Status.Plan
	if plan == nil || !sourceLossRelevant(object.Status.WorkflowStatus) {
		return nil
	}

	deleted, err := deletedPlannedSourcePVC(ctx, m.client, object.Namespace, plan.Volumes)
	if err != nil || !deleted {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error {
			return m.fail(ctx, object, sourceLossFailure("pod migration"))
		})
}

// FailSourceDeleted mirrors the cluster-scoped convergence for offline
// ClusterMigration workflows.
func (m *ClusterMigrationExecutor) FailSourceDeleted(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
) error {
	plan := object.Status.Plan
	if plan == nil || !sourceLossRelevant(object.Status.WorkflowStatus) {
		return nil
	}

	deleted, err := deletedPlannedSourcePVC(
		ctx,
		m.client,
		string(plan.SourceNamespace),
		plan.Volumes,
	)
	if err != nil || !deleted {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object,
		func(ctx context.Context) error {
			return m.fail(ctx, object, sourceLossFailure("migration"))
		})
}

// FailSourceDeleted mirrors the cluster-scoped convergence for namespaced
// offline Migration workflows.
func (m *MigrationExecutor) FailSourceDeleted(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	plan := object.Status.Plan
	if plan == nil || !sourceLossRelevant(object.Status.WorkflowStatus) {
		return nil
	}

	deleted, err := deletedPlannedSourcePVC(ctx, m.client, object.Namespace, plan.Volumes)
	if err != nil || !deleted {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error {
			return m.fail(ctx, object, sourceLossFailure("migration"))
		})
}
