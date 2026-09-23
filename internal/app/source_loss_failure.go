package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/client-go/kubernetes"
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

// failSourceDeleted is the shared convergence core behind every executor's
// FailSourceDeleted probe. A deleted source PVC can never be migrated:
// reserve and final-sync revalidation would re-read the missing identity
// forever, wedging the workflow in a non-terminal phase and looping the
// reconciler. The scope variance between workflow kinds is exactly the
// source namespace plus each executor's lock and failure plumbing; the
// decision itself is written once here.
func failSourceDeleted(
	ctx context.Context,
	status v1alpha1.WorkflowStatus,
	planVolumes []v1alpha1.VolumeSpec,
	sourceNamespace string,
	clients kubernetes.Interface,
	lock func(context.Context, func(context.Context) error) error,
	fail func(context.Context, error) error,
	operation string,
) error {
	if !sourceLossRelevant(status) {
		return nil
	}

	deleted, err := deletedPlannedSourcePVC(ctx, clients, sourceNamespace, planVolumes)
	if err != nil || !deleted {
		return err
	}

	return lock(ctx, func(ctx context.Context) error {
		return fail(ctx, sourceLossFailure(operation))
	})
}

// FailSourceDeleted converges a live workflow whose planned source PVC was
// deleted to Failed. Returns nil without mutating the workflow when the
// source storage still exists or the phase is terminal.
func (m *PodMigrationExecutor) FailSourceDeleted(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	plan := object.Status.Plan
	if plan == nil {
		return nil
	}

	return failSourceDeleted(ctx, object.Status.WorkflowStatus,
		plan.Volumes, object.Namespace, m.client,
		func(ctx context.Context, run func(context.Context) error) error {
			return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object, run)
		},
		func(ctx context.Context, cause error) error {
			return m.fail(ctx, object, cause)
		}, "pod migration")
}

// FailSourceDeleted mirrors the cluster-scoped convergence for offline
// ClusterMigration workflows.
func (m *ClusterMigrationExecutor) FailSourceDeleted(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
) error {
	plan := object.Status.Plan
	if plan == nil {
		return nil
	}

	return failSourceDeleted(ctx, object.Status.WorkflowStatus,
		plan.Volumes, string(plan.SourceNamespace), m.client,
		func(ctx context.Context, run func(context.Context) error) error {
			return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object, run)
		},
		func(ctx context.Context, cause error) error {
			return m.fail(ctx, object, cause)
		}, "migration")
}

// FailSourceDeleted mirrors the cluster-scoped convergence for namespaced
// offline Migration workflows.
func (m *MigrationExecutor) FailSourceDeleted(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	plan := object.Status.Plan
	if plan == nil {
		return nil
	}

	return failSourceDeleted(ctx, object.Status.WorkflowStatus,
		plan.Volumes, object.Namespace, m.client,
		func(ctx context.Context, run func(context.Context) error) error {
			return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object, run)
		},
		func(ctx context.Context, cause error) error {
			return m.fail(ctx, object, cause)
		}, "migration")
}
