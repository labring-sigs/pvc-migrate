package app

import (
	"context"
	"fmt"
	"time"

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

// sourceTerminationFailure builds the terminal failure recorded when a
// planned source PVC is terminating before final sync completed. The wording
// mirrors the planner's terminating-source fence: the deletion cannot be
// cancelled through the API, only waited out and recovered from.
func sourceTerminationFailure(operation string, termination sourceTermination) error {
	return domain.NewError(
		domain.ErrorPrecondition,
		operation,
		fmt.Sprintf(
			"source PVC %s/%s is terminating (deletion requested at %s) before final sync completed; a requested deletion cannot be cancelled through the API — let it settle and restore storage from the retained volume or a backup before retrying",
			termination.PVC.Namespace,
			termination.PVC.Name,
			termination.Since.UTC().Format(time.RFC3339),
		),
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

// sourceTerminationRelevant reports whether the workflow phase still depends
// on the source PVC staying alive. Every kind served here recreates the
// source claim at activation, so from FinalSynced onward a terminating or
// missing source is the cutover itself, not an external deletion.
func sourceTerminationRelevant(status v1alpha1.WorkflowStatus) bool {
	switch status.Phase {
	case "", domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved,
		domain.PhaseWarmCopying, domain.PhaseWarmCopied, domain.PhasePausing,
		domain.PhasePaused, domain.PhaseFinalSyncing:
		return true
	}

	return false
}

// failSourceDeleted is the shared convergence core behind every executor's
// FailSourceDeleted probe. A deleted source PVC can never be migrated:
// reserve and final-sync revalidation would re-read the missing identity
// forever, wedging the workflow in a non-terminal phase and looping the
// reconciler. A source PVC that is merely terminating is failed the same way
// while final sync has not completed — the data transfer is still backed by
// that claim, so an armed deletion must surface instead of racing the
// cutover. The scope variance between workflow kinds is exactly the
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

	scan, err := scanPlannedSourcePVCs(ctx, clients, sourceNamespace, planVolumes)
	if err != nil {
		return err
	}

	var cause error
	switch {
	case scan.Deleted:
		cause = sourceLossFailure(operation)
	case scan.Terminating != nil && sourceTerminationRelevant(status):
		cause = sourceTerminationFailure(operation, *scan.Terminating)
	default:
		return nil
	}

	return lock(ctx, func(ctx context.Context) error {
		return fail(ctx, cause)
	})
}

// validateFinalSyncSources guards every final-sync entry. The pause window
// between the workload stopping and the final-sync mount is exactly where an
// armed source deletion fires — nothing holds the claim anymore, so the
// pvc-protection finalizer releases it — and the transfer must not start on
// storage that is being deleted.
func validateFinalSyncSources(
	ctx context.Context,
	client kubernetes.Interface,
	sourceNamespace string,
	volumes []v1alpha1.VolumeSpec,
	operation string,
) error {
	scan, err := scanPlannedSourcePVCs(ctx, client, sourceNamespace, volumes)
	if err != nil {
		return err
	}

	switch {
	case scan.Deleted:
		return sourceLossFailure(operation)
	case scan.Terminating != nil:
		return sourceTerminationFailure(operation, *scan.Terminating)
	}

	return nil
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
