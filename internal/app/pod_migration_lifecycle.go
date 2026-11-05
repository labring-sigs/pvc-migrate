package app

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (m *PodMigrationExecutor) Run(ctx context.Context, object *v1alpha1.PodMigration) error {
	return m.runWithPauseApproval(ctx, object, nil)
}

// RunWithPauseApproval runs the migration with an interactive approval in
// front of the workload pause: the callback fires once, immediately before
// the workload would be stopped for the final sync, and a declined approval
// leaves the record at its warm-copy phase with the workload untouched.
func (m *PodMigrationExecutor) RunWithPauseApproval(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	pauseApproval func(context.Context, *v1alpha1.PodMigration) error,
) error {
	return m.runWithPauseApproval(ctx, object, pauseApproval)
}

func (m *PodMigrationExecutor) runWithPauseApproval(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	pauseApproval func(context.Context, *v1alpha1.PodMigration) error,
) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	err := withStoredWorkflowLock(
		ctx,
		m.store,
		m.locker,
		object.Namespace,
		object,
		func(ctx context.Context) error { return m.run(ctx, object, pauseApproval) },
	)

	// A manual-cutover hold is a successful stopping point, not a failure:
	// the record rests at WarmCopied and continues on the next run.
	if errors.Is(err, errPodMigrationHeld) {
		return nil
	}

	return err
}

func (m *PodMigrationExecutor) run(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	pauseApproval func(context.Context, *v1alpha1.PodMigration) error,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		switch workflowResumePhase(object.Status.WorkflowStatus) {
		case domain.PhasePlanned, domain.PhaseReserving:
			if object.Status.Plan != nil &&
				object.Status.Plan.PrecopyPasses > object.Status.WarmPassesCompleted {
				if err := m.ValidateWarmCopy(ctx, object); err != nil {
					return err
				}
			}

			if err := m.reserve(ctx, object); err != nil {
				return err
			}
		case domain.PhaseReserved, domain.PhaseWarmCopied:
			if object.Status.Plan.PrecopyPasses > object.Status.WarmPassesCompleted {
				if err := m.warmCopy(ctx, object); err != nil {
					return err
				}
			} else {
				if err := m.pauseAndFinalSync(ctx, object, pauseApproval); err != nil {
					return err
				}
			}
		case domain.PhaseWarmCopying:
			if err := m.warmCopy(ctx, object); err != nil {
				return err
			}
		case domain.PhaseAborting, domain.PhaseAborted:
			return m.abort(ctx, object)
		case domain.PhaseRollingBack, domain.PhaseRolledBack:
			return m.rollback(ctx, object)
		case domain.PhasePausing:
			if err := m.pauseAndFinalSync(ctx, object, pauseApproval); err != nil {
				return err
			}
		case domain.PhasePaused, domain.PhaseFinalSyncing:
			if err := m.finalSync(ctx, object); err != nil {
				return err
			}
		case domain.PhaseFinalSynced, domain.PhaseActivating:
			if err := m.activate(ctx, object); err != nil {
				return err
			}
		case domain.PhaseActivated, domain.PhaseResuming, domain.PhaseCompleted:
			return m.resumeWorkload(ctx, object)
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"pod migration",
				"pod migration phase requires planning or recovery",
			)
		}
	}
}
