package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (m *ClusterPodMigrationExecutor) Run(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(
		ctx,
		m.store,
		m.locker,
		m.storageNamespace,
		object,
		func(ctx context.Context) error { return m.run(ctx, object) },
	)
}

func (m *ClusterPodMigrationExecutor) run(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
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
				if err := m.pauseAndFinalSync(ctx, object); err != nil {
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
			if err := m.pauseAndFinalSync(ctx, object); err != nil {
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

func (m *PodMigrationExecutor) Run(ctx context.Context, object *v1alpha1.PodMigration) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(
		ctx,
		m.store,
		m.locker,
		object.Namespace,
		object,
		func(ctx context.Context) error { return m.run(ctx, object) },
	)
}

func (m *PodMigrationExecutor) run(ctx context.Context, object *v1alpha1.PodMigration) error {
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
				if err := m.pauseAndFinalSync(ctx, object); err != nil {
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
			if err := m.pauseAndFinalSync(ctx, object); err != nil {
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
