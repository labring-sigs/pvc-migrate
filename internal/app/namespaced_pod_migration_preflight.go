package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (m *PodMigrationExecutor) Validate(ctx context.Context, object *v1alpha1.PodMigration) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return nil
	}

	switch workflowResumePhase(object.Status.WorkflowStatus) {
	case domain.PhasePlanned, domain.PhaseReserving:
		if object.Status.Plan.PrecopyPasses > 0 {
			return m.ValidateWarmCopy(ctx, object)
		}
		return m.ValidateReservation(ctx, object)
	case domain.PhaseReserved, domain.PhaseWarmCopied:
		if object.Status.Plan.PrecopyPasses > object.Status.WarmPassesCompleted {
			return m.ValidateWarmCopy(ctx, object)
		}
		return m.ValidatePause(ctx, object)
	case domain.PhaseWarmCopying:
		return m.ValidateWarmCopy(ctx, object)
	case domain.PhaseAborting, domain.PhaseAborted:
		return m.ValidateAbort(ctx, object)
	case domain.PhaseRollingBack, domain.PhaseRolledBack:
		return m.ValidateRollback(ctx, object)
	case domain.PhasePausing:
		return m.ValidatePause(ctx, object)
	case domain.PhasePaused, domain.PhaseFinalSyncing:
		return m.ValidateFinalSync(ctx, object)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		return m.ValidateActivation(ctx, object)
	case domain.PhaseActivated, domain.PhaseResuming, domain.PhaseCompleted:
		return m.ValidateWorkloadResume(ctx, object)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"pod migration",
			"pod migration phase requires recovery",
		)
	}
}

func (m *PodMigrationExecutor) RequestResume(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(
		ctx,
		m.store,
		m.locker,
		object.Namespace,
		object,
		func(ctx context.Context) error {
			if err := m.Validate(ctx, object); err != nil {
				return err
			}

			if object.Status.Phase != domain.PhaseFailed {
				return nil
			}

			previous := object.Status.WorkflowStatus.DeepCopy()
			if err := domain.ReactivateWorkflow(
				&object.Status.WorkflowStatus,
				"pod migration resume requested",
				m.now(),
			); err != nil {
				return err
			}

			if err := persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return m.store.Save(ctx, object) },
			); err != nil {
				object.Status.WorkflowStatus = *previous
				return err
			}

			return nil
		},
	)
}
