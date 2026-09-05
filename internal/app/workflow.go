package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func requireWorkflowSession(
	session *domain.Session,
	expected domain.SessionType,
	action string,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, action, "session is nil")
	}

	if session.Spec.Type != expected {
		return domain.NewError(
			domain.ErrorPrecondition,
			action,
			fmt.Sprintf("requires a %s session, got %s", expected, session.Spec.Type),
		)
	}

	return nil
}

func (s *Service) validateWorkflowResume(
	ctx context.Context,
	session *domain.Session,
	expected domain.SessionType,
	action string,
	validate func(context.Context, *domain.Session, domain.Phase) error,
) error {
	if err := requireWorkflowSession(session, expected, action); err != nil {
		return err
	}

	phase, err := s.validateResumePrerequisites(ctx, session)
	if err != nil {
		return err
	}

	return validate(ctx, session, phase)
}

func (s *Service) resumeWorkflow(
	ctx context.Context,
	session *domain.Session,
	expected domain.SessionType,
	action string,
	resume func(context.Context, *domain.Session, domain.Phase) error,
) error {
	if err := requireWorkflowSession(session, expected, action); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		phase, err := persistedResumePhase(session)
		if err != nil {
			return err
		}

		if err := s.cleanupInterruptedCopy(lockedCtx, session, phase); err != nil {
			return err
		}

		return resume(lockedCtx, session, phase)
	})
}

func (s *Service) validateWorkflowAbort(
	ctx context.Context,
	session *domain.Session,
	expected domain.SessionType,
	action string,
) error {
	if err := requireWorkflowSession(session, expected, action); err != nil {
		return err
	}

	return s.validateAbort(ctx, session)
}

func (s *Service) abortWorkflow(
	ctx context.Context,
	session *domain.Session,
	expected domain.SessionType,
	action string,
) error {
	if err := requireWorkflowSession(session, expected, action); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.abort(lockedCtx, session)
	})
}

func (s *Service) validateWorkflowRollback(
	ctx context.Context,
	session *domain.Session,
	expected domain.SessionType,
	action string,
) error {
	if err := requireWorkflowSession(session, expected, action); err != nil {
		return err
	}

	return s.validateRollback(ctx, session)
}

func (s *Service) rollbackWorkflow(
	ctx context.Context,
	session *domain.Session,
	expected domain.SessionType,
	action string,
) error {
	if err := requireWorkflowSession(session, expected, action); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.rollback(lockedCtx, session)
	})
}

func (s *Service) validateWorkflowCleanup(
	ctx context.Context,
	session *domain.Session,
	expected domain.SessionType,
	action string,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(session, expected, action); err != nil {
		return err
	}

	return s.validateCleanup(ctx, session, options)
}

func (s *Service) cleanupTypedWorkflow(
	ctx context.Context,
	session *domain.Session,
	expected domain.SessionType,
	action string,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(session, expected, action); err != nil {
		return err
	}

	return s.cleanupWorkflow(ctx, session, options)
}

func persistedResumePhase(session *domain.Session) (domain.Phase, error) {
	if err := validateRetryableSessionFailure(session); err != nil {
		return "", err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	return phase, nil
}
