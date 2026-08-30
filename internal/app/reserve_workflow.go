package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateReserveResume(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeReserve,
		"resume reserve",
	); err != nil {
		return err
	}

	phase, err := s.validateResumePrerequisites(ctx, session)
	if err != nil {
		return err
	}

	return s.validateReserveResume(ctx, session, phase)
}

func (s *Service) ResumeReserve(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeReserve,
		"resume reserve",
	); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		phase, err := persistedResumePhase(session)
		if err != nil {
			return err
		}

		return s.resumeReserve(lockedCtx, session, phase)
	})
}

func (s *Service) ValidateReserveAbort(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeReserve,
		"abort reserve",
	); err != nil {
		return err
	}

	return s.validateAbort(ctx, session)
}

func (s *Service) AbortReserve(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeReserve,
		"abort reserve",
	); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.abort(lockedCtx, session)
	})
}

func (s *Service) ValidateReserveCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeReserve,
		"cleanup reserve",
	); err != nil {
		return err
	}

	return s.validateCleanup(ctx, session, options)
}

func (s *Service) CleanupReserve(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeReserve,
		"cleanup reserve",
	); err != nil {
		return err
	}

	return s.cleanupWorkflow(ctx, session, options)
}
