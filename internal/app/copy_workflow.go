package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateCopyResume(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeCopy, "resume copy"); err != nil {
		return err
	}

	phase, err := s.validateResumePrerequisites(ctx, session)
	if err != nil {
		return err
	}

	return s.validateCopyResume(ctx, session, phase)
}

func (s *Service) ResumeCopy(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeCopy, "resume copy"); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		phase, err := persistedResumePhase(session)
		if err != nil {
			return err
		}

		return s.resumeCopy(lockedCtx, session, phase)
	})
}

func (s *Service) ValidateCopyAbort(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeCopy, "abort copy"); err != nil {
		return err
	}
	return s.validateAbort(ctx, session)
}

func (s *Service) AbortCopy(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeCopy, "abort copy"); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.abort(lockedCtx, session)
	})
}

func (s *Service) ValidateCopyCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(session, domain.SessionTypeCopy, "cleanup copy"); err != nil {
		return err
	}
	return s.validateCleanup(ctx, session, options)
}

func (s *Service) CleanupCopy(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(session, domain.SessionTypeCopy, "cleanup copy"); err != nil {
		return err
	}
	return s.cleanupWorkflow(ctx, session, options)
}
