package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateCopyResume(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowResume(
		ctx,
		session,
		domain.SessionTypeCopy,
		"resume copy",
		s.validateCopyResume,
	)
}

func (s *Service) ResumeCopy(ctx context.Context, session *domain.Session) error {
	return s.resumeWorkflow(
		ctx,
		session,
		domain.SessionTypeCopy,
		"resume copy",
		s.resumeCopy,
	)
}

func (s *Service) ValidateCopyAbort(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowAbort(ctx, session, domain.SessionTypeCopy, "abort copy")
}

func (s *Service) AbortCopy(ctx context.Context, session *domain.Session) error {
	return s.abortWorkflow(ctx, session, domain.SessionTypeCopy, "abort copy")
}

func (s *Service) ValidateCopyCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.validateWorkflowCleanup(
		ctx,
		session,
		domain.SessionTypeCopy,
		"cleanup copy",
		options,
	)
}

func (s *Service) CleanupCopy(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupTypedWorkflow(
		ctx,
		session,
		domain.SessionTypeCopy,
		"cleanup copy",
		options,
	)
}
