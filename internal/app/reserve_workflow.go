package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateReserveResume(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowResume(
		ctx,
		session,
		domain.SessionTypeReserve,
		"resume reserve",
		s.validateReserveResume,
	)
}

func (s *Service) ResumeReserve(ctx context.Context, session *domain.Session) error {
	return s.resumeWorkflow(
		ctx,
		session,
		domain.SessionTypeReserve,
		"resume reserve",
		s.resumeReserve,
	)
}

func (s *Service) ValidateReserveAbort(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowAbort(ctx, session, domain.SessionTypeReserve, "abort reserve")
}

func (s *Service) AbortReserve(ctx context.Context, session *domain.Session) error {
	return s.abortWorkflow(ctx, session, domain.SessionTypeReserve, "abort reserve")
}

func (s *Service) ValidateReserveCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.validateWorkflowCleanup(
		ctx,
		session,
		domain.SessionTypeReserve,
		"cleanup reserve",
		options,
	)
}

func (s *Service) CleanupReserve(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupTypedWorkflow(
		ctx,
		session,
		domain.SessionTypeReserve,
		"cleanup reserve",
		options,
	)
}
