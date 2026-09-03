package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateRestoreAbort(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowAbort(ctx, session, domain.SessionTypeRestore, "abort restore")
}

func (s *Service) AbortRestore(ctx context.Context, session *domain.Session) error {
	return s.abortWorkflow(ctx, session, domain.SessionTypeRestore, "abort restore")
}

func (s *Service) ValidateRestoreCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.validateWorkflowCleanup(
		ctx,
		session,
		domain.SessionTypeRestore,
		"cleanup restore",
		options,
	)
}

func (s *Service) CleanupRestore(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupTypedWorkflow(
		ctx,
		session,
		domain.SessionTypeRestore,
		"cleanup restore",
		options,
	)
}
