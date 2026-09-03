package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateBackupAbort(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowAbort(ctx, session, domain.SessionTypeBackup, "abort backup")
}

func (s *Service) AbortBackup(ctx context.Context, session *domain.Session) error {
	return s.abortWorkflow(ctx, session, domain.SessionTypeBackup, "abort backup")
}

func (s *Service) ValidateBackupCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.validateWorkflowCleanup(
		ctx,
		session,
		domain.SessionTypeBackup,
		"cleanup backup",
		options,
	)
}

func (s *Service) CleanupBackup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupTypedWorkflow(
		ctx,
		session,
		domain.SessionTypeBackup,
		"cleanup backup",
		options,
	)
}
