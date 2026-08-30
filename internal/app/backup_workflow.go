package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateBackupAbort(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeBackup, "abort backup"); err != nil {
		return err
	}
	return s.validateAbort(ctx, session)
}

func (s *Service) AbortBackup(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeBackup, "abort backup"); err != nil {
		return err
	}
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.abort(lockedCtx, session)
	})
}

func (s *Service) ValidateBackupCleanup(ctx context.Context, session *domain.Session, options CleanupOptions) error {
	if err := requireWorkflowSession(session, domain.SessionTypeBackup, "cleanup backup"); err != nil {
		return err
	}
	return s.validateCleanup(ctx, session, options)
}

func (s *Service) CleanupBackup(ctx context.Context, session *domain.Session, options CleanupOptions) error {
	if err := requireWorkflowSession(session, domain.SessionTypeBackup, "cleanup backup"); err != nil {
		return err
	}
	return s.cleanupWorkflow(ctx, session, options)
}
