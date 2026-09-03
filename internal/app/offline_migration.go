package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// OfflineMigrate executes reserve, final sync, activation, and completion
// without workload discovery or controller orchestration.
func (s *Service) OfflineMigrate(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.offlineMigrate(lockedCtx, session)
	})
}

func (s *Service) ValidateOfflineMigrationResume(
	ctx context.Context,
	session *domain.Session,
) error {
	return s.validateWorkflowResume(
		ctx,
		session,
		domain.SessionTypeMigrate,
		"resume migrate",
		s.validateOfflineMigrationResume,
	)
}

func (s *Service) ResumeOfflineMigration(ctx context.Context, session *domain.Session) error {
	return s.resumeWorkflow(
		ctx,
		session,
		domain.SessionTypeMigrate,
		"resume migrate",
		s.resumeOfflineMigration,
	)
}

func (s *Service) ValidateOfflineMigrationAbort(
	ctx context.Context,
	session *domain.Session,
) error {
	return s.validateWorkflowAbort(ctx, session, domain.SessionTypeMigrate, "abort migrate")
}

func (s *Service) AbortOfflineMigration(ctx context.Context, session *domain.Session) error {
	return s.abortWorkflow(ctx, session, domain.SessionTypeMigrate, "abort migrate")
}

func (s *Service) ValidateOfflineMigrationRollback(
	ctx context.Context,
	session *domain.Session,
) error {
	return s.validateWorkflowRollback(
		ctx,
		session,
		domain.SessionTypeMigrate,
		"rollback migrate",
	)
}

func (s *Service) RollbackOfflineMigration(ctx context.Context, session *domain.Session) error {
	return s.rollbackWorkflow(ctx, session, domain.SessionTypeMigrate, "rollback migrate")
}

func (s *Service) ValidateOfflineMigrationCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.validateWorkflowCleanup(
		ctx,
		session,
		domain.SessionTypeMigrate,
		"cleanup migrate",
		options,
	)
}

func (s *Service) CleanupOfflineMigration(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupTypedWorkflow(
		ctx,
		session,
		domain.SessionTypeMigrate,
		"cleanup migrate",
		options,
	)
}

func (s *Service) OfflineFinalSync(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.offlineFinalSync(lockedCtx, session) },
	)
}

func (s *Service) OfflineActivate(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.offlineActivate(lockedCtx, session) },
	)
}
