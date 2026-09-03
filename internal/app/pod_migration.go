package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// MigratePod executes warm copy and controller-managed workload cutover.
func (s *Service) MigratePod(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.migratePod(lockedCtx, session)
	})
}

func (s *Service) ValidatePodMigrationResume(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowResume(
		ctx,
		session,
		domain.SessionTypeMigratePod,
		"resume migrate-pod",
		s.validatePodMigrationResume,
	)
}

func (s *Service) ResumePodMigration(ctx context.Context, session *domain.Session) error {
	return s.resumeWorkflow(
		ctx,
		session,
		domain.SessionTypeMigratePod,
		"resume migrate-pod",
		s.resumePodMigration,
	)
}

func (s *Service) ValidatePodMigrationAbort(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowAbort(ctx, session, domain.SessionTypeMigratePod, "abort migrate-pod")
}

func (s *Service) AbortPodMigration(ctx context.Context, session *domain.Session) error {
	return s.abortWorkflow(ctx, session, domain.SessionTypeMigratePod, "abort migrate-pod")
}

func (s *Service) ValidatePodMigrationRollback(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowRollback(
		ctx,
		session,
		domain.SessionTypeMigratePod,
		"rollback migrate-pod",
	)
}

func (s *Service) RollbackPodMigration(ctx context.Context, session *domain.Session) error {
	return s.rollbackWorkflow(ctx, session, domain.SessionTypeMigratePod, "rollback migrate-pod")
}

func (s *Service) ValidatePodMigrationCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.validateWorkflowCleanup(
		ctx,
		session,
		domain.SessionTypeMigratePod,
		"cleanup migrate-pod",
		options,
	)
}

func (s *Service) CleanupPodMigration(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupTypedWorkflow(
		ctx,
		session,
		domain.SessionTypeMigratePod,
		"cleanup migrate-pod",
		options,
	)
}

func (s *Service) PodPause(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.pause(lockedCtx, session) },
	)
}

func (s *Service) PodFinalSync(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.podFinalSync(lockedCtx, session) },
	)
}

// PodPauseAndFinalSync keeps workload pause and the terminal copy under one
// Session lease.
func (s *Service) PodPauseAndFinalSync(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.podPauseAndFinalSync(lockedCtx, session)
	})
}

func (s *Service) PodActivate(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.podActivate(lockedCtx, session) },
	)
}

func (s *Service) ResumePodWorkload(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.resumeWorkload(lockedCtx, session) },
	)
}
