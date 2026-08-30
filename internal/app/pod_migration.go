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
	if err := requireWorkflowSession(session, domain.SessionTypeMigratePod, "resume migrate-pod"); err != nil {
		return err
	}
	phase, err := s.validateResumePrerequisites(ctx, session)
	if err != nil {
		return err
	}
	return s.validatePodMigrationResume(ctx, session, phase)
}

func (s *Service) ResumePodMigration(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeMigratePod, "resume migrate-pod"); err != nil {
		return err
	}
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		phase, err := persistedResumePhase(session)
		if err != nil {
			return err
		}
		return s.resumePodMigration(lockedCtx, session, phase)
	})
}

func (s *Service) ValidatePodMigrationAbort(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeMigratePod, "abort migrate-pod"); err != nil {
		return err
	}
	return s.validateAbort(ctx, session)
}

func (s *Service) AbortPodMigration(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeMigratePod, "abort migrate-pod"); err != nil {
		return err
	}
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.abort(lockedCtx, session)
	})
}

func (s *Service) ValidatePodMigrationRollback(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeMigratePod, "rollback migrate-pod"); err != nil {
		return err
	}
	return s.validateRollback(ctx, session)
}

func (s *Service) RollbackPodMigration(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(session, domain.SessionTypeMigratePod, "rollback migrate-pod"); err != nil {
		return err
	}
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.rollback(lockedCtx, session)
	})
}

func (s *Service) ValidatePodMigrationCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(session, domain.SessionTypeMigratePod, "cleanup migrate-pod"); err != nil {
		return err
	}
	return s.validateCleanup(ctx, session, options)
}

func (s *Service) CleanupPodMigration(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(session, domain.SessionTypeMigratePod, "cleanup migrate-pod"); err != nil {
		return err
	}
	return s.cleanupWorkflow(ctx, session, options)
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
