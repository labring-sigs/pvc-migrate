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
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeMigrate,
		"resume migrate",
	); err != nil {
		return err
	}

	phase, err := s.validateResumePrerequisites(ctx, session)
	if err != nil {
		return err
	}

	return s.validateOfflineMigrationResume(ctx, session, phase)
}

func (s *Service) ResumeOfflineMigration(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeMigrate,
		"resume migrate",
	); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		phase, err := persistedResumePhase(session)
		if err != nil {
			return err
		}

		return s.resumeOfflineMigration(lockedCtx, session, phase)
	})
}

func (s *Service) ValidateOfflineMigrationAbort(
	ctx context.Context,
	session *domain.Session,
) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeMigrate,
		"abort migrate",
	); err != nil {
		return err
	}

	return s.validateAbort(ctx, session)
}

func (s *Service) AbortOfflineMigration(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeMigrate,
		"abort migrate",
	); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.abort(lockedCtx, session)
	})
}

func (s *Service) ValidateOfflineMigrationRollback(
	ctx context.Context,
	session *domain.Session,
) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeMigrate,
		"rollback migrate",
	); err != nil {
		return err
	}

	return s.validateRollback(ctx, session)
}

func (s *Service) RollbackOfflineMigration(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeMigrate,
		"rollback migrate",
	); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.rollback(lockedCtx, session)
	})
}

func (s *Service) ValidateOfflineMigrationCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeMigrate,
		"cleanup migrate",
	); err != nil {
		return err
	}

	return s.validateCleanup(ctx, session, options)
}

func (s *Service) CleanupOfflineMigration(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeMigrate,
		"cleanup migrate",
	); err != nil {
		return err
	}

	return s.cleanupWorkflow(ctx, session, options)
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
