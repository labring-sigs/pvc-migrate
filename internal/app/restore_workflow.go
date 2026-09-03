package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateRestoreAbort(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeRestore,
		"abort restore",
	); err != nil {
		return err
	}

	return s.validateAbort(ctx, session)
}

func (s *Service) AbortRestore(ctx context.Context, session *domain.Session) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeRestore,
		"abort restore",
	); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.abort(lockedCtx, session)
	})
}

func (s *Service) ValidateRestoreCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeRestore,
		"cleanup restore",
	); err != nil {
		return err
	}

	return s.validateCleanup(ctx, session, options)
}

func (s *Service) CleanupRestore(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(
		session,
		domain.SessionTypeRestore,
		"cleanup restore",
	); err != nil {
		return err
	}

	return s.cleanupWorkflow(ctx, session, options)
}
