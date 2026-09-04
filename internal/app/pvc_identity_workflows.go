package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateRenameResume(ctx context.Context, session *domain.Session) error {
	return s.validatePVCIdentityWorkflowResume(
		ctx,
		session,
		domain.SessionTypeRename,
		domain.OperationRename,
	)
}

func (s *Service) ResumeRename(ctx context.Context, session *domain.Session) error {
	return s.resumePVCIdentityWorkflow(
		ctx,
		session,
		domain.SessionTypeRename,
		domain.OperationRename,
	)
}

func (s *Service) ValidateRenameAbort(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowAbort(
		ctx,
		session,
		domain.SessionTypeRename,
		"abort rename",
	)
}

func (s *Service) AbortRename(ctx context.Context, session *domain.Session) error {
	return s.abortWorkflow(ctx, session, domain.SessionTypeRename, "abort rename")
}

func (s *Service) ValidateRenameRollback(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowRollback(
		ctx,
		session,
		domain.SessionTypeRename,
		"rollback rename",
	)
}

func (s *Service) RollbackRename(ctx context.Context, session *domain.Session) error {
	return s.rollbackWorkflow(ctx, session, domain.SessionTypeRename, "rollback rename")
}

func (s *Service) ValidateRenameCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.validateWorkflowCleanup(
		ctx,
		session,
		domain.SessionTypeRename,
		"cleanup rename",
		options,
	)
}

func (s *Service) CleanupRename(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupTypedWorkflow(
		ctx,
		session,
		domain.SessionTypeRename,
		"cleanup rename",
		options,
	)
}

func (s *Service) ValidateMoveResume(ctx context.Context, session *domain.Session) error {
	return s.validatePVCIdentityWorkflowResume(
		ctx,
		session,
		domain.SessionTypeMove,
		domain.OperationMove,
	)
}

func (s *Service) ResumeMove(ctx context.Context, session *domain.Session) error {
	return s.resumePVCIdentityWorkflow(ctx, session, domain.SessionTypeMove, domain.OperationMove)
}

func (s *Service) ValidateMoveAbort(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowAbort(ctx, session, domain.SessionTypeMove, "abort move")
}

func (s *Service) AbortMove(ctx context.Context, session *domain.Session) error {
	return s.abortWorkflow(ctx, session, domain.SessionTypeMove, "abort move")
}

func (s *Service) ValidateMoveRollback(ctx context.Context, session *domain.Session) error {
	return s.validateWorkflowRollback(
		ctx,
		session,
		domain.SessionTypeMove,
		"rollback move",
	)
}

func (s *Service) RollbackMove(ctx context.Context, session *domain.Session) error {
	return s.rollbackWorkflow(ctx, session, domain.SessionTypeMove, "rollback move")
}

func (s *Service) ValidateMoveCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.validateWorkflowCleanup(
		ctx,
		session,
		domain.SessionTypeMove,
		"cleanup move",
		options,
	)
}

func (s *Service) CleanupMove(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupTypedWorkflow(
		ctx,
		session,
		domain.SessionTypeMove,
		"cleanup move",
		options,
	)
}

func (s *Service) validatePVCIdentityWorkflowResume(
	ctx context.Context,
	session *domain.Session,
	typeName domain.SessionType,
	operation domain.Operation,
) error {
	if err := requireWorkflowSession(session, typeName, "resume "+string(operation)); err != nil {
		return err
	}

	phase, err := s.validateResumePrerequisites(ctx, session)
	if err != nil {
		return err
	}

	return s.validatePVCIdentityResume(ctx, session, phase, operation)
}

func (s *Service) resumePVCIdentityWorkflow(
	ctx context.Context,
	session *domain.Session,
	typeName domain.SessionType,
	operation domain.Operation,
) error {
	if err := requireWorkflowSession(session, typeName, "resume "+string(operation)); err != nil {
		return err
	}

	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		phase, err := persistedResumePhase(session)
		if err != nil {
			return err
		}

		return s.resumePVCIdentity(lockedCtx, session, phase, operation)
	})
}
