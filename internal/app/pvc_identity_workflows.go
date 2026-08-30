package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateRenameResume(ctx context.Context, session *domain.Session) error {
	return s.validatePVCIdentityWorkflowResume(ctx, session, domain.SessionTypeRename, domain.OperationRename)
}

func (s *Service) ResumeRename(ctx context.Context, session *domain.Session) error {
	return s.resumePVCIdentityWorkflow(ctx, session, domain.SessionTypeRename, domain.OperationRename)
}

func (s *Service) ValidateRenameAbort(ctx context.Context, session *domain.Session) error {
	return s.validatePVCIdentityWorkflowAbort(ctx, session, domain.SessionTypeRename, "abort rename")
}

func (s *Service) AbortRename(ctx context.Context, session *domain.Session) error {
	return s.abortPVCIdentityWorkflow(ctx, session, domain.SessionTypeRename, "abort rename")
}

func (s *Service) ValidateRenameRollback(ctx context.Context, session *domain.Session) error {
	return s.validatePVCIdentityWorkflowRollback(ctx, session, domain.SessionTypeRename, "rollback rename")
}

func (s *Service) RollbackRename(ctx context.Context, session *domain.Session) error {
	return s.rollbackPVCIdentityWorkflow(ctx, session, domain.SessionTypeRename, "rollback rename")
}

func (s *Service) ValidateRenameCleanup(ctx context.Context, session *domain.Session, options CleanupOptions) error {
	return s.validatePVCIdentityWorkflowCleanup(ctx, session, domain.SessionTypeRename, "cleanup rename", options)
}

func (s *Service) CleanupRename(ctx context.Context, session *domain.Session, options CleanupOptions) error {
	return s.cleanupPVCIdentityWorkflow(ctx, session, domain.SessionTypeRename, "cleanup rename", options)
}

func (s *Service) ValidateMoveResume(ctx context.Context, session *domain.Session) error {
	return s.validatePVCIdentityWorkflowResume(ctx, session, domain.SessionTypeMove, domain.OperationMove)
}

func (s *Service) ResumeMove(ctx context.Context, session *domain.Session) error {
	return s.resumePVCIdentityWorkflow(ctx, session, domain.SessionTypeMove, domain.OperationMove)
}

func (s *Service) ValidateMoveAbort(ctx context.Context, session *domain.Session) error {
	return s.validatePVCIdentityWorkflowAbort(ctx, session, domain.SessionTypeMove, "abort move")
}

func (s *Service) AbortMove(ctx context.Context, session *domain.Session) error {
	return s.abortPVCIdentityWorkflow(ctx, session, domain.SessionTypeMove, "abort move")
}

func (s *Service) ValidateMoveRollback(ctx context.Context, session *domain.Session) error {
	return s.validatePVCIdentityWorkflowRollback(ctx, session, domain.SessionTypeMove, "rollback move")
}

func (s *Service) RollbackMove(ctx context.Context, session *domain.Session) error {
	return s.rollbackPVCIdentityWorkflow(ctx, session, domain.SessionTypeMove, "rollback move")
}

func (s *Service) ValidateMoveCleanup(ctx context.Context, session *domain.Session, options CleanupOptions) error {
	return s.validatePVCIdentityWorkflowCleanup(ctx, session, domain.SessionTypeMove, "cleanup move", options)
}

func (s *Service) CleanupMove(ctx context.Context, session *domain.Session, options CleanupOptions) error {
	return s.cleanupPVCIdentityWorkflow(ctx, session, domain.SessionTypeMove, "cleanup move", options)
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

func (s *Service) validatePVCIdentityWorkflowAbort(
	ctx context.Context,
	session *domain.Session,
	typeName domain.SessionType,
	action string,
) error {
	if err := requireWorkflowSession(session, typeName, action); err != nil {
		return err
	}
	return s.validateAbort(ctx, session)
}

func (s *Service) abortPVCIdentityWorkflow(
	ctx context.Context,
	session *domain.Session,
	typeName domain.SessionType,
	action string,
) error {
	if err := requireWorkflowSession(session, typeName, action); err != nil {
		return err
	}
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.abort(lockedCtx, session)
	})
}

func (s *Service) validatePVCIdentityWorkflowRollback(
	ctx context.Context,
	session *domain.Session,
	typeName domain.SessionType,
	action string,
) error {
	if err := requireWorkflowSession(session, typeName, action); err != nil {
		return err
	}
	return s.validateRollback(ctx, session)
}

func (s *Service) rollbackPVCIdentityWorkflow(
	ctx context.Context,
	session *domain.Session,
	typeName domain.SessionType,
	action string,
) error {
	if err := requireWorkflowSession(session, typeName, action); err != nil {
		return err
	}
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.rollback(lockedCtx, session)
	})
}

func (s *Service) validatePVCIdentityWorkflowCleanup(
	ctx context.Context,
	session *domain.Session,
	typeName domain.SessionType,
	action string,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(session, typeName, action); err != nil {
		return err
	}
	return s.validateCleanup(ctx, session, options)
}

func (s *Service) cleanupPVCIdentityWorkflow(
	ctx context.Context,
	session *domain.Session,
	typeName domain.SessionType,
	action string,
	options CleanupOptions,
) error {
	if err := requireWorkflowSession(session, typeName, action); err != nil {
		return err
	}
	return s.cleanupWorkflow(ctx, session, options)
}
