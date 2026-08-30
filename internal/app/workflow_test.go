package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) validateResumeWorkflowForTest(
	ctx context.Context,
	session *domain.Session,
) error {
	switch session.Spec.Type {
	case domain.SessionTypeMigrate:
		return s.ValidateOfflineMigrationResume(ctx, session)
	case domain.SessionTypeMigratePod:
		return s.ValidatePodMigrationResume(ctx, session)
	case domain.SessionTypeReserve:
		return s.ValidateReserveResume(ctx, session)
	case domain.SessionTypeCopy:
		return s.ValidateCopyResume(ctx, session)
	case domain.SessionTypeRename:
		return s.ValidateRenameResume(ctx, session)
	case domain.SessionTypeMove:
		return s.ValidateMoveResume(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"test resume",
			"workflow has no app resume API",
		)
	}
}

func (s *Service) resumeWorkflowForTest(ctx context.Context, session *domain.Session) error {
	switch session.Spec.Type {
	case domain.SessionTypeMigrate:
		return s.ResumeOfflineMigration(ctx, session)
	case domain.SessionTypeMigratePod:
		return s.ResumePodMigration(ctx, session)
	case domain.SessionTypeReserve:
		return s.ResumeReserve(ctx, session)
	case domain.SessionTypeCopy:
		return s.ResumeCopy(ctx, session)
	case domain.SessionTypeRename:
		return s.ResumeRename(ctx, session)
	case domain.SessionTypeMove:
		return s.ResumeMove(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"test resume",
			"workflow has no app resume API",
		)
	}
}

func (s *Service) validateAbortWorkflowForTest(ctx context.Context, session *domain.Session) error {
	switch session.Spec.Type {
	case domain.SessionTypeMigrate:
		return s.ValidateOfflineMigrationAbort(ctx, session)
	case domain.SessionTypeMigratePod:
		return s.ValidatePodMigrationAbort(ctx, session)
	case domain.SessionTypeReserve:
		return s.ValidateReserveAbort(ctx, session)
	case domain.SessionTypeCopy:
		return s.ValidateCopyAbort(ctx, session)
	case domain.SessionTypeBackup:
		return s.ValidateBackupAbort(ctx, session)
	case domain.SessionTypeRename:
		return s.ValidateRenameAbort(ctx, session)
	case domain.SessionTypeMove:
		return s.ValidateMoveAbort(ctx, session)
	default:
		return domain.NewError(domain.ErrorPrecondition, "test abort", "workflow has no abort API")
	}
}

func (s *Service) abortWorkflowForTest(ctx context.Context, session *domain.Session) error {
	switch session.Spec.Type {
	case domain.SessionTypeMigrate:
		return s.AbortOfflineMigration(ctx, session)
	case domain.SessionTypeMigratePod:
		return s.AbortPodMigration(ctx, session)
	case domain.SessionTypeReserve:
		return s.AbortReserve(ctx, session)
	case domain.SessionTypeCopy:
		return s.AbortCopy(ctx, session)
	case domain.SessionTypeBackup:
		return s.AbortBackup(ctx, session)
	case domain.SessionTypeRename:
		return s.AbortRename(ctx, session)
	case domain.SessionTypeMove:
		return s.AbortMove(ctx, session)
	default:
		return domain.NewError(domain.ErrorPrecondition, "test abort", "workflow has no abort API")
	}
}

func (s *Service) validateRollbackWorkflowForTest(
	ctx context.Context,
	session *domain.Session,
) error {
	switch session.Spec.Type {
	case domain.SessionTypeMigrate:
		return s.ValidateOfflineMigrationRollback(ctx, session)
	case domain.SessionTypeMigratePod:
		return s.ValidatePodMigrationRollback(ctx, session)
	case domain.SessionTypeRename:
		return s.ValidateRenameRollback(ctx, session)
	case domain.SessionTypeMove:
		return s.ValidateMoveRollback(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"test rollback",
			"workflow has no rollback API",
		)
	}
}

func (s *Service) rollbackWorkflowForTest(ctx context.Context, session *domain.Session) error {
	switch session.Spec.Type {
	case domain.SessionTypeMigrate:
		return s.RollbackOfflineMigration(ctx, session)
	case domain.SessionTypeMigratePod:
		return s.RollbackPodMigration(ctx, session)
	case domain.SessionTypeRename:
		return s.RollbackRename(ctx, session)
	case domain.SessionTypeMove:
		return s.RollbackMove(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"test rollback",
			"workflow has no rollback API",
		)
	}
}

func (s *Service) validateCleanupWorkflowForTest(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.validateCleanup(ctx, session, options)
}

func (s *Service) cleanupWorkflowForTest(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.cleanupWorkflow(ctx, session, options)
}
