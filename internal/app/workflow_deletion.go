package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FinalizeDeletedWorkflow converges storage before releasing CR protection.
// The same Lease covers recovery and cleanup so CLI lifecycle operations cannot
// intervene between the two steps.
func (s *Service) FinalizeDeletedWorkflow(ctx context.Context, session *domain.Session) error {
	ctx = context.WithValue(ctx, workflowDeletionContextKey{}, true)

	return s.withSessionLock(ctx, session, func(ctx context.Context) error {
		store, ok := s.store.(kube.ControllerSessionStore)
		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"finalize workflow",
				"controller session store is required",
			)
		}

		latest, err := store.GetByKind(
			ctx,
			session.Spec.SessionNamespace,
			session.ID,
			session.BackendResource,
		)
		if err != nil {
			return err
		}

		if latest.BackendUID != session.BackendUID || !latest.Deleting {
			return domain.NewError(
				domain.ErrorConflict,
				"finalize workflow",
				"workflow identity or deletion intent changed",
			)
		}

		*session = *latest
		if err := s.verifyBackupToolsStopped(ctx, session); err != nil {
			return err
		}

		phase := session.Status.Phase
		if phase == domain.PhaseFailed {
			phase = session.Status.ResumeFrom
		}

		reason := "Cancelling"
		if deletionRequiresConvergence(phase) {
			reason = "ConvergingStorage"
		} else if cleanupPhaseAllowed(session) {
			reason = "CleaningUp"
		}

		session.SetCondition(domain.Condition{
			Type:               "Deleting",
			Status:             metav1.ConditionTrue,
			Reason:             reason,
			Message:            "Recovering workload and cleaning workflow resources before deletion",
			LastTransitionTime: metav1.Now(),
		})

		if err := s.persist(ctx, session); err != nil {
			return err
		}

		if !cleanupPhaseAllowed(session) {
			if err := s.cleanupInterruptedCopy(ctx, session, phase); err != nil {
				return err
			}

			if deletionRequiresConvergence(phase) {
				switch session.Spec.Type {
				case domain.SessionTypeMigrate:
					err = s.ResumeOfflineMigration(ctx, session)
				case domain.SessionTypeMigratePod:
					err = s.ResumePodMigration(ctx, session)
				case domain.SessionTypeRename:
					err = s.ResumeRename(ctx, session)
				case domain.SessionTypeMove:
					err = s.ResumeMove(ctx, session)
				default:
					err = domain.NewError(
						domain.ErrorPrecondition,
						"finalize workflow",
						"workflow requires recovery before deletion",
					)
				}
			} else {
				err = s.abort(ctx, session)
			}

			if err != nil {
				return err
			}
		}

		options := CleanupOptions{
			DeleteTemporary: true,
			DeleteRollback:  true,
			Finalize:        true,
			DeleteSession:   true,
		}
		if session.Spec.Operation() == domain.OperationCopy &&
			session.Status.Phase == domain.PhaseWarmCopied {
			options.DeleteTemporary = false
			options.DeleteRollback = false
		}

		return s.cleanup(ctx, session, options)
	})
}

type workflowDeletionContextKey struct{}

func deletionRequiresConvergence(phase domain.Phase) bool {
	switch phase {
	case domain.PhaseActivating,
		domain.PhaseActivated,
		domain.PhaseResuming,
		domain.PhaseRenaming,
		domain.PhaseMoving,
		domain.PhaseRollingBack:
		return true
	default:
		return false
	}
}
