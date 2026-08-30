package cli

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

// workflowSession loads and type-checks a session for an operation-owned
// lifecycle command. It keeps Kubernetes lookup/error formatting mechanical;
// each operation still owns the command and service method that follows.
func (r *rootState) workflowSession(
	ctx context.Context,
	runtime *commandRuntime,
	cmd *cobra.Command,
	id string,
	expected domain.SessionType,
	action string,
) (*domain.Session, error) {
	session, err := runtime.store.Get(ctx, r.global.sessionNamespace, id)
	if err != nil {
		return nil, reportSessionLookupError(cmd, r.global.sessionNamespace, id, err)
	}
	if err := requireCLISessionType(session, expected, action); err != nil {
		return nil, reportSessionError(cmd, session, err)
	}
	return session, nil
}

func (r *rootState) workflowSessionList(
	ctx context.Context,
	runtime *commandRuntime,
	cmd *cobra.Command,
	typeName domain.SessionType,
	name string,
) error {
	sessions, err := runtime.store.List(ctx, r.global.sessionNamespace)
	if err != nil {
		return reportSessionLookupError(cmd, r.global.sessionNamespace, "", err)
	}
	sessions = filterSessionsByType(sessions, typeName)
	if err := runtime.printer.Print(sessions); err != nil {
		return err
	}
	return writeSessionListGuidance(
		cmd.ErrOrStderr(),
		r.global.sessionNamespace,
		sessions,
		sessionCommandPrefixForCommand(cmd, r.global.sessionNamespace),
		name,
	)
}

func requireCLISessionType(session *domain.Session, expected domain.SessionType, action string) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, action, "session is nil")
	}
	if session.Spec.Type != expected {
		return domain.NewError(
			domain.ErrorPrecondition,
			action,
			fmt.Sprintf("requires a %s session, got %s", expected, session.Spec.Type),
		)
	}
	return nil
}

func filterSessionsByType(sessions []*domain.Session, typeName domain.SessionType) []*domain.Session {
	filtered := make([]*domain.Session, 0, len(sessions))
	for _, session := range sessions {
		if session != nil && session.Spec.Type == typeName {
			filtered = append(filtered, session)
		}
	}
	return filtered
}

func sessionResumePhase(session *domain.Session) domain.Phase {
	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}
	return phase
}

func requiresResumeApproval(phase domain.Phase) bool {
	switch phase {
	case domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing,
		domain.PhaseFinalSynced, domain.PhaseActivating, domain.PhaseActivated,
		domain.PhaseResuming, domain.PhaseRollingBack, domain.PhaseRenaming,
		domain.PhaseMoving, domain.PhaseAborting:
		return true
	default:
		return false
	}
}

func requiresOperationResumeApproval(operation domain.Operation, phase domain.Phase) bool {
	return phase == domain.PhasePlanned && operation.RebindsPVC()
}

func bindCleanupFlags(command *cobra.Command, options *app.CleanupOptions) {
	command.Flags().BoolVar(&options.DeleteTemporary, "delete-temporary", false, "Delete retained staged PVCs owned by the session")
	command.Flags().BoolVar(&options.DeleteRollback, "delete-rollback-pv", false, "Restore each Released rollback PV's recorded reclaim policy, then delete it")
	command.Flags().BoolVar(&options.Finalize, "finalize", false, "Restore the active PV's recorded reclaim policy")
	command.Flags().BoolVar(&options.DeleteSession, "delete-session", false, "Delete the session ConfigMap after cleanup")
}

func bindIdentityCleanupFlags(command *cobra.Command, options *app.CleanupOptions) {
	command.Flags().BoolVar(&options.Finalize, "finalize", false, "Restore the active PV's recorded reclaim policy")
	command.Flags().BoolVar(&options.DeleteSession, "delete-session", false, "Delete the session ConfigMap after cleanup")
}

func printDeletedSession(cmd *cobra.Command, id string) error {
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"session %s cleanup completed; the session ConfigMap and Lease were deleted, and active workload storage was preserved\n",
		id,
	)
	return err
}
