package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

// addReserveLifecycle attaches lifecycle commands owned exclusively by the
// storage-reservation workflow.
func (r *rootState) addReserveLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newReserveStatusCommand(),
		r.newReserveResumeCommand(),
		r.newReserveAbortCommand(),
		r.newReserveCleanupCommand(),
	)
}

func (r *rootState) newReserveStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use: "status [SESSION]", Short: "Show one reserve session or list all reserve sessions", Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			if len(args) == 1 {
				session, err := r.workflowSession(ctx, runtime, cmd, args[0], domain.SessionTypeReserve, "reserve status")
				if err != nil {
					return err
				}
				return printSessionResult(cmd, runtime, session)
			}
			return r.workflowSessionList(ctx, runtime, cmd, domain.SessionTypeReserve, "reserve")
		},
	}
}

func (r *rootState) newReserveResumeCommand() *cobra.Command {
	var dryRun bool
	command := &cobra.Command{
		Use: "resume SESSION", Short: "Continue a reserve session from its persisted phase", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			session, err := r.workflowSession(ctx, runtime, cmd, args[0], domain.SessionTypeReserve, "reserve resume")
			if err != nil {
				return err
			}
			if dryRun {
				if err := runtime.service.ValidateReserveResume(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}
			phase := sessionResumePhase(session)
			if requiresResumeApproval(phase) {
				if err := r.confirm(ctx, cmd, args[0]); err != nil {
					return reportApprovalError(cmd, err)
				}
			}
			if err := runtime.service.ResumeReserve(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}
			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)
	return command
}

func (r *rootState) newReserveAbortCommand() *cobra.Command {
	var dryRun bool
	command := &cobra.Command{
		Use: "abort SESSION", Short: "Abort a reserve session", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			session, err := r.workflowSession(ctx, runtime, cmd, args[0], domain.SessionTypeReserve, "reserve abort")
			if err != nil {
				return err
			}
			if dryRun {
				if err := runtime.service.ValidateReserveAbort(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}
			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}
			if err := runtime.service.AbortReserve(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}
			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)
	return command
}

func (r *rootState) newReserveCleanupCommand() *cobra.Command {
	var options app.CleanupOptions
	var dryRun bool
	command := &cobra.Command{
		Use: "cleanup SESSION", Short: "Clean up a reserve session", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			session, err := r.workflowSession(ctx, runtime, cmd, args[0], domain.SessionTypeReserve, "reserve cleanup")
			if err != nil {
				return err
			}
			if dryRun {
				if err := runtime.service.ValidateReserveCleanup(ctx, session, options); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}
			if options.DeleteTemporary || options.DeleteRollback || options.Finalize || options.DeleteSession {
				if err := r.confirm(ctx, cmd, args[0]); err != nil {
					return reportApprovalError(cmd, err)
				}
			}
			if err := runtime.service.CleanupReserve(ctx, session, options); err != nil {
				return reportCleanupError(cmd, session, options, err)
			}
			if options.DeleteSession {
				return printDeletedSession(cmd, args[0])
			}
			return printSessionResult(cmd, runtime, session)
		},
	}
	bindCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)
	return command
}
