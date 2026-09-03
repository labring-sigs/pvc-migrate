package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

// addMoveLifecycle attaches lifecycle commands owned by cluster-scoped PVC
// moves. Tenant-scoped rename has a separate command module.
func (r *rootState) addMoveLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newMoveStatusCommand(),
		r.newMoveResumeCommand(),
		r.newMoveAbortCommand(),
		r.newMoveRollbackCommand(),
		r.newMoveCleanupCommand(),
	)
}

func (r *rootState) newMoveStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
		Short: "Show one move session or list all move sessions",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				session, err := r.workflowSession(
					ctx,
					runtime,
					cmd,
					args[0],
					domain.SessionTypeMove,
					"move status",
				)
				if err != nil {
					return err
				}

				return printSessionResult(cmd, runtime, session)
			}

			return r.workflowSessionList(ctx, runtime, cmd, domain.SessionTypeMove, "move")
		},
	}
}

func (r *rootState) newMoveResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue a move session from its persisted phase",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := r.workflowSession(
				ctx,
				runtime,
				cmd,
				args[0],
				domain.SessionTypeMove,
				"move resume",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateMoveResume(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if deferred, err := deferControllerExecution(ctx, cmd, runtime, session); deferred {
				return err
			}

			if err := runtime.service.ResumeMove(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newMoveAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use: "abort SESSION", Short: "Abort a move session", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := r.workflowSession(
				ctx,
				runtime,
				cmd,
				args[0],
				domain.SessionTypeMove,
				"move abort",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateMoveAbort(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.AbortMove(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newMoveRollbackCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use: "rollback SESSION", Short: "Roll back a move session", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := r.workflowSession(
				ctx,
				runtime,
				cmd,
				args[0],
				domain.SessionTypeMove,
				"move rollback",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateMoveRollback(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.RollbackMove(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newMoveCleanupCommand() *cobra.Command {
	var (
		options app.CleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use: "cleanup SESSION", Short: "Clean up a move session", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := r.workflowSession(
				ctx,
				runtime,
				cmd,
				args[0],
				domain.SessionTypeMove,
				"move cleanup",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateMoveCleanup(ctx, session, options); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if options.Finalize || options.DeleteSession {
				if err := r.confirm(ctx, cmd, args[0]); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			if err := runtime.service.CleanupMove(ctx, session, options); err != nil {
				return reportCleanupError(cmd, session, options, err)
			}

			if options.DeleteSession {
				return printDeletedSession(cmd, session)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindIdentityCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)

	return command
}
