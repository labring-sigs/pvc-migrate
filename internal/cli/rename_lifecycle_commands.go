package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

// addRenameLifecycle attaches lifecycle commands owned by same-namespace PVC
// rename. Cross-namespace move has a separate command module.
func (r *rootState) addRenameLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newRenameStatusCommand(),
		r.newRenameResumeCommand(),
		r.newRenameAbortCommand(),
		r.newRenameRollbackCommand(),
		r.newRenameCleanupCommand(),
	)
}

func (r *rootState) newRenameStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
		Short: "Show one rename session or list all rename sessions",
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
					domain.SessionTypeRename,
					"rename status",
				)
				if err != nil {
					return err
				}

				return printSessionResult(cmd, runtime, session)
			}

			return r.workflowSessionList(ctx, runtime, cmd, domain.SessionTypeRename, "rename")
		},
	}
}

func (r *rootState) newRenameResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue a rename session from its persisted phase",
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
				domain.SessionTypeRename,
				"rename resume",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateRenameResume(ctx, session); err != nil {
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

			if err := runtime.service.ResumeRename(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRenameAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use: "abort SESSION", Short: "Abort a rename session", Args: cobra.ExactArgs(1),
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
				domain.SessionTypeRename,
				"rename abort",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateRenameAbort(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.AbortRename(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRenameRollbackCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use: "rollback SESSION", Short: "Roll back a rename session", Args: cobra.ExactArgs(1),
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
				domain.SessionTypeRename,
				"rename rollback",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateRenameRollback(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.RollbackRename(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRenameCleanupCommand() *cobra.Command {
	var (
		options app.CleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use: "cleanup SESSION", Short: "Clean up a rename session", Args: cobra.ExactArgs(1),
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
				domain.SessionTypeRename,
				"rename cleanup",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateRenameCleanup(ctx, session, options); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if options.Finalize || options.DeleteSession {
				if err := r.confirm(ctx, cmd, args[0]); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			if err := runtime.service.CleanupRename(ctx, session, options); err != nil {
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
