package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func (r *rootState) newOfflineMigrationStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
		Short: "Show one offline migration session or list all offline migrations",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				session, err := runtime.store.Get(ctx, r.global.sessionNamespace, args[0])
				if err != nil {
					return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
				}

				if err := requireCLISessionType(
					session,
					domain.SessionTypeMigrate,
					"migrate status",
				); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			sessions, err := runtime.store.List(ctx, r.global.sessionNamespace)
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, "", err)
			}

			sessions = filterSessionsByType(sessions, domain.SessionTypeMigrate)
			if err := runtime.printer.Print(sessions); err != nil {
				return err
			}

			return writeSessionListGuidance(
				cmd.ErrOrStderr(),
				r.global.sessionNamespace,
				sessions,
				sessionCommandPrefixForCommand(cmd, r.global.sessionNamespace),
				"migrate",
			)
		},
	}
}

func (r *rootState) newOfflineMigrationResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue an offline migration from its persisted phase",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := runtime.store.Get(ctx, r.global.sessionNamespace, args[0])
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
			}

			if err := requireCLISessionType(
				session,
				domain.SessionTypeMigrate,
				"migrate resume",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidateOfflineMigrationResume(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			phase := sessionResumePhase(session)
			if requiresResumeApproval(phase) ||
				requiresOperationResumeApproval(session.Spec.Operation(), phase) {
				if err := r.confirm(ctx, cmd, args[0]); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			if deferred, err := deferControllerExecution(ctx, cmd, runtime, session); deferred {
				return err
			}

			if err := runtime.service.ResumeOfflineMigration(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newOfflineMigrationAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort SESSION",
		Short: "Stop an offline migration before cutover and retain staged storage",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := runtime.store.Get(ctx, r.global.sessionNamespace, args[0])
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
			}

			if err := requireCLISessionType(
				session,
				domain.SessionTypeMigrate,
				"migrate abort",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidateOfflineMigrationAbort(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.AbortOfflineMigration(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newOfflineMigrationRollbackCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "rollback SESSION",
		Short: "Restore source PV bindings after offline migration cutover",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := runtime.store.Get(ctx, r.global.sessionNamespace, args[0])
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
			}

			if err := requireCLISessionType(
				session,
				domain.SessionTypeMigrate,
				"migrate rollback",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidateOfflineMigrationRollback(
					ctx,
					session,
				); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.RollbackOfflineMigration(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newOfflineMigrationCleanupCommand() *cobra.Command {
	var (
		options app.CleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Delete retained offline migration resources or close its rollback window",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := runtime.store.Get(ctx, r.global.sessionNamespace, args[0])
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
			}

			if err := requireCLISessionType(
				session,
				domain.SessionTypeMigrate,
				"migrate cleanup",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidateOfflineMigrationCleanup(
					ctx,
					session,
					options,
				); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			if options.DeleteTemporary || options.DeleteRollback || options.Finalize ||
				options.DeleteSession {
				if err := r.confirm(ctx, cmd, args[0]); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			if err := runtime.service.CleanupOfflineMigration(ctx, session, options); err != nil {
				return reportCleanupError(cmd, session, options, err)
			}

			if options.DeleteSession {
				return printDeletedSession(cmd, session)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)

	return command
}
