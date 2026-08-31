package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func (r *rootState) newPodMigrationStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
		Short: "Show one real-time Pod migration session or list all Pod migrations",
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
					domain.SessionTypeMigratePod,
					"migrate-pod status",
				); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			sessions, err := runtime.store.List(ctx, r.global.sessionNamespace)
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, "", err)
			}

			sessions = filterSessionsByType(sessions, domain.SessionTypeMigratePod)
			if err := runtime.printer.Print(sessions); err != nil {
				return err
			}

			return writeSessionListGuidance(
				cmd.ErrOrStderr(),
				r.global.sessionNamespace,
				sessions,
				sessionCommandPrefixForCommand(cmd, r.global.sessionNamespace),
				"migrate-pod",
			)
		},
	}
}

func (r *rootState) newPodMigrationResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue a real-time Pod migration from its persisted phase",
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
				domain.SessionTypeMigratePod,
				"migrate-pod resume",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidatePodMigrationResume(ctx, session); err != nil {
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

			if deferred, err := deferControllerExecution(ctx, cmd, runtime, session); deferred {
				return err
			}

			if err := runtime.service.ResumePodMigration(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort SESSION",
		Short: "Stop a real-time Pod migration before cutover and retain staged storage",
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
				domain.SessionTypeMigratePod,
				"migrate-pod abort",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidatePodMigrationAbort(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.AbortPodMigration(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationRollbackCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "rollback SESSION",
		Short: "Restore source PV bindings after real-time Pod migration cutover",
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
				domain.SessionTypeMigratePod,
				"migrate-pod rollback",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidatePodMigrationRollback(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.RollbackPodMigration(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationCleanupCommand() *cobra.Command {
	var (
		options app.CleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Delete retained real-time Pod migration resources or close its rollback window",
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
				domain.SessionTypeMigratePod,
				"migrate-pod cleanup",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidatePodMigrationCleanup(
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

			if err := runtime.service.CleanupPodMigration(ctx, session, options); err != nil {
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
