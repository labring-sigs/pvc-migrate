package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func (r *rootState) newBackupStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
		Short: "Show one backup session or list all backups",
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
					domain.SessionTypeBackup,
					"backup status",
				)
				if err != nil {
					return err
				}

				return printSessionResult(cmd, runtime, session)
			}

			return r.workflowSessionList(ctx, runtime, cmd, domain.SessionTypeBackup, "backup")
		},
	}
}

func (r *rootState) newBackupResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue a backup session from its persisted phase",
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
				domain.SessionTypeBackup,
				"backup resume",
			)
			if err != nil {
				return err
			}

			request := r.backupResumeRequest(runtime)
			if dryRun {
				if err := backup.ValidateResume(
					ctx,
					runtime.clients.Kubernetes,
					request,
					session,
				); err != nil {
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

			if err := backup.Resume(ctx, runtime.clients.Kubernetes, request, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newBackupAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort SESSION",
		Short: "Abort a backup session without publishing a recovery point",
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
				domain.SessionTypeBackup,
				"backup abort",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateBackupAbort(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.AbortBackup(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newBackupCleanupCommand() *cobra.Command {
	var (
		options app.CleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Delete backup session credentials and metadata",
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
				domain.SessionTypeBackup,
				"backup cleanup",
			)
			if err != nil {
				return err
			}

			if dryRun {
				if err := runtime.service.ValidateBackupCleanup(ctx, session, options); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if options.Finalize || options.DeleteSession {
				if err := r.confirm(ctx, cmd, args[0]); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			if err := runtime.service.CleanupBackup(ctx, session, options); err != nil {
				return reportCleanupError(cmd, session, options, err)
			}

			if options.DeleteSession {
				return printDeletedSession(cmd, session)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindBackupCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)

	return command
}

func bindBackupCleanupFlags(command *cobra.Command, options *app.CleanupOptions) {
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Delete the backup credentials Secret")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the backup session ConfigMap after credentials cleanup")
}
