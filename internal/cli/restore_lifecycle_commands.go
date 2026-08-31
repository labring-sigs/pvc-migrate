package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func (r *rootState) newRestoreStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
		Short: "Show one restore session or list all restores",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				session, getErr := runtime.store.Get(ctx, r.global.sessionNamespace, args[0])
				if getErr != nil {
					return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], getErr)
				}

				if typeErr := requireCLISessionType(
					session,
					domain.SessionTypeRestore,
					"restore status",
				); typeErr != nil {
					return reportSessionError(cmd, session, typeErr)
				}

				return printSessionResult(cmd, runtime, session)
			}

			sessions, listErr := runtime.store.List(ctx, r.global.sessionNamespace)
			if listErr != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, "", listErr)
			}

			sessions = filterSessionsByType(sessions, domain.SessionTypeRestore)
			if err := runtime.printer.Print(sessions); err != nil {
				return err
			}

			return writeSessionListGuidance(
				cmd.ErrOrStderr(),
				r.global.sessionNamespace,
				sessions,
				sessionCommandPrefixForCommand(cmd, r.global.sessionNamespace),
				"restore",
			)
		},
	}
}

func (r *rootState) newRestoreResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue a restore session from its persisted phase",
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
				domain.SessionTypeRestore,
				"restore resume",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				request := r.backupResumeRequest(runtime)
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

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if deferred, err := deferControllerExecution(ctx, cmd, runtime, session); deferred {
				return err
			}

			request := r.backupResumeRequest(runtime)
			if err := backup.Resume(ctx, runtime.clients.Kubernetes, request, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRestoreAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort SESSION",
		Short: "Abort a restore session",
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
				domain.SessionTypeRestore,
				"restore abort",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidateRestoreAbort(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.AbortRestore(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRestoreCleanupCommand() *cobra.Command {
	var (
		options app.CleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Clean up a completed or aborted restore session",
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
				domain.SessionTypeRestore,
				"restore cleanup",
			); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if dryRun {
				if err := runtime.service.ValidateRestoreCleanup(
					ctx,
					session,
					options,
				); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			if options.Finalize || options.DeleteSession {
				if err := r.confirm(ctx, cmd, args[0]); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			if err := runtime.service.CleanupRestore(ctx, session, options); err != nil {
				return reportSessionError(cmd, session, err)
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
