package cli

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func (r *rootState) newSessionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "session",
		Short: "Inspect and recover persistent migration sessions",
	}
	command.AddCommand(
		r.newSessionStatusCommand(),
		r.newSessionResumeCommand(),
		r.newSessionAbortCommand(),
		r.newSessionRollbackCommand(),
		r.newSessionCleanupCommand(),
	)
	return command
}

func (r *rootState) newSessionStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
		Short: "Show one session or list all sessions",
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
					return err
				}
				return runtime.printer.Print(session)
			}
			sessions, err := runtime.store.List(ctx, r.global.sessionNamespace)
			if err != nil {
				return err
			}
			return runtime.printer.Print(sessions)
		},
	}
}

func (r *rootState) newSessionResumeCommand() *cobra.Command {
	var dryRun bool
	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue from the persisted idempotent stage",
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
				return err
			}
			if dryRun {
				if err := runtime.service.ValidateResume(ctx, session); err != nil {
					return err
				}
				return runtime.printer.Print(session)
			}
			phase := session.Status.Phase
			if phase == domain.PhaseFailed {
				phase = session.Status.ResumeFrom
			}
			if requiresResumeApproval(phase) {
				if err := r.confirm(cmd, args[0]); err != nil {
					return err
				}
			}
			if err := runtime.service.ResumeSession(ctx, session); err != nil {
				return err
			}
			return runtime.printer.Print(session)
		},
	}
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newSessionActionPlanCommand("resume", func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
		return runtime.service.ValidateResume(ctx, session)
	}))
	return command
}

func requiresResumeApproval(phase domain.Phase) bool {
	switch phase {
	case domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced,
		domain.PhaseActivating, domain.PhaseActivated, domain.PhaseResuming, domain.PhaseRollingBack,
		domain.PhaseRenaming, domain.PhaseMoving:
		return true
	default:
		return false
	}
}

func (r *rootState) newSessionAbortCommand() *cobra.Command {
	var dryRun bool
	command := &cobra.Command{
		Use:   "abort SESSION",
		Short: "Resume a paused workload and mark the migration aborted",
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
				return err
			}
			if dryRun {
				if err := runtime.service.ValidateAbort(ctx, session); err != nil {
					return err
				}
				return runtime.printer.Print(session)
			}
			if err := r.confirm(cmd, args[0]); err != nil {
				return err
			}
			if err := runtime.service.Abort(ctx, session); err != nil {
				return err
			}
			return runtime.printer.Print(session)
		},
	}
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newSessionActionPlanCommand("abort", func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
		return runtime.service.ValidateAbort(ctx, session)
	}))
	return command
}

func (r *rootState) newSessionRollbackCommand() *cobra.Command {
	var dryRun bool
	command := &cobra.Command{
		Use:   "rollback SESSION",
		Short: "Restore application PVC identities to retained source PVs",
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
				return err
			}
			if dryRun {
				if err := runtime.service.ValidateRollback(ctx, session); err != nil {
					return err
				}
				return runtime.printer.Print(session)
			}
			if err := r.confirm(cmd, args[0]); err != nil {
				return err
			}
			if err := runtime.service.Rollback(ctx, session); err != nil {
				return err
			}
			return runtime.printer.Print(session)
		},
	}
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newSessionActionPlanCommand("rollback", func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
		return runtime.service.ValidateRollback(ctx, session)
	}))
	return command
}

func (r *rootState) newSessionCleanupCommand() *cobra.Command {
	var options app.CleanupOptions
	var dryRun bool
	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Remove staged resources or finalize the rollback window",
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
				return err
			}
			if dryRun {
				if err := runtime.service.ValidateCleanup(ctx, session, options); err != nil {
					return err
				}
				return runtime.printer.Print(session)
			}
			if options.DeleteTemporary || options.DeleteRollback || options.Finalize || options.DeleteSession {
				if err := r.confirm(cmd, args[0]); err != nil {
					return err
				}
			}
			if err := runtime.service.Cleanup(ctx, session, options); err != nil {
				return err
			}
			if options.DeleteSession {
				return nil
			}
			return runtime.printer.Print(session)
		},
	}
	bindCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)
	planOptions := app.CleanupOptions{}
	plan := r.newSessionActionPlanCommand("cleanup", func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
		return runtime.service.ValidateCleanup(ctx, session, planOptions)
	})
	bindCleanupFlags(plan, &planOptions)
	command.AddCommand(plan)
	return command
}

func (r *rootState) newSessionActionPlanCommand(action string, validate func(context.Context, *commandRuntime, *domain.Session) error) *cobra.Command {
	return &cobra.Command{
		Use:   "plan SESSION",
		Short: "Validate session " + action + " without mutations",
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
				return err
			}
			if err := validate(ctx, runtime, session); err != nil {
				return err
			}
			return runtime.printer.Print(session)
		},
	}
}

func bindCleanupFlags(command *cobra.Command, options *app.CleanupOptions) {
	command.Flags().BoolVar(&options.DeleteTemporary, "delete-temporary", false, "Delete retained staged PVCs owned by the session")
	command.Flags().BoolVar(&options.DeleteRollback, "delete-rollback-pv", false, "Delete Released rollback PV objects; Retain preserves backend data")
	command.Flags().BoolVar(&options.Finalize, "finalize", false, "Restore the active PV's recorded reclaim policy")
	command.Flags().BoolVar(&options.DeleteSession, "delete-session", false, "Delete the session ConfigMap after cleanup")
}
