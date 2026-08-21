package cli

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
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
		r.newSessionOrphanCleanupCommand(),
	)

	return command
}

func (r *rootState) newSessionStatusCommand() *cobra.Command {
	command := &cobra.Command{
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
					return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			sessions, err := runtime.store.List(ctx, r.global.sessionNamespace)
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, "", err)
			}

			if err := runtime.printer.Print(sessions); err != nil {
				_ = writeSessionListGuidance(
					cmd.ErrOrStderr(),
					r.global.sessionNamespace,
					sessions,
					sessionCommandPrefixForCommand(cmd, r.global.sessionNamespace),
				)

				return err
			}

			return writeSessionListGuidance(
				cmd.ErrOrStderr(),
				r.global.sessionNamespace,
				sessions,
				sessionCommandPrefixForCommand(cmd, r.global.sessionNamespace),
			)
		},
	}
	command.AddCommand(r.newCrossClusterSessionStatusCommand())

	return command
}

func (r *rootState) newCrossClusterSessionStatusCommand() *cobra.Command {
	flags := &crossClusterFlags{}
	command := &cobra.Command{
		Use:   "cross-cluster SESSION",
		Short: "Show a cross-cluster copy session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.sessionID = args[0]

			service, err := r.crossClusterService(flags)
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := service.Get(ctx, flags.sessionNamespace, args[0])
			if err != nil {
				return err
			}

			return r.crossPrinter().Print(session)
		},
	}
	flags.bindConnections(command, r)

	return command
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
				return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
			}

			if dryRun {
				if session.Spec.Type == domain.SessionTypeBackup {
					err := backup.ValidateResume(
						ctx,
						runtime.clients.Kubernetes,
						r.backupResumeRequest(runtime),
						session,
					)
					if err != nil {
						return reportSessionError(cmd, session, err)
					}

					return printSessionResult(cmd, runtime, session)
				}

				if err := runtime.service.ValidateResume(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			phase := session.Status.Phase
			if phase == domain.PhaseFailed {
				phase = session.Status.ResumeFrom
			}

			if (session.Spec.Type == domain.SessionTypeBackup && phase != domain.PhaseCompleted) ||
				requiresResumeApproval(phase) ||
				requiresOperationResumeApproval(session.Spec.Operation(), phase) {
				if err := r.confirm(ctx, cmd, args[0]); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			if session.Spec.Type == domain.SessionTypeBackup {
				err = backup.Resume(
					ctx,
					runtime.clients.Kubernetes,
					r.backupResumeRequest(runtime),
					session,
				)
			} else {
				err = runtime.service.ResumeSession(ctx, session)
			}

			if err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)
	command.AddCommand(
		r.newSessionActionPlanCommand(
			"resume",
			func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
				if session.Spec.Type == domain.SessionTypeBackup {
					return backup.ValidateResume(
						ctx,
						runtime.clients.Kubernetes,
						r.backupResumeRequest(runtime),
						session,
					)
				}

				return runtime.service.ValidateResume(ctx, session)
			},
		),
	)
	command.AddCommand(r.newCrossClusterSessionResumeCommand())

	return command
}

func (r *rootState) backupResumeRequest(runtime *commandRuntime) backup.Request {
	return backup.Request{
		HelmTimeout:        r.global.helmTimeout,
		KubeconfigPath:     r.global.kubeconfig,
		KubeContext:        r.global.kubeContext,
		StreamToolLogs:     r.global.streamToolLogs,
		StructuredLogs:     r.global.logFormat == "json",
		Writer:             r.errWriter(),
		Logger:             runtime.logger,
		ToolImageProber:    kube.NewToolImageProber(runtime.clients.Kubernetes),
		SessionStore:       runtime.store,
		SessionNamespace:   r.global.sessionNamespace,
		OpenEBSLVMManager:  runtime.openEBSLVMSharedVolumeManager,
		ObjectStoreFactory: r.options.objectStoreFactory,
	}
}

func requiresResumeApproval(phase domain.Phase) bool {
	switch phase {
	case domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced,
		domain.PhaseActivating, domain.PhaseActivated, domain.PhaseResuming, domain.PhaseRollingBack,
		domain.PhaseRenaming, domain.PhaseMoving, domain.PhaseAborting:
		return true
	default:
		return false
	}
}

func (r *rootState) newCrossClusterSessionResumeCommand() *cobra.Command {
	flags := &crossClusterFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "cross-cluster SESSION",
		Short: "Resume a cross-cluster copy session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.sessionID = args[0]

			service, err := r.crossClusterService(flags)
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := service.Get(ctx, flags.sessionNamespace, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				return r.crossPrinter().Print(session)
			}

			if err := service.Copy(
				ctx,
				session,
				r.global.retries,
				r.global.noCompress,
			); err != nil {
				return err
			}

			return r.crossPrinter().Print(session)
		},
	}
	flags.bindConnections(command, r)
	bindDryRun(command, &dryRun)

	return command
}

func requiresOperationResumeApproval(operation domain.Operation, phase domain.Phase) bool {
	return phase == domain.PhasePlanned && operation.RebindsPVC()
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
				return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
			}

			if dryRun {
				if err := runtime.service.ValidateAbort(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.Abort(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)
	command.AddCommand(
		r.newSessionActionPlanCommand(
			"abort",
			func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
				return runtime.service.ValidateAbort(ctx, session)
			},
		),
	)

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
				return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
			}

			if dryRun {
				if err := runtime.service.ValidateRollback(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := runtime.service.Rollback(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindDryRun(command, &dryRun)
	command.AddCommand(
		r.newSessionActionPlanCommand(
			"rollback",
			func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
				return runtime.service.ValidateRollback(ctx, session)
			},
		),
	)

	return command
}

func (r *rootState) newSessionCleanupCommand() *cobra.Command {
	var (
		options app.CleanupOptions
		dryRun  bool
	)

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
				return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
			}

			if dryRun {
				if err := runtime.service.ValidateCleanup(ctx, session, options); err != nil {
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

			if err := runtime.service.Cleanup(ctx, session, options); err != nil {
				return reportCleanupError(cmd, session, options, err)
			}

			if options.DeleteSession {
				_, err := fmt.Fprintf(
					cmd.ErrOrStderr(),
					"session %s cleanup completed; the session ConfigMap and Lease were deleted, and active workload storage was preserved\n",
					args[0],
				)

				return err
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	bindCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newCrossClusterCleanupCommand())

	planOptions := app.CleanupOptions{}
	plan := r.newSessionActionPlanCommand(
		"cleanup",
		func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
			return runtime.service.ValidateCleanup(ctx, session, planOptions)
		},
	)
	bindCleanupFlags(plan, &planOptions)
	command.AddCommand(plan)

	return command
}

func (r *rootState) newSessionOrphanCleanupCommand() *cobra.Command {
	var (
		sourceNamespace string
		sourcePVC       string
		dryRun          bool
	)

	command := &cobra.Command{
		Use:   "cleanup-orphan SESSION",
		Short: "Inspect and safely clear ownership left after a session ConfigMap was lost",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			options := app.OrphanCleanupOptions{
				SessionID:        args[0],
				SessionNamespace: r.global.sessionNamespace,
				SourceNamespace:  sourceNamespace,
				SourcePVC:        sourcePVC,
			}
			prefix := sessionCommandPrefixForCommand(cmd, r.global.sessionNamespace)
			validateCommand := fmt.Sprintf(
				"%s session cleanup-orphan %s --source-namespace %s --source-pvc %s",
				prefix,
				args[0],
				sourceNamespace,
				sourcePVC,
			)
			executeCommand := fmt.Sprintf(
				"%s --yes session cleanup-orphan %s --source-namespace %s --source-pvc %s --dry-run=false",
				prefix,
				args[0],
				sourceNamespace,
				sourcePVC,
			)

			plan, err := runtime.service.PlanOrphanCleanup(ctx, options)
			if err != nil {
				_, _ = fmt.Fprintln(
					cmd.ErrOrStderr(),
					"\nOrphan ownership inspection failed. Retry validation:",
					validateCommand,
				)

				return err
			}

			if !plan.Ready {
				if printErr := runtime.printer.Print(plan); printErr != nil {
					return printErr
				}

				_, _ = fmt.Fprintln(
					cmd.ErrOrStderr(),
					"\nOrphan cleanup is blocked by failed checks. Resolve them, then retry:",
					validateCommand,
				)

				return domain.NewError(
					domain.ErrorPrecondition,
					"cleanup orphan",
					"orphan cleanup plan contains failed checks",
				)
			}

			if dryRun {
				if err := runtime.printer.Print(plan); err != nil {
					return err
				}

				_, err := fmt.Fprintln(
					cmd.ErrOrStderr(),
					"\nOrphan cleanup validation passed. Execute:",
					executeCommand,
				)

				return err
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			plan, err = runtime.service.CleanupOrphan(ctx, options)
			if err != nil {
				if plan != nil {
					_ = runtime.printer.Print(plan)
				}

				_, _ = fmt.Fprintln(
					cmd.ErrOrStderr(),
					"\nOrphan cleanup stopped before confirmed completion. Revalidate current resource state:",
					validateCommand,
				)

				return err
			}

			if err := runtime.printer.Print(plan); err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				cmd.ErrOrStderr(),
				"orphan ownership for session %s was cleared; the session ConfigMap was already absent\n",
				args[0],
			)

			return err
		},
	}
	command.Flags().
		StringVarP(&sourceNamespace, "source-namespace", "n", "default", "Namespace of the owned source PVC")
	command.Flags().StringVar(&sourcePVC, "source-pvc", "", "Name of the owned source PVC")

	if err := command.MarkFlagRequired("source-pvc"); err != nil {
		panic(err)
	}

	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newSessionActionPlanCommand(
	action string,
	validate func(context.Context, *commandRuntime, *domain.Session) error,
) *cobra.Command {
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
				return reportSessionLookupError(cmd, r.global.sessionNamespace, args[0], err)
			}

			if err := validate(ctx, runtime, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
}

func bindCleanupFlags(command *cobra.Command, options *app.CleanupOptions) {
	command.Flags().
		BoolVar(&options.DeleteTemporary, "delete-temporary", false, "Delete retained staged PVCs owned by the session")
	command.Flags().
		BoolVar(&options.DeleteRollback, "delete-rollback-pv", false, "Delete Released rollback PV objects; Retain preserves backend data")
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Restore the active PV's recorded reclaim policy")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the session ConfigMap after cleanup")
}
