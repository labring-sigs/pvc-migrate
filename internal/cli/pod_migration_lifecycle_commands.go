package cli

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) newPodMigrationStatusCommand(source workflowSource) *cobra.Command {
	return &cobra.Command{
		Use:   "status [" + workflowArgLabel(source) + "]",
		Short: "Show one Pod migration or list Pod migrations",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				object, _, err := r.loadPodMigration(ctx, cmd, runtime, args[0], source)
				if err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				return writePodMigrationNextSteps(cmd, r, object)
			}

			var list []crclient.Object

			if source == sourceController {
				namespaced, err := runtime.podMigrationStore.List(ctx, crNamespaceForCommand(cmd))
				if err != nil {
					return err
				}

				for _, object := range namespaced {
					list = append(list, object)
				}

				return runtime.printer.Print(list)
			}

			// Session ConfigMaps persist PodMigration objects whose
			// metadata.namespace is the tenant namespace; the configured
			// session namespace is only the ConfigMap storage location.
			sessions, err := runtime.podMigrationSessionStore.List(ctx, "")
			if err != nil {
				return err
			}

			for _, object := range sessions {
				list = append(list, object)
			}

			return runtime.printer.Print(list)
		},
	}
}

func (r *rootState) newPodMigrationResumeCommand(source workflowSource) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume " + workflowArgLabel(source),
		Short: "Continue a Pod migration from its persisted phase",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, dispatch, err := r.loadPodMigration(ctx, cmd, runtime, args[0], source)
			if err != nil {
				return err
			}

			if dryRun {
				if err := dispatch.validate(ctx); err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				return writePodMigrationDryRunNotice(cmd, r, object, "resume")
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := dispatch.requestResume(ctx); err != nil {
				return err
			}

			if err := dispatch.run(ctx); err != nil {
				return err
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writePodMigrationNextSteps(cmd, r, object)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationAbortCommand(source workflowSource) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort " + workflowArgLabel(source),
		Short: "Stop a Pod migration before cutover",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, dispatch, err := r.loadPodMigration(ctx, cmd, runtime, args[0], source)
			if err != nil {
				return err
			}

			if dryRun {
				if err := dispatch.validateAbort(ctx); err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				return writePodMigrationDryRunNotice(cmd, r, object, "abort")
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := dispatch.abort(ctx); err != nil {
				return err
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writePodMigrationNextSteps(cmd, r, object)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationRollbackCommand(source workflowSource) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "rollback " + workflowArgLabel(source),
		Short: "Restore source bindings after Pod migration cutover",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, dispatch, err := r.loadPodMigration(ctx, cmd, runtime, args[0], source)
			if err != nil {
				return err
			}

			if dryRun {
				if err := dispatch.validateRollback(ctx); err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				return writePodMigrationDryRunNotice(cmd, r, object, "rollback")
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := dispatch.rollback(ctx); err != nil {
				return err
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writePodMigrationNextSteps(cmd, r, object)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationCleanupCommand(source workflowSource) *cobra.Command {
	var (
		options app.MigrationCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup " + workflowArgLabel(source),
		Short: "Apply storage reclaim policies and close the Pod migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, dispatch, err := r.loadPodMigration(ctx, cmd, runtime, args[0], source)
			if err != nil {
				return err
			}

			if dryRun {
				if err := dispatch.validateCleanup(ctx, options); err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				namespace := podMigrationHintNamespace(cmd, r, object)

				return writeDryRunNotice(
					cmd.ErrOrStderr(),
					cleanupExecuteCommand(
						cmd,
						guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
						"migrate-pod",
						namespace,
						object.GetName(),
						options.UnusedStoragePolicy,
						options.Finalize,
						options.DeleteSession,
					),
				)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := dispatch.cleanup(ctx, options); err != nil {
				return err
			}

			// The record is gone; print the shared closing confirmation
			// instead of exiting silently.
			if options.DeleteSession {
				return writeDeletedWorkflow(cmd, "pod migration workflow", object.GetName())
			}

			return runtime.printer.Print(object)
		},
	}
	bindMigrationCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)

	return command
}

// writePodMigrationDryRunNotice prints the execute form of the lifecycle
// subcommand just previewed, resolved from the namespace the workflow record
// lives in: the session storage namespace for session records, the tenant
// namespace for CRs.
func writePodMigrationDryRunNotice(
	cmd *cobra.Command,
	r *rootState,
	object crclient.Object,
	subcommand string,
) error {
	namespace := podMigrationHintNamespace(cmd, r, object)

	return writeDryRunNotice(
		cmd.ErrOrStderr(),
		lifecycleExecuteCommand(
			cmd,
			guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
			"migrate-pod",
			subcommand,
			namespace,
			object.GetName(),
		),
	)
}

func bindMigrationCleanupFlags(command *cobra.Command, options *app.MigrationCleanupOptions) {
	command.Flags().
		StringVar(&options.UnusedStoragePolicy, "unused-storage-policy", "", "Keep or Delete replaced storage; defaults to the recorded policy. Delete removes the old source PV after a completed cutover, or the staged destination after a rollback or abort; the PVC the workload runs on is always kept")
	command.Flags().BoolVar(&options.Finalize, "finalize", false, "Finalize cleanup")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the workflow after cleanup")
}

// podMigrationDispatch binds the lifecycle operations of whichever executor
// owns the loaded workflow, so commands stay executor-agnostic across the
// session (ConfigMap), cluster CRD, and namespaced CRD backends.
type podMigrationDispatch struct {
	object           crclient.Object
	validate         func(context.Context) error
	validateAbort    func(context.Context) error
	validateRollback func(context.Context) error
	validateCleanup  func(context.Context, app.MigrationCleanupOptions) error
	requestResume    func(context.Context) error
	run              func(context.Context) error
	abort            func(context.Context) error
	rollback         func(context.Context) error
	cleanup          func(context.Context, app.MigrationCleanupOptions) error
}

// loadPodMigration resolves one Pod migration from the backend its command
// family addresses — session (ConfigMap) records for the session commands,
// namespaced PodMigration CRs for the cr commands — and returns the executor
// operations bound to that backend.
func (r *rootState) loadPodMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	name string,
	source workflowSource,
) (crclient.Object, *podMigrationDispatch, error) {
	if runtime == nil || runtime.podMigrationStore == nil ||
		runtime.podMigrationSessionStore == nil {
		return nil, nil, domain.NewError(
			domain.ErrorInternal,
			"pod migration",
			"workflow store is not configured",
		)
	}

	bindDispatch := func(
		object *v1alpha1.PodMigration,
		executor *app.PodMigrationExecutor,
	) *podMigrationDispatch {
		return &podMigrationDispatch{
			object:           object,
			validate:         func(ctx context.Context) error { return executor.Validate(ctx, object) },
			validateAbort:    func(ctx context.Context) error { return executor.ValidateAbort(ctx, object) },
			validateRollback: func(ctx context.Context) error { return executor.ValidateRollback(ctx, object) },
			validateCleanup: func(ctx context.Context, options app.MigrationCleanupOptions) error {
				return executor.ValidateCleanup(ctx, object, options)
			},
			requestResume: func(ctx context.Context) error { return executor.RequestResume(ctx, object) },
			run:           func(ctx context.Context) error { return executor.Run(ctx, object) },
			abort:         func(ctx context.Context) error { return executor.Abort(ctx, object) },
			rollback:      func(ctx context.Context) error { return executor.Rollback(ctx, object) },
			cleanup: func(ctx context.Context, options app.MigrationCleanupOptions) error {
				return executor.Cleanup(ctx, object, options)
			},
		}
	}

	key := crclient.ObjectKey{Name: name}

	if source == sourceController {
		namespace := crNamespaceForCommand(cmd)
		if namespace == "" {
			return nil, nil, domain.NewError(
				domain.ErrorValidation,
				"pod migration",
				"-n/--namespace is required to address a namespaced workflow CR",
			)
		}

		object, err := runtime.podMigrationStore.Load(
			ctx, crclient.ObjectKey{Name: name, Namespace: namespace},
		)

		return object, bindDispatch(object, runtime.podMigrationExecutor), err
	}

	object, err := runtime.podMigrationSessionStore.Load(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	return object, bindDispatch(object, runtime.podMigrationSessionExecutor), nil
}
