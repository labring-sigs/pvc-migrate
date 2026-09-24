package cli

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) addMoveLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newMoveStatusCommand(sourceSession),
		r.newMoveResumeCommand(sourceSession),
		r.newMoveAbortCommand(sourceSession),
		r.newMoveRollbackCommand(sourceSession),
		r.newMoveCleanupCommand(sourceSession),
	)
}

func moveStorageNamespace(object *v1alpha1.Move) string {
	if object.Status.Plan != nil {
		return string(object.Status.Plan.SessionNamespace)
	}

	if object.Spec.SessionNamespace != "" {
		return string(object.Spec.SessionNamespace)
	}

	return string(object.Spec.SourceNamespace)
}

func moveStore(
	runtime *commandRuntime,
	namespace string,
) (kube.WorkflowStore[*v1alpha1.Move], error) {
	return kube.NewConfigMapWorkflowStore(
		runtime.clients.Kubernetes,
		namespace,
		func() *v1alpha1.Move { return &v1alpha1.Move{} },
	)
}

// loadMove resolves one move from the backend its command family addresses:
// ConfigMap session records for the session commands, the cluster-scoped Move
// CR for the cr commands.
func (r *rootState) loadMove(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	source workflowSource,
) (*v1alpha1.Move, kube.WorkflowStore[*v1alpha1.Move], string, error) {
	namespace := r.global.sessionNamespace

	object, backend, err := r.loadWorkflowWithBackend(
		ctx,
		cmd,
		runtime,
		namespace,
		id,
		map[domain.ControllerKind]crclient.Object{
			domain.ControllerKindMove: &v1alpha1.Move{},
		},
		source,
	)
	if err != nil {
		return nil, nil, "", reportSessionLookupError(cmd, namespace, id, err)
	}

	move, ok := object.(*v1alpha1.Move)
	if !ok {
		return nil, nil, "", domain.NewError(
			domain.ErrorValidation,
			"move",
			"stored workflow is not a move",
		)
	}

	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		namespace,
		func() *v1alpha1.Move { return &v1alpha1.Move{} },
	)
	if err != nil {
		return nil, nil, "", err
	}

	return move, store, backend, nil
}

func (r *rootState) newMoveStatusCommand(source workflowSource) *cobra.Command {
	return &cobra.Command{
		Use:   "status [" + workflowArgLabel(source) + "]",
		Short: "Show one move workflow or list move workflows",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				object, _, _, err := r.loadMove(ctx, cmd, runtime, args[0], source)
				if err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				namespace := moveStorageNamespace(object)

				return writeWorkflowNextSteps(
					cmd.ErrOrStderr(),
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"move",
					namespace,
					object.Name,
					object.Status.Phase,
					true,
				)
			}

			if source != sourceSession {
				if !crdListable(runtime) ||
					(len(runtime.controllerKinds) != 0 &&
						!slices.Contains(runtime.controllerKinds, domain.ControllerKindMove)) {
					return runtime.printer.Print([]crclient.Object(nil))
				}

				items, err := listControllerWorkflows(ctx, runtime, domain.ControllerKindMove)
				if err != nil {
					return err
				}

				return runtime.printer.Print(items)
			}

			store, err := moveStore(runtime, r.global.sessionNamespace)
			if err != nil {
				return err
			}

			objects, err := store.List(ctx, "")
			if err != nil {
				return err
			}

			return runtime.printer.Print(objects)
		},
	}
}

type moveAction func(context.Context, *app.MoveExecutor, *v1alpha1.Move) error

func (r *rootState) moveLifecycleCommand(
	use, short string,
	source workflowSource,
	validate, execute moveAction,
) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   use + " " + workflowArgLabel(source),
		Short: short,
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, backend, err := r.loadMove(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		namespace := moveStorageNamespace(object)

		executor := app.NewMoveExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLockerForBackend(runtime, backend),
			namespace,
		)
		if dryRun {
			if err := validate(ctx, executor, object); err != nil {
				return reportMoveError(cmd, object, err)
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"move",
					use,
					namespace,
					object.Name,
				),
			)
		}

		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		if err := execute(ctx, executor, object); err != nil {
			return reportMoveError(cmd, object, err)
		}

		if err := runtime.printer.Print(object); err != nil {
			return err
		}

		return writeWorkflowNextSteps(
			cmd.ErrOrStderr(),
			cmd,
			guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
			"move",
			namespace,
			object.Name,
			object.Status.Phase,
			true,
		)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newMoveAbortCommand(source workflowSource) *cobra.Command {
	return r.moveLifecycleCommand("abort", "Abort a move workflow", source,
		func(_ context.Context, executor *app.MoveExecutor, object *v1alpha1.Move) error {
			return executor.ValidateAbort(object)
		},
		func(ctx context.Context, executor *app.MoveExecutor, object *v1alpha1.Move) error {
			return executor.Abort(ctx, object)
		})
}

func (r *rootState) newMoveRollbackCommand(source workflowSource) *cobra.Command {
	return r.moveLifecycleCommand("rollback", "Restore the original PVC namespace and name", source,
		func(ctx context.Context, executor *app.MoveExecutor, object *v1alpha1.Move) error {
			return executor.ValidateRollback(ctx, object)
		},
		func(ctx context.Context, executor *app.MoveExecutor, object *v1alpha1.Move) error {
			return executor.Rollback(ctx, object)
		})
}

func (r *rootState) newMoveResumeCommand(source workflowSource) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume " + workflowArgLabel(source),
		Short: "Continue a move from its persisted checkpoint",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, backend, err := r.loadMove(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		namespace := moveStorageNamespace(object)

		executor := app.NewMoveExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLockerForBackend(runtime, backend),
			namespace,
		)
		if dryRun {
			if err := executor.ValidateResume(ctx, object); err != nil {
				return reportMoveError(cmd, object, err)
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"move",
					"resume",
					namespace,
					object.Name,
				),
			)
		}

		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		if err := executor.RequestResume(ctx, object); err != nil {
			return reportMoveError(cmd, object, err)
		}

		if err := executor.Run(ctx, object); err != nil {
			return reportMoveError(cmd, object, err)
		}

		if err := runtime.printer.Print(object); err != nil {
			return err
		}

		return writeWorkflowNextSteps(
			cmd.ErrOrStderr(),
			cmd,
			guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
			"move",
			namespace,
			object.Name,
			object.Status.Phase,
			true,
		)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newMoveCleanupCommand(source workflowSource) *cobra.Command {
	var (
		options app.IdentityCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup " + workflowArgLabel(source),
		Short: "Finalize retained move resources and clean up the workflow",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, backend, err := r.loadMove(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		namespace := moveStorageNamespace(object)

		executor := app.NewMoveExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLockerForBackend(runtime, backend),
			namespace,
		)
		if dryRun {
			if err := executor.ValidateCleanup(ctx, object, options); err != nil {
				return reportMoveError(cmd, object, err)
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				cleanupExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"move",
					namespace,
					object.Name,
					"",
					options.Finalize,
					options.DeleteSession,
				),
			)
		}

		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		if err := executor.Cleanup(ctx, object, options); err != nil {
			return reportMoveError(cmd, object, err)
		}

		if options.DeleteSession {
			return writeDeletedWorkflow(cmd, "move workflow", object.Name)
		}

		return runtime.printer.Print(object)
	}
	bindIdentityCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)

	return command
}
