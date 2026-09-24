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

func (r *rootState) addRenameLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newRenameStatusCommand(sourceSession),
		r.newRenameResumeCommand(sourceSession),
		r.newRenameAbortCommand(sourceSession),
		r.newRenameRollbackCommand(sourceSession),
		r.newRenameCleanupCommand(sourceSession),
	)
}

func (r *rootState) renameStorageNamespace(cmd *cobra.Command) string {
	return workflowNamespaceForCommand(r, cmd)
}

// loadRename resolves one rename from the backend its command family
// addresses: ConfigMap session records for the session commands, namespaced
// Rename CRs for the cr commands.
func (r *rootState) loadRename(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	source workflowSource,
) (*v1alpha1.Rename, kube.WorkflowStore[*v1alpha1.Rename], string, error) {
	storageNamespace := r.renameStorageNamespace(cmd)

	object, backend, err := r.loadWorkflowWithBackend(
		ctx,
		cmd,
		runtime,
		storageNamespace,
		id,
		map[domain.ControllerKind]crclient.Object{
			domain.ControllerKindRename: &v1alpha1.Rename{},
		},
		source,
	)
	if err != nil {
		return nil, nil, "", reportSessionLookupError(cmd, storageNamespace, id, err)
	}

	rename, ok := object.(*v1alpha1.Rename)
	if !ok {
		return nil, nil, "", domain.NewError(
			domain.ErrorValidation,
			"rename",
			"stored workflow is not a rename",
		)
	}

	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		storageNamespace,
		func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
	)
	if err != nil {
		return nil, nil, "", err
	}

	return rename, store, backend, nil
}

func (r *rootState) newRenameStatusCommand(source workflowSource) *cobra.Command {
	return &cobra.Command{
		Use:   "status [" + workflowArgLabel(source) + "]",
		Short: "Show one rename workflow or list rename workflows",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				object, _, _, err := r.loadRename(ctx, cmd, runtime, args[0], source)
				if err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				return writeWorkflowNextSteps(
					cmd.ErrOrStderr(),
					cmd,
					guidancePrefixesForCommand(cmd, object.Namespace).pvcMigrate,
					"rename",
					object.Namespace,
					object.Name,
					object.Status.Phase,
					true,
				)
			}

			if source != sourceSession {
				if !crdListable(runtime) ||
					(len(runtime.controllerKinds) != 0 &&
						!slices.Contains(runtime.controllerKinds, domain.ControllerKindRename)) {
					return runtime.printer.Print([]crclient.Object(nil))
				}

				items, err := listControllerWorkflows(ctx, runtime, domain.ControllerKindRename)
				if err != nil {
					return err
				}

				return runtime.printer.Print(items)
			}

			store, err := renameStore(runtime, r.renameStorageNamespace(cmd))
			if err != nil {
				return err
			}

			namespace := workflowNamespaceForCommand(r, cmd)

			objects, err := store.List(ctx, namespace)
			if err != nil {
				return err
			}

			return runtime.printer.Print(objects)
		},
	}
}

type renameAction func(context.Context, *app.RenameExecutor, *v1alpha1.Rename) error

func (r *rootState) renameLifecycleCommand(
	use, short string,
	source workflowSource,
	validate, execute renameAction,
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

		object, store, backend, err := r.loadRename(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		namespace := workflowLeaseNamespace(backend, r.renameStorageNamespace(cmd), object)

		executor := app.NewRenameExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLockerForBackend(runtime, backend),
			namespace,
		)
		if dryRun {
			if err := validate(ctx, executor, object); err != nil {
				return reportRenameError(cmd, object, err)
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"rename",
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
			return reportRenameError(cmd, object, err)
		}

		if err := runtime.printer.Print(object); err != nil {
			return err
		}

		return writeWorkflowNextSteps(
			cmd.ErrOrStderr(),
			cmd,
			guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
			"rename",
			namespace,
			object.Name,
			object.Status.Phase,
			true,
		)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRenameResumeCommand(source workflowSource) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume " + workflowArgLabel(source),
		Short: "Continue a rename from its persisted checkpoint",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, backend, err := r.loadRename(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		namespace := workflowLeaseNamespace(backend, r.renameStorageNamespace(cmd), object)

		executor := app.NewRenameExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLockerForBackend(runtime, backend),
			namespace,
		)
		if dryRun {
			if err := executor.ValidateResume(ctx, object); err != nil {
				return reportRenameError(cmd, object, err)
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"rename",
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
			return reportRenameError(cmd, object, err)
		}

		if err := executor.Run(ctx, object); err != nil {
			return reportRenameError(cmd, object, err)
		}

		return runtime.printer.Print(object)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRenameAbortCommand(source workflowSource) *cobra.Command {
	return r.renameLifecycleCommand(
		"abort",
		"Abort a rename workflow",
		source,
		func(_ context.Context, executor *app.RenameExecutor, object *v1alpha1.Rename) error {
			return executor.ValidateAbort(object)
		},
		func(ctx context.Context, executor *app.RenameExecutor, object *v1alpha1.Rename) error {
			return executor.Abort(ctx, object)
		},
	)
}

func (r *rootState) newRenameRollbackCommand(source workflowSource) *cobra.Command {
	return r.renameLifecycleCommand(
		"rollback",
		"Restore the original PVC name",
		source,
		func(ctx context.Context, executor *app.RenameExecutor, object *v1alpha1.Rename) error {
			return executor.ValidateRollback(ctx, object)
		},
		func(ctx context.Context, executor *app.RenameExecutor, object *v1alpha1.Rename) error {
			return executor.Rollback(ctx, object)
		},
	)
}

func (r *rootState) newRenameCleanupCommand(source workflowSource) *cobra.Command {
	var (
		options app.IdentityCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup " + workflowArgLabel(source),
		Short: "Finalize retained rename resources and clean up the workflow",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, backend, err := r.loadRename(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		namespace := workflowLeaseNamespace(backend, r.renameStorageNamespace(cmd), object)

		executor := app.NewRenameExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLockerForBackend(runtime, backend),
			namespace,
		)
		if dryRun {
			if err := executor.ValidateCleanup(ctx, object, options); err != nil {
				return reportRenameError(cmd, object, err)
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				cleanupExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"rename",
					namespace,
					object.Name,
					"",
					options.Finalize,
					options.DeleteSession,
				),
			)
		}

		if options.Finalize || options.DeleteSession {
			if err := r.confirm(ctx, cmd, object.Name); err != nil {
				return reportApprovalError(cmd, err)
			}
		}

		if err := executor.Cleanup(ctx, object, options); err != nil {
			return reportRenameError(cmd, object, err)
		}

		if options.DeleteSession {
			return writeDeletedWorkflow(cmd, "rename workflow", object.Name)
		}

		return runtime.printer.Print(object)
	}
	bindIdentityCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)

	return command
}
