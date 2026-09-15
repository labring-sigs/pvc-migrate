package cli

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) addRenameLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newRenameStatusCommand(),
		r.newRenameResumeCommand(),
		r.newRenameAbortCommand(),
		r.newRenameRollbackCommand(),
		r.newRenameCleanupCommand(),
	)
}

func (r *rootState) renameStorageNamespace(cmd *cobra.Command) string {
	return workflowNamespaceForCommand(r, cmd)
}

func (r *rootState) loadRename(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
) (*v1alpha1.Rename, kube.WorkflowStore[*v1alpha1.Rename], error) {
	storageNamespace := r.renameStorageNamespace(cmd)

	store, err := renameStore(runtime, storageNamespace)
	if err != nil {
		return nil, nil, err
	}

	key := crclient.ObjectKey{Name: id, Namespace: storageNamespace}

	object, err := store.Load(ctx, key)
	if err != nil {
		return nil, nil, reportSessionLookupError(cmd, storageNamespace, id, err)
	}

	return object, store, nil
}

func (r *rootState) newRenameStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
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
				object, _, err := r.loadRename(ctx, cmd, runtime, args[0])
				if err != nil {
					return err
				}

				return runtime.printer.Print(object)
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
	validate, execute renameAction,
) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{Use: use + " SESSION", Short: short, Args: cobra.ExactArgs(1)}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, err := r.loadRename(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		executor := app.NewRenameExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLocker(runtime),
			r.renameStorageNamespace(cmd),
		)
		if dryRun {
			if err := validate(ctx, executor, object); err != nil {
				return reportRenameError(cmd, object, err)
			}
			return runtime.printer.Print(object)
		}

		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		if err := execute(ctx, executor, object); err != nil {
			return reportRenameError(cmd, object, err)
		}

		return runtime.printer.Print(object)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRenameResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
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

		object, store, err := r.loadRename(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		executor := app.NewRenameExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLocker(runtime),
			r.renameStorageNamespace(cmd),
		)
		if dryRun {
			if err := executor.ValidateResume(ctx, object); err != nil {
				return reportRenameError(cmd, object, err)
			}
			return runtime.printer.Print(object)
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

func (r *rootState) newRenameAbortCommand() *cobra.Command {
	return r.renameLifecycleCommand(
		"abort",
		"Abort a rename workflow",
		func(_ context.Context, executor *app.RenameExecutor, object *v1alpha1.Rename) error {
			return executor.ValidateAbort(object)
		},
		func(ctx context.Context, executor *app.RenameExecutor, object *v1alpha1.Rename) error {
			return executor.Abort(ctx, object)
		},
	)
}

func (r *rootState) newRenameRollbackCommand() *cobra.Command {
	return r.renameLifecycleCommand(
		"rollback",
		"Restore the original PVC name",
		func(ctx context.Context, executor *app.RenameExecutor, object *v1alpha1.Rename) error {
			return executor.ValidateRollback(ctx, object)
		},
		func(ctx context.Context, executor *app.RenameExecutor, object *v1alpha1.Rename) error {
			return executor.Rollback(ctx, object)
		},
	)
}

func (r *rootState) newRenameCleanupCommand() *cobra.Command {
	var (
		options app.IdentityCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup SESSION",
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

		object, store, err := r.loadRename(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		executor := app.NewRenameExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLocker(runtime),
			r.renameStorageNamespace(cmd),
		)
		if dryRun {
			if err := executor.ValidateCleanup(ctx, object, options); err != nil {
				return reportRenameError(cmd, object, err)
			}
			return runtime.printer.Print(object)
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
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Deleted rename workflow %s.\n", object.Name)
			return err
		}

		return runtime.printer.Print(object)
	}
	bindIdentityCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)

	return command
}
