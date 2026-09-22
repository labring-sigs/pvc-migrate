package cli

import (
	"errors"
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) addCopyLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newCopyStatusCommand(),
		r.newCopyResumeCommand(),
		r.newCopyAbortCommand(),
		r.newCopyCleanupCommand(),
	)
}

func (r *rootState) newCopyStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use: "status [SESSION]", Short: "Show one copy or list copies", Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				object, err := r.loadCopy(ctx, cmd, runtime, args[0])
				if err != nil {
					return err
				}

				return runtime.printer.Print(object)
			}

			namespace := r.workflowStorageNamespace(cmd)

			objects := []crclient.Object{}
			if len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindCopy) {
				store, err := cliWorkflowStore(
					runtime,
					namespace,
					func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
				)
				if err != nil {
					return err
				}

				items, err := store.List(ctx, namespace)
				if err != nil {
					return err
				}

				for _, object := range items {
					objects = append(objects, object)
				}
			}

			if len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindClusterCopy) {
				store, err := cliWorkflowStore(
					runtime,
					namespace,
					func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
				)
				if err != nil {
					return err
				}

				items, err := store.List(ctx, "")
				if err != nil {
					return err
				}

				for _, object := range items {
					objects = append(objects, object)
				}
			}

			if crdListable(runtime) && (len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindCopy)) {
				crdStore, err := cliCRDWorkflowStore(
					runtime,
					func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
				)
				if err != nil {
					return err
				}

				items, err := crdStore.List(ctx, namespace)
				if err != nil {
					return err
				}

				for _, object := range items {
					objects = append(objects, object)
				}
			}

			if crdListable(runtime) && (len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindClusterCopy)) {
				crdStore, err := cliCRDWorkflowStore(
					runtime,
					func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
				)
				if err != nil {
					return err
				}

				items, err := crdStore.List(ctx, "")
				if err != nil {
					return err
				}

				for _, object := range items {
					objects = append(objects, object)
				}
			}

			return runtime.printer.Print(objects)
		},
	}
}

func (r *rootState) newCopyResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue a copy from its persisted checkpoint",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		return r.resumeCopy(ctx, cmd, runtime, args[0], dryRun)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newCopyAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{Use: "abort SESSION", Short: "Abort a copy", Args: cobra.ExactArgs(1)}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, backend, err := r.loadCopyWithBackend(ctx, cmd, runtime, args[0], false)
		if err != nil {
			return err
		}

		if !dryRun {
			if err := r.confirm(ctx, cmd, object.GetName()); err != nil {
				return reportApprovalError(cmd, err)
			}
		}

		switch current := object.(type) {
		case *v1alpha1.Copy:
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.workflowStorageNamespace(cmd),
				func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
			)
			if err != nil {
				return err
			}

			executor := app.NewCopyExecutor(
				runtime.clients.Kubernetes,
				store,
				cliWorkflowLockerForBackend(runtime, backend),
				copyengine.NewPVMigrate(),
				r.copyConfig(runtime),
			)
			if dryRun {
				err = executor.ValidateAbort(current)
			} else {
				err = executor.Abort(ctx, current)
			}

			if err != nil {
				return reportCopyError(cmd, current.Name, current.Status.Phase, err)
			}
		case *v1alpha1.ClusterCopy:
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.workflowStorageNamespace(cmd),
				func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
			)
			if err != nil {
				return err
			}

			namespace := string(current.Spec.SessionNamespace)
			if namespace == "" {
				namespace = string(current.Spec.SourceNamespace)
			}

			executor := app.NewClusterCopyExecutor(
				runtime.clients.Kubernetes,
				store,
				cliWorkflowLockerForBackend(runtime, backend),
				namespace,
				copyengine.NewPVMigrate(),
				r.copyConfig(runtime),
			)
			if dryRun {
				err = executor.ValidateAbort(current)
			} else {
				err = executor.Abort(ctx, current)
			}

			if err != nil {
				return reportCopyError(cmd, current.Name, current.Status.Phase, err)
			}
		}

		return runtime.printer.Print(object)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newCopyCleanupCommand() *cobra.Command {
	var (
		options app.CopyCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Finalize copy storage and clean up its workflow",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, backend, err := r.loadCopyWithBackend(ctx, cmd, runtime, args[0], false)
		if err != nil {
			return err
		}

		if !dryRun {
			if err := r.confirm(ctx, cmd, object.GetName()); err != nil {
				return reportApprovalError(cmd, err)
			}
		}

		switch current := object.(type) {
		case *v1alpha1.Copy:
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.workflowStorageNamespace(cmd),
				func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
			)
			if err != nil {
				return err
			}

			executor := app.NewCopyExecutor(
				runtime.clients.Kubernetes,
				store,
				cliWorkflowLockerForBackend(runtime, backend),
				copyengine.NewPVMigrate(),
				r.copyConfig(runtime),
			)
			if dryRun {
				err = executor.ValidateCleanup(ctx, current, options)
			} else {
				err = executor.Cleanup(ctx, current, options)
			}

			if err != nil {
				return reportCopyCleanupError(
					cmd,
					workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), current),
					current.Name,
					options,
					err,
				)
			}
		case *v1alpha1.ClusterCopy:
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.workflowStorageNamespace(cmd),
				func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
			)
			if err != nil {
				return err
			}

			namespace := string(current.Spec.SessionNamespace)
			if namespace == "" {
				namespace = string(current.Spec.SourceNamespace)
			}

			executor := app.NewClusterCopyExecutor(
				runtime.clients.Kubernetes,
				store,
				cliWorkflowLockerForBackend(runtime, backend),
				namespace,
				copyengine.NewPVMigrate(),
				r.copyConfig(runtime),
			)
			if dryRun {
				err = executor.ValidateCleanup(ctx, current, options)
			} else {
				err = executor.Cleanup(ctx, current, options)
			}

			if err != nil {
				return reportCopyCleanupError(
					cmd,
					namespace,
					current.Name,
					options,
					err,
				)
			}
		}

		if options.DeleteSession && !dryRun {
			_, err := fmt.Fprintf(
				cmd.OutOrStdout(),
				"Deleted copy workflow %s.\n",
				object.GetName(),
			)

			return err
		}

		return runtime.printer.Print(object)
	}
	command.Flags().
		StringVar(&options.UnusedStoragePolicy, "unused-storage-policy", "", "Keep or Delete an undelivered destination; defaults to the recorded policy. Delete removes the destination PVC only when the copy aborted before completing; a completed copy's destination and the source are always kept")
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Release ownership of retained storage and close the recovery window")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the workflow record after cleanup")
	bindDryRun(command, &dryRun)

	return command
}

func reportCopyError(
	cmd *cobra.Command,
	name string,
	phase v1alpha1.WorkflowPhase,
	cause error,
) error {
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Copy %s stopped in phase %s. Inspect copy status %s before resume or cleanup.\n",
		name,
		phase,
		name,
	)

	return errors.Join(cause, err)
}

func reportCopyCleanupError(
	cmd *cobra.Command,
	namespace, name string,
	options app.CopyCleanupOptions,
	cause error,
) error {
	if blocker, ok := errors.AsType[*app.CleanupPodBlockerError](cause); ok {
		if err := writeCleanupPodBlockerGuidance(cmd.ErrOrStderr(), cmd, blocker); err != nil {
			cause = errors.Join(cause, err)
		}
	}

	prefix := guidancePrefixesForCommand(cmd, namespace).pvcMigrate

	retry := "copy cleanup " + shellQuote(name)
	if options.UnusedStoragePolicy != "" {
		retry += " --unused-storage-policy " + shellQuote(
			options.UnusedStoragePolicy,
		)
	}

	if options.Finalize {
		retry += " --finalize"
	}

	if options.DeleteSession {
		retry += " --delete-session"
	}

	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Cleanup stopped before confirmed completion. Inspect current state: %s copy status %s\nRevalidate cleanup before retrying: %s %s --dry-run\n",
		prefix,
		shellQuote(name),
		prefix,
		retry,
	)

	return errors.Join(cause, err)
}
