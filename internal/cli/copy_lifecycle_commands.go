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

// addCopyLifecycle mounts the namespaced session family verbs: they address
// only Copy (and Reservation graduation) records. The records live in the
// session storage namespace, so the verbs carry no namespace flag — the -n
// the run command binds addresses the tenant namespace of the workflow, never
// the ConfigMap storage location.
func (r *rootState) addCopyLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newScopedCopyStatusCommand(sourceSession, namespacedRecords),
		r.newScopedCopyResumeCommand(sourceSession, namespacedRecords),
		r.newScopedCopyAbortCommand(sourceSession, namespacedRecords),
		r.newScopedCopyCleanupCommand(sourceSession, namespacedRecords),
	)
}

// addClusterCopyLifecycle mounts the cluster-scoped session family verbs: they
// address only ClusterCopy (and ClusterReservation graduation) records.
func (r *rootState) addClusterCopyLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newScopedCopyStatusCommand(sourceSession, clusterRecords),
		r.newScopedCopyResumeCommand(sourceSession, clusterRecords),
		r.newScopedCopyAbortCommand(sourceSession, clusterRecords),
		r.newScopedCopyCleanupCommand(sourceSession, clusterRecords),
	)
}

func (r *rootState) newCopyStatusCommand(source workflowSource) *cobra.Command {
	return r.newScopedCopyStatusCommand(source, recordScopeForSource(source))
}

func (r *rootState) newCopyResumeCommand(source workflowSource) *cobra.Command {
	return r.newScopedCopyResumeCommand(source, recordScopeForSource(source))
}

func (r *rootState) newCopyAbortCommand(source workflowSource) *cobra.Command {
	return r.newScopedCopyAbortCommand(source, recordScopeForSource(source))
}

func (r *rootState) newCopyCleanupCommand(source workflowSource) *cobra.Command {
	return r.newScopedCopyCleanupCommand(source, recordScopeForSource(source))
}

func (r *rootState) newScopedCopyStatusCommand(
	source workflowSource,
	scope recordScope,
) *cobra.Command {
	command := &cobra.Command{
		Use:   "status [" + workflowArgLabel(source) + "]",
		Short: "Show one copy or list copies",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				object, err := r.loadCopy(ctx, cmd, runtime, args[0], source, scope)
				if err != nil {
					return err
				}

				return runtime.printer.Print(object)
			}

			objects := []crclient.Object{}

			if source != sourceSession {
				if !crdListable(runtime) {
					return runtime.printer.Print(objects)
				}

				kind := domain.ControllerKindCopy
				if source == sourceClusterController {
					kind = domain.ControllerKindClusterCopy
				}

				if len(runtime.controllerKinds) != 0 &&
					!slices.Contains(runtime.controllerKinds, kind) {
					return runtime.printer.Print(objects)
				}

				items, err := listControllerWorkflows(ctx, runtime, kind)
				if err != nil {
					return err
				}

				objects = append(objects, items...)

				return runtime.printer.Print(objects)
			}

			// One session family lists only its own kind: copy lists the
			// namespaced Copy records, cluster-copy the ClusterCopy records.
			// The ConfigMap namespace is the storage location; the objects
			// inside carry the tenant namespace, so the listing is not
			// filtered by it.
			namespace := r.workflowStorageNamespace(cmd)

			if scope == namespacedRecords {
				copyStore, err := cliWorkflowStore(
					runtime,
					namespace,
					func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
				)
				if err != nil {
					return err
				}

				items, err := copyStore.List(ctx, "")
				if err != nil {
					return err
				}

				for _, object := range items {
					objects = append(objects, object)
				}

				return runtime.printer.Print(objects)
			}

			clusterStore, err := cliWorkflowStore(
				runtime,
				namespace,
				func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
			)
			if err != nil {
				return err
			}

			clusterItems, err := clusterStore.List(ctx, "")
			if err != nil {
				return err
			}

			for _, object := range clusterItems {
				objects = append(objects, object)
			}

			return runtime.printer.Print(objects)
		},
	}

	return command
}

func (r *rootState) newScopedCopyResumeCommand(
	source workflowSource,
	scope recordScope,
) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume " + workflowArgLabel(source),
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

		return r.resumeCopy(ctx, cmd, runtime, args[0], dryRun, source, scope)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newScopedCopyAbortCommand(
	source workflowSource,
	scope recordScope,
) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort " + workflowArgLabel(source),
		Short: "Abort a copy",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, backend, err := r.loadCopyWithBackend(
			ctx,
			cmd,
			runtime,
			args[0],
			false,
			source,
			scope,
		)
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
			// Session records persist in the session storage namespace
			// whatever tenant namespace their object carries; a CRD record
			// ignores the store namespace entirely, so one resolution serves
			// both backends.
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.migrationRecordNamespace(),
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
				return reportCopyError(cmd, "copy", current.Name, current.Status.Phase, err)
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
				return reportCopyError(cmd, "cluster-copy", current.Name, current.Status.Phase, err)
			}
		}

		return runtime.printer.Print(object)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newScopedCopyCleanupCommand(
	source workflowSource,
	scope recordScope,
) *cobra.Command {
	var (
		options app.CopyCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup " + workflowArgLabel(source),
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

		object, backend, err := r.loadCopyWithBackend(
			ctx,
			cmd,
			runtime,
			args[0],
			false,
			source,
			scope,
		)
		if err != nil {
			return err
		}

		if !dryRun {
			if err := r.confirm(ctx, cmd, object.GetName()); err != nil {
				return reportApprovalError(cmd, err)
			}
		}

		family := sessionFamilyCommand(scope, "copy")

		switch current := object.(type) {
		case *v1alpha1.Copy:
			// Session records persist in the session storage namespace
			// whatever tenant namespace their object carries; a CRD record
			// ignores the store namespace entirely, so one resolution serves
			// both backends.
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.migrationRecordNamespace(),
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
					family,
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
					family,
					namespace,
					current.Name,
					options,
					err,
				)
			}
		}

		if options.DeleteSession && !dryRun {
			return writeDeletedWorkflow(cmd, "copy workflow", object.GetName())
		}

		if err := runtime.printer.Print(object); err != nil {
			return err
		}

		if dryRun {
			namespace := workflowHintNamespace(backend, r, cmd, object)

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				cleanupExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					family,
					namespace,
					object.GetName(),
					options.UnusedStoragePolicy,
					options.Finalize,
					options.DeleteSession,
				),
			)
		}

		return nil
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
	family, name string,
	phase v1alpha1.WorkflowPhase,
	cause error,
) error {
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Copy %s stopped in phase %s. Inspect %s status %s before resume or cleanup.\n",
		name,
		phase,
		workflowCommandPath(cmd, family),
		name,
	)

	return errors.Join(cause, err)
}

func reportCopyCleanupError(
	cmd *cobra.Command,
	family, namespace, name string,
	options app.CopyCleanupOptions,
	cause error,
) error {
	if blocker, ok := errors.AsType[*app.CleanupPodBlockerError](cause); ok {
		if err := writeCleanupPodBlockerGuidance(cmd.ErrOrStderr(), cmd, blocker); err != nil {
			cause = errors.Join(cause, err)
		}
	}

	prefix := guidancePrefixesForCommand(cmd, namespace).pvcMigrate
	path := workflowCommandPath(cmd, family)

	retry := path + " cleanup " + shellQuote(name)
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
		"Cleanup stopped before confirmed completion. Inspect current state: %s %s status %s\nRevalidate cleanup before retrying: %s %s --dry-run\n",
		prefix,
		path,
		shellQuote(name),
		prefix,
		retry,
	)

	return errors.Join(cause, err)
}
