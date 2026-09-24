package cli

import (
	"errors"
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// addOfflineMigrationLifecycle mounts the namespaced session family verbs:
// they address only Migration records.
func (r *rootState) addOfflineMigrationLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newScopedOfflineMigrationStatusCommand(sourceSession, namespacedRecords),
		r.newScopedOfflineMigrationResumeCommand(sourceSession, namespacedRecords),
		r.newScopedOfflineMigrationAbortCommand(sourceSession, namespacedRecords),
		r.newScopedOfflineMigrationRollbackCommand(sourceSession, namespacedRecords),
		r.newScopedOfflineMigrationCleanupCommand(sourceSession, namespacedRecords),
	)
}

// addClusterOfflineMigrationLifecycle mounts the cluster-scoped session family
// verbs: they address only ClusterMigration records.
func (r *rootState) addClusterOfflineMigrationLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newScopedOfflineMigrationStatusCommand(sourceSession, clusterRecords),
		r.newScopedOfflineMigrationResumeCommand(sourceSession, clusterRecords),
		r.newScopedOfflineMigrationAbortCommand(sourceSession, clusterRecords),
		r.newScopedOfflineMigrationRollbackCommand(sourceSession, clusterRecords),
		r.newScopedOfflineMigrationCleanupCommand(sourceSession, clusterRecords),
	)
}

func (r *rootState) newOfflineMigrationStatusCommand(source workflowSource) *cobra.Command {
	return r.newScopedOfflineMigrationStatusCommand(source, recordScopeForSource(source))
}

func (r *rootState) newOfflineMigrationResumeCommand(source workflowSource) *cobra.Command {
	return r.newScopedOfflineMigrationResumeCommand(source, recordScopeForSource(source))
}

func (r *rootState) newOfflineMigrationAbortCommand(source workflowSource) *cobra.Command {
	return r.newScopedOfflineMigrationAbortCommand(source, recordScopeForSource(source))
}

func (r *rootState) newOfflineMigrationRollbackCommand(source workflowSource) *cobra.Command {
	return r.newScopedOfflineMigrationRollbackCommand(source, recordScopeForSource(source))
}

func (r *rootState) newOfflineMigrationCleanupCommand(source workflowSource) *cobra.Command {
	return r.newScopedOfflineMigrationCleanupCommand(source, recordScopeForSource(source))
}

// newScopedOfflineMigrationStatusCommand shows or lists the migrations of one
// family scope: migrate lists Migration sessions, cluster-migrate lists
// ClusterMigration sessions, and the cr families list their CRDs.
func (r *rootState) newScopedOfflineMigrationStatusCommand(
	source workflowSource,
	scope recordScope,
) *cobra.Command {
	return &cobra.Command{
		Use:   "status [" + workflowArgLabel(source) + "]",
		Short: "Show one offline migration or list migrations",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				object, backend, err := r.loadMigrationWithBackend(
					ctx,
					cmd,
					runtime,
					args[0],
					source,
					scope,
				)
				if err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				return writeOfflineMigrationNextSteps(
					cmd,
					r,
					backend,
					sessionFamilyCommand(scope, "migrate"),
					object,
				)
			}

			objects := []crclient.Object{}

			if source != sourceSession {
				if !crdListable(runtime) {
					return runtime.printer.Print(objects)
				}

				kind := domain.ControllerKindMigration
				if scope == clusterRecords {
					kind = domain.ControllerKindClusterMigration
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

			// One session family lists only its own kind: migrate lists the
			// namespaced Migration records, cluster-migrate the
			// ClusterMigration records. The ConfigMap namespace is the storage
			// location; the objects inside carry the tenant namespace, so the
			// listing is not filtered by it.
			namespace := r.workflowStorageNamespace(cmd)

			if scope == clusterRecords {
				clusterStore, err := cliWorkflowStore(
					runtime,
					namespace,
					func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} },
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
			}

			migrationStore, err := cliWorkflowStore(
				runtime,
				namespace,
				func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
			)
			if err != nil {
				return err
			}

			items, err := migrationStore.List(ctx, "")
			if err != nil {
				return err
			}

			for _, object := range items {
				objects = append(objects, object)
			}

			return runtime.printer.Print(objects)
		},
	}
}

func (r *rootState) newScopedOfflineMigrationResumeCommand(
	source workflowSource,
	scope recordScope,
) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume " + workflowArgLabel(source),
		Short: "Continue an offline migration from its checkpoint",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			return r.resumeMigration(ctx, cmd, runtime, args[0], dryRun, source, scope)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newScopedOfflineMigrationAbortCommand(
	source workflowSource,
	scope recordScope,
) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use: "abort " + workflowArgLabel(
			source,
		),
		Short: "Abort an offline migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, backend, err := r.loadMigrationWithBackend(
				ctx,
				cmd,
				runtime,
				args[0],
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

			family := sessionFamilyCommand(scope, "migrate")

			switch current := object.(type) {
			case *v1alpha1.Migration:
				executor, err := r.migrationExecutor(runtime, backend)
				if err != nil {
					return err
				}

				if dryRun {
					err = executor.ValidateAbort(ctx, current)
				} else {
					err = executor.Abort(ctx, current)
				}

				if err != nil {
					return reportMigrationError(
						cmd,
						family,
						current.Name,
						current.Status.Phase,
						err,
					)
				}

			case *v1alpha1.ClusterMigration:
				executor, err := r.clusterMigrationExecutor(runtime, cmd, current, backend)
				if err != nil {
					return err
				}

				if dryRun {
					err = executor.ValidateAbort(ctx, current)
				} else {
					err = executor.Abort(ctx, current)
				}

				if err != nil {
					return reportMigrationError(
						cmd,
						family,
						current.Name,
						current.Status.Phase,
						err,
					)
				}
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			if dryRun {
				return writeOfflineMigrationDryRunNotice(cmd, r, backend, family, object, "abort")
			}

			return writeOfflineMigrationNextSteps(cmd, r, backend, family, object)
		},
	}

	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newScopedOfflineMigrationRollbackCommand(
	source workflowSource,
	scope recordScope,
) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use: "rollback " + workflowArgLabel(
			source,
		),
		Short: "Rollback an offline migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, backend, err := r.loadMigrationWithBackend(
				ctx,
				cmd,
				runtime,
				args[0],
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

			family := sessionFamilyCommand(scope, "migrate")

			switch current := object.(type) {
			case *v1alpha1.Migration:
				executor, err := r.migrationExecutor(runtime, backend)
				if err != nil {
					return err
				}

				if dryRun {
					err = executor.ValidateRollback(ctx, current)
				} else {
					err = executor.Rollback(ctx, current)
				}

				if err != nil {
					return reportMigrationError(
						cmd,
						family,
						current.Name,
						current.Status.Phase,
						err,
					)
				}

			case *v1alpha1.ClusterMigration:
				executor, err := r.clusterMigrationExecutor(runtime, cmd, current, backend)
				if err != nil {
					return err
				}

				if dryRun {
					err = executor.ValidateRollback(ctx, current)
				} else {
					err = executor.Rollback(ctx, current)
				}

				if err != nil {
					return reportMigrationError(
						cmd,
						family,
						current.Name,
						current.Status.Phase,
						err,
					)
				}
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			if dryRun {
				return writeOfflineMigrationDryRunNotice(
					cmd,
					r,
					backend,
					family,
					object,
					"rollback",
				)
			}

			return writeOfflineMigrationNextSteps(cmd, r, backend, family, object)
		},
	}

	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newScopedOfflineMigrationCleanupCommand(
	source workflowSource,
	scope recordScope,
) *cobra.Command {
	var (
		dryRun  bool
		options app.MigrationCleanupOptions
	)

	command := &cobra.Command{
		Use: "cleanup " + workflowArgLabel(
			source,
		),
		Short: "Cleanup an offline migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, backend, err := r.loadMigrationWithBackend(
				ctx,
				cmd,
				runtime,
				args[0],
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

			family := sessionFamilyCommand(scope, "migrate")

			switch current := object.(type) {
			case *v1alpha1.Migration:
				executor, err := r.migrationExecutor(runtime, backend)
				if err != nil {
					return err
				}

				if dryRun {
					err = executor.ValidateCleanup(ctx, current, options)
				} else {
					err = executor.Cleanup(ctx, current, options)
				}

				if err != nil {
					return reportMigrationCleanupError(
						cmd,
						family,
						workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), current),
						current.Name,
						options,
						err,
					)
				}

			case *v1alpha1.ClusterMigration:
				executor, err := r.clusterMigrationExecutor(runtime, cmd, current, backend)
				if err != nil {
					return err
				}

				if dryRun {
					err = executor.ValidateCleanup(ctx, current, options)
				} else {
					err = executor.Cleanup(ctx, current, options)
				}

				if err != nil {
					return reportMigrationCleanupError(
						cmd,
						family,
						clusterMigrationStorageNamespace(current),
						current.Name,
						options,
						err,
					)
				}
			}

			if options.DeleteSession && !dryRun {
				return writeDeletedWorkflow(cmd, "migration workflow", object.GetName())
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			if dryRun {
				return writeDryRunNotice(
					cmd.ErrOrStderr(),
					cleanupExecuteCommand(
						cmd,
						guidancePrefixesForCommand(
							cmd,
							workflowHintNamespace(backend, r, cmd, object),
						).pvcMigrate,
						family,
						workflowHintNamespace(backend, r, cmd, object),
						object.GetName(),
						options.UnusedStoragePolicy,
						options.Finalize,
						options.DeleteSession,
					),
				)
			}

			return nil
		},
	}
	command.Flags().
		StringVar(&options.UnusedStoragePolicy, "unused-storage-policy", "", "Keep or Delete replaced storage; defaults to the recorded policy. Delete removes the old source PV after a completed cutover, or the staged destination after a rollback or abort; the PVC the workload runs on is always kept")
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Release retained storage ownership and close the rollback window")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the workflow record after cleanup")
	bindDryRun(command, &dryRun)

	return command
}

func writeOfflineMigrationNextSteps(
	cmd *cobra.Command,
	r *rootState,
	backend string,
	family string,
	object crclient.Object,
) error {
	namespace := workflowHintNamespace(backend, r, cmd, object)

	return writeWorkflowNextSteps(
		cmd.ErrOrStderr(),
		cmd,
		guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
		family,
		namespace,
		object.GetName(),
		workflowObjectPhase(object),
		true,
	)
}

func writeOfflineMigrationDryRunNotice(
	cmd *cobra.Command,
	r *rootState,
	backend string,
	family string,
	object crclient.Object,
	subcommand string,
) error {
	namespace := workflowHintNamespace(backend, r, cmd, object)

	return writeDryRunNotice(
		cmd.ErrOrStderr(),
		lifecycleExecuteCommand(
			cmd,
			guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
			family,
			subcommand,
			namespace,
			object.GetName(),
		),
	)
}

func reportMigrationCleanupError(
	cmd *cobra.Command,
	family, namespace, name string,
	options app.MigrationCleanupOptions,
	cause error,
) error {
	if blocker, ok := errors.AsType[*app.CleanupPodBlockerError](cause); ok {
		if err := writeCleanupPodBlockerGuidance(cmd.ErrOrStderr(), cmd, blocker); err != nil {
			cause = errors.Join(cause, err)
		}
	}

	prefix := guidancePrefixesForCommand(cmd, namespace).pvcMigrate

	retry := "cleanup " + workflowHintAddress(cmd, namespace, name)
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
		"Cleanup stopped before confirmed completion. Inspect current state: %s %s status %s\nRevalidate cleanup before retrying: %s --yes %s %s --dry-run\n",
		prefix,
		workflowCommandPath(cmd, family),
		workflowHintAddress(cmd, namespace, name),
		prefix,
		workflowCommandPath(cmd, family),
		retry,
	)

	return errors.Join(cause, err)
}
