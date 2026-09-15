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

func (r *rootState) newOfflineMigrationStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
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
				object, _, err := r.loadMigrationWithBackend(ctx, cmd, runtime, args[0])
				if err != nil {
					return err
				}

				return runtime.printer.Print(object)
			}

			namespace := r.workflowStorageNamespace(cmd)
			objects := []crclient.Object{}

			if len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindMigration) {
				store, err := cliWorkflowStore(
					runtime,
					namespace,
					func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
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
				slices.Contains(runtime.controllerKinds, domain.ControllerKindClusterMigration) {
				store, err := cliWorkflowStore(
					runtime,
					namespace,
					func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} },
				)
				if err != nil {
					return err
				}

				filter := ""

				items, err := store.List(ctx, filter)
				if err != nil {
					return err
				}

				for _, object := range items {
					objects = append(objects, object)
				}
			}

			if crdListable(runtime) && (len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindMigration)) {
				crdStore, err := cliCRDWorkflowStore(
					runtime,
					func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
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
				slices.Contains(runtime.controllerKinds, domain.ControllerKindClusterMigration)) {
				crdStore, err := cliCRDWorkflowStore(
					runtime,
					func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} },
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

func (r *rootState) newOfflineMigrationResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue an offline migration from its checkpoint",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			return r.resumeMigration(ctx, cmd, runtime, args[0], dryRun)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newOfflineMigrationAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use: "abort SESSION", Short: "Abort an offline migration", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, backend, err := r.loadMigrationWithBackend(ctx, cmd, runtime, args[0])
			if err != nil {
				return err
			}

			if !dryRun {
				if err := r.confirm(ctx, cmd, object.GetName()); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			switch current := object.(type) {
			case *v1alpha1.Migration:
				executor, err := r.migrationExecutor(runtime, cmd, backend)
				if err != nil {
					return err
				}

				if dryRun {
					err = executor.ValidateAbort(ctx, current)
				} else {
					err = executor.Abort(ctx, current)
				}

				if err != nil {
					return reportMigrationError(cmd, current.Name, current.Status.Phase, err)
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
					return reportMigrationError(cmd, current.Name, current.Status.Phase, err)
				}
			}

			return runtime.printer.Print(object)
		},
	}

	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newOfflineMigrationRollbackCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use: "rollback SESSION", Short: "Rollback an offline migration", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, backend, err := r.loadMigrationWithBackend(ctx, cmd, runtime, args[0])
			if err != nil {
				return err
			}

			if !dryRun {
				if err := r.confirm(ctx, cmd, object.GetName()); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			switch current := object.(type) {
			case *v1alpha1.Migration:
				executor, err := r.migrationExecutor(runtime, cmd, backend)
				if err != nil {
					return err
				}

				if dryRun {
					err = executor.ValidateRollback(ctx, current)
				} else {
					err = executor.Rollback(ctx, current)
				}

				if err != nil {
					return reportMigrationError(cmd, current.Name, current.Status.Phase, err)
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
					return reportMigrationError(cmd, current.Name, current.Status.Phase, err)
				}
			}

			return runtime.printer.Print(object)
		},
	}

	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newOfflineMigrationCleanupCommand() *cobra.Command {
	var (
		dryRun  bool
		options app.MigrationCleanupOptions
	)

	command := &cobra.Command{
		Use: "cleanup SESSION", Short: "Cleanup an offline migration", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, backend, err := r.loadMigrationWithBackend(ctx, cmd, runtime, args[0])
			if err != nil {
				return err
			}

			if !dryRun {
				if err := r.confirm(ctx, cmd, object.GetName()); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			switch current := object.(type) {
			case *v1alpha1.Migration:
				executor, err := r.migrationExecutor(runtime, cmd, backend)
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
						r.workflowStorageNamespace(cmd),
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
						r.workflowStorageNamespace(cmd),
						current.Name,
						options,
						err,
					)
				}
			}

			if options.DeleteSession && !dryRun {
				_, err := fmt.Fprintf(
					cmd.OutOrStdout(),
					"Deleted migration workflow %s.\n",
					object.GetName(),
				)

				return err
			}

			return runtime.printer.Print(object)
		},
	}
	command.Flags().
		StringVar(&options.SourcePVReclaimPolicy, "source-pv-reclaim-policy", "", "Old source PV policy: Retain or Delete; defaults to the recorded policy")
	command.Flags().
		StringVar(&options.DestinationPVCReclaimPolicy, "destination-pvc-reclaim-policy", "", "Destination PVC policy: Retain or Delete; defaults to the recorded policy")
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Release retained storage ownership and close the rollback window")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the workflow record after cleanup")
	bindDryRun(command, &dryRun)

	return command
}

func reportMigrationCleanupError(
	cmd *cobra.Command,
	namespace, name string,
	options app.MigrationCleanupOptions,
	cause error,
) error {
	if blocker, ok := errors.AsType[*app.CleanupPodBlockerError](cause); ok {
		if err := writeCleanupPodBlockerGuidance(cmd.ErrOrStderr(), cmd, blocker); err != nil {
			cause = errors.Join(cause, err)
		}
	}

	prefix := guidancePrefixesForCommand(cmd, namespace).pvcMigrate

	retry := "migrate cleanup " + shellQuote(name)
	if options.SourcePVReclaimPolicy != "" {
		retry += " --source-pv-reclaim-policy " + shellQuote(options.SourcePVReclaimPolicy)
	}

	if options.DestinationPVCReclaimPolicy != "" {
		retry += " --destination-pvc-reclaim-policy " + shellQuote(
			options.DestinationPVCReclaimPolicy,
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
		"Cleanup stopped before confirmed completion. Inspect current state: %s migrate status %s\nRevalidate cleanup before retrying: %s %s --dry-run\n",
		prefix,
		shellQuote(name),
		prefix,
		retry,
	)

	return errors.Join(cause, err)
}
