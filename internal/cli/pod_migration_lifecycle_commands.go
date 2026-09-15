package cli

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) newPodMigrationStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
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
				object, _, err := r.loadPodMigration(ctx, runtime, args[0])
				if err != nil {
					return err
				}

				return runtime.printer.Print(object)
			}

			sessions, err := runtime.clusterPodMigrationSessionStore.List(
				ctx,
				workflowNamespaceForCommand(r, cmd),
			)
			if err != nil {
				return err
			}

			objects, err := runtime.clusterPodMigrationStore.List(
				ctx,
				workflowNamespaceForCommand(r, cmd),
			)
			if err != nil {
				return err
			}

			return runtime.printer.Print(append(sessions, objects...))
		},
	}
}

func (r *rootState) newPodMigrationResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue a Pod migration from its persisted phase",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, executor, err := r.loadPodMigration(ctx, runtime, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				if err := executor.Validate(ctx, object); err != nil {
					return err
				}
				return runtime.printer.Print(object)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := executor.RequestResume(ctx, object); err != nil {
				return err
			}

			if err := executor.Run(ctx, object); err != nil {
				return err
			}

			return runtime.printer.Print(object)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort SESSION",
		Short: "Stop a Pod migration before cutover",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, executor, err := r.loadPodMigration(ctx, runtime, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				if err := executor.ValidateAbort(ctx, object); err != nil {
					return err
				}
				return runtime.printer.Print(object)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := executor.Abort(ctx, object); err != nil {
				return err
			}

			return runtime.printer.Print(object)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationRollbackCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "rollback SESSION",
		Short: "Restore source bindings after Pod migration cutover",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, executor, err := r.loadPodMigration(ctx, runtime, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				if err := executor.ValidateRollback(ctx, object); err != nil {
					return err
				}
				return runtime.printer.Print(object)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := executor.Rollback(ctx, object); err != nil {
				return err
			}

			return runtime.printer.Print(object)
		},
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationCleanupCommand() *cobra.Command {
	var (
		options app.MigrationCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Apply storage reclaim policies and close the Pod migration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			object, executor, err := r.loadPodMigration(ctx, runtime, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				if err := executor.ValidateCleanup(ctx, object, options); err != nil {
					return err
				}
				return runtime.printer.Print(object)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := executor.Cleanup(ctx, object, options); err != nil {
				return err
			}

			if options.DeleteSession {
				return nil
			}

			return runtime.printer.Print(object)
		},
	}
	bindMigrationCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)

	return command
}

func bindMigrationCleanupFlags(command *cobra.Command, options *app.MigrationCleanupOptions) {
	command.Flags().
		StringVar(&options.SourcePVReclaimPolicy, "source-pv-reclaim-policy", "", "Source PV reclaim policy")
	command.Flags().
		StringVar(&options.DestinationPVCReclaimPolicy, "destination-pvc-reclaim-policy", "", "Destination PVC reclaim policy")
	command.Flags().BoolVar(&options.Finalize, "finalize", false, "Finalize cleanup")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the workflow after cleanup")
}

// loadPodMigration resolves one Pod migration from session (ConfigMap) or
// controller (CRD) storage and returns the executor bound to that backend.
func (r *rootState) loadPodMigration(
	ctx context.Context,
	runtime *commandRuntime,
	name string,
) (*v1alpha1.ClusterPodMigration, *app.ClusterPodMigrationExecutor, error) {
	if runtime == nil || runtime.clusterPodMigrationStore == nil ||
		runtime.clusterPodMigrationSessionStore == nil {
		return nil, nil, domain.NewError(
			domain.ErrorInternal,
			"pod migration",
			"workflow store is not configured",
		)
	}

	key := crclient.ObjectKey{Name: name}

	object, err := runtime.clusterPodMigrationSessionStore.Load(ctx, key)
	if err == nil {
		return object, runtime.clusterPodMigrationSessionExecutor, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, nil, err
	}

	object, err = runtime.clusterPodMigrationStore.Load(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	return object, runtime.clusterPodMigrationExecutor, nil
}
