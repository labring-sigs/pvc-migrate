package cli

import (
	"context"
	"slices"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

			namespace := workflowNamespaceForCommand(r, cmd)

			sessions, err := runtime.clusterPodMigrationSessionStore.List(ctx, namespace)
			if err != nil {
				return err
			}

			objects, err := runtime.clusterPodMigrationStore.List(ctx, namespace)
			if err != nil {
				return err
			}

			list := make([]crclient.Object, 0, len(sessions)+len(objects))
			for _, object := range sessions {
				list = append(list, object)
			}

			for _, object := range objects {
				list = append(list, object)
			}

			if crdListable(runtime) {
				for _, probe := range r.podMigrationProbeNamespaces(nil) {
					namespaced, err := runtime.podMigrationStore.List(ctx, probe)
					if err != nil {
						return err
					}

					for _, object := range namespaced {
						list = append(list, object)
					}
				}
			}

			return runtime.printer.Print(list)
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

			object, dispatch, err := r.loadPodMigration(ctx, runtime, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				if err := dispatch.validate(ctx); err != nil {
					return err
				}
				return runtime.printer.Print(object)
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

			object, dispatch, err := r.loadPodMigration(ctx, runtime, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				if err := dispatch.validateAbort(ctx); err != nil {
					return err
				}
				return runtime.printer.Print(object)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := dispatch.abort(ctx); err != nil {
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

			object, dispatch, err := r.loadPodMigration(ctx, runtime, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				if err := dispatch.validateRollback(ctx); err != nil {
					return err
				}
				return runtime.printer.Print(object)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := dispatch.rollback(ctx); err != nil {
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

			object, dispatch, err := r.loadPodMigration(ctx, runtime, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				if err := dispatch.validateCleanup(ctx, options); err != nil {
					return err
				}
				return runtime.printer.Print(object)
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := dispatch.cleanup(ctx, options); err != nil {
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
		StringVar(&options.UnusedStoragePolicy, "unused-storage-policy", "", "What happens to storage that is no longer in use at a terminal state: Keep or Delete; defaults to the recorded policy")
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

// loadPodMigration resolves one Pod migration from session (ConfigMap),
// cluster CRD, or namespaced CRD storage and returns the executor operations
// bound to the backend that owns the record.
func (r *rootState) loadPodMigration(
	ctx context.Context,
	runtime *commandRuntime,
	name string,
) (crclient.Object, *podMigrationDispatch, error) {
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
		executor := runtime.clusterPodMigrationSessionExecutor

		return object, &podMigrationDispatch{
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
		}, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, nil, err
	}

	object, err = runtime.clusterPodMigrationStore.Load(ctx, key)
	if err == nil {
		executor := runtime.clusterPodMigrationExecutor

		return object, &podMigrationDispatch{
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
		}, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, nil, err
	}

	if runtime.podMigrationStore == nil {
		return nil, nil, apierrors.NewNotFound(
			schema.GroupResource{
				Group:    v1alpha1.GroupVersion.Group,
				Resource: "clusterpodmigrations",
			},
			name,
		)
	}

	for _, namespace := range r.podMigrationProbeNamespaces(nil) {
		namespaced, nsErr := runtime.podMigrationStore.Load(
			ctx, crclient.ObjectKey{Name: name, Namespace: namespace},
		)
		if apierrors.IsNotFound(nsErr) {
			continue
		}

		if nsErr != nil {
			return nil, nil, nsErr
		}

		executor := runtime.podMigrationExecutor

		return namespaced, &podMigrationDispatch{
			object:           namespaced,
			validate:         func(ctx context.Context) error { return executor.Validate(ctx, namespaced) },
			validateAbort:    func(ctx context.Context) error { return executor.ValidateAbort(ctx, namespaced) },
			validateRollback: func(ctx context.Context) error { return executor.ValidateRollback(ctx, namespaced) },
			validateCleanup: func(ctx context.Context, options app.MigrationCleanupOptions) error {
				return executor.ValidateCleanup(ctx, namespaced, options)
			},
			requestResume: func(ctx context.Context) error { return executor.RequestResume(ctx, namespaced) },
			run:           func(ctx context.Context) error { return executor.Run(ctx, namespaced) },
			abort:         func(ctx context.Context) error { return executor.Abort(ctx, namespaced) },
			rollback:      func(ctx context.Context) error { return executor.Rollback(ctx, namespaced) },
			cleanup: func(ctx context.Context, options app.MigrationCleanupOptions) error {
				return executor.Cleanup(ctx, namespaced, options)
			},
		}, nil
	}

	return nil, nil, apierrors.NewNotFound(
		schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "podmigrations"}, name,
	)
}

// podMigrationProbeNamespaces lists the tenant namespaces a namespaced
// PodMigration lookup must cover.
func (r *rootState) podMigrationProbeNamespaces(_ *cobra.Command) []string {
	namespaces := make([]string, 0, 3)
	for _, candidate := range []string{r.global.workflowNamespace, r.global.sessionNamespace} {
		if strings.TrimSpace(candidate) != "" && !slices.Contains(namespaces, candidate) {
			namespaces = append(namespaces, strings.TrimSpace(candidate))
		}
	}

	return namespaces
}
