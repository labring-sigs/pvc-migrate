package cli

import (
	"context"
	"fmt"
	"slices"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) addMoveLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newMoveStatusCommand(),
		r.newMoveResumeCommand(),
		r.newMoveAbortCommand(),
		r.newMoveRollbackCommand(),
		r.newMoveCleanupCommand(),
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

func (r *rootState) loadMove(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
) (*v1alpha1.Move, kube.WorkflowStore[*v1alpha1.Move], error) {
	store, err := moveStore(runtime, r.global.sessionNamespace)
	if err != nil {
		return nil, nil, err
	}

	// Session records carry no namespace of their own; the ConfigMap location
	// is the session namespace, not a workload namespace.
	object, err := store.Load(ctx, crclient.ObjectKey{Name: id})
	if err == nil {
		return object, store, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, nil, err
	}

	// Controller-submitted Move CRs live in the source namespace. Probe the
	// namespaces a caller could have addressed.
	crdStore, err := cliCRDWorkflowStore(
		runtime,
		func() *v1alpha1.Move { return &v1alpha1.Move{} },
	)
	if err != nil {
		return nil, nil, err
	}

	var lastErr error
	for _, namespace := range r.moveProbeNamespaces(cmd) {
		object, err := crdStore.Load(ctx, crclient.ObjectKey{Name: id, Namespace: namespace})
		if apierrors.IsNotFound(err) {
			lastErr = err
			continue
		}

		if err != nil {
			return nil, nil, err
		}

		return object, crdStore, nil
	}

	if lastErr == nil {
		lastErr = apierrors.NewNotFound(
			schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "moves"}, id,
		)
	}

	return nil, nil, reportSessionLookupError(cmd, r.global.sessionNamespace, id, lastErr)
}

// moveProbeNamespaces lists the tenant namespaces a controller-submitted Move
// CR lookup must cover.
func (r *rootState) moveProbeNamespaces(cmd *cobra.Command) []string {
	namespaces := make([]string, 0, 3)

	if cmd != nil {
		for _, name := range []string{"source-namespace", "namespace", "workflow-namespace"} {
			flag := cmd.Flags().Lookup(name)
			if flag == nil {
				continue
			}

			value, err := cmd.Flags().GetString(name)
			if err != nil || strings.TrimSpace(value) == "" || value == "default" {
				continue
			}

			if !slices.Contains(namespaces, strings.TrimSpace(value)) {
				namespaces = append(namespaces, strings.TrimSpace(value))
			}
		}
	}

	if candidate := strings.TrimSpace(r.global.workflowNamespace); candidate != "" &&
		!slices.Contains(namespaces, candidate) {
		namespaces = append(namespaces, candidate)
	}

	if candidate := strings.TrimSpace(r.global.sessionNamespace); candidate != "" &&
		!slices.Contains(namespaces, candidate) {
		namespaces = append(namespaces, candidate)
	}

	return namespaces
}

func (r *rootState) newMoveStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
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
				object, _, err := r.loadMove(ctx, cmd, runtime, args[0])
				if err != nil {
					return err
				}

				return runtime.printer.Print(object)
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
	validate, execute moveAction,
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

		object, store, err := r.loadMove(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		executor := app.NewMoveExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLocker(runtime),
			moveStorageNamespace(object),
		)
		if dryRun {
			if err := validate(ctx, executor, object); err != nil {
				return reportMoveError(cmd, object, err)
			}
			return runtime.printer.Print(object)
		}

		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		if err := execute(ctx, executor, object); err != nil {
			return reportMoveError(cmd, object, err)
		}

		return runtime.printer.Print(object)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newMoveAbortCommand() *cobra.Command {
	return r.moveLifecycleCommand("abort", "Abort a move workflow",
		func(_ context.Context, executor *app.MoveExecutor, object *v1alpha1.Move) error {
			return executor.ValidateAbort(object)
		},
		func(ctx context.Context, executor *app.MoveExecutor, object *v1alpha1.Move) error {
			return executor.Abort(ctx, object)
		})
}

func (r *rootState) newMoveRollbackCommand() *cobra.Command {
	return r.moveLifecycleCommand("rollback", "Restore the original PVC namespace and name",
		func(ctx context.Context, executor *app.MoveExecutor, object *v1alpha1.Move) error {
			return executor.ValidateRollback(ctx, object)
		},
		func(ctx context.Context, executor *app.MoveExecutor, object *v1alpha1.Move) error {
			return executor.Rollback(ctx, object)
		})
}

func (r *rootState) newMoveResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
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

		object, store, err := r.loadMove(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		executor := app.NewMoveExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLocker(runtime),
			moveStorageNamespace(object),
		)
		if dryRun {
			if err := executor.ValidateResume(ctx, object); err != nil {
				return reportMoveError(cmd, object, err)
			}
			return runtime.printer.Print(object)
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

		return runtime.printer.Print(object)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newMoveCleanupCommand() *cobra.Command {
	var (
		options app.IdentityCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup SESSION",
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

		object, store, err := r.loadMove(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		executor := app.NewMoveExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLocker(runtime),
			moveStorageNamespace(object),
		)
		if dryRun {
			if err := executor.ValidateCleanup(ctx, object, options); err != nil {
				return reportMoveError(cmd, object, err)
			}
			return runtime.printer.Print(object)
		}

		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		if err := executor.Cleanup(ctx, object, options); err != nil {
			return reportMoveError(cmd, object, err)
		}

		if options.DeleteSession {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "Deleted move workflow %s.\n", object.Name)
			return err
		}

		return runtime.printer.Print(object)
	}
	bindIdentityCleanupFlags(command, &options)
	bindDryRun(command, &dryRun)

	return command
}
