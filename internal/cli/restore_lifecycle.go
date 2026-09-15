package cli

import (
	"context"
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) loadRestore(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	name string,
) (*v1alpha1.Restore, kube.WorkflowStore[*v1alpha1.Restore], error) {
	namespace := r.workflowStorageNamespace(cmd)

	store, err := cliWorkflowStore(
		runtime,
		namespace,
		func() *v1alpha1.Restore { return &v1alpha1.Restore{} },
	)
	if err != nil {
		return nil, nil, err
	}

	key := crclient.ObjectKey{Name: name, Namespace: namespace}

	object, err := store.Load(ctx, key)
	if err == nil {
		return object, store, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, nil, err
	}

	crdStore, err := cliCRDWorkflowStore(
		runtime,
		func() *v1alpha1.Restore { return &v1alpha1.Restore{} },
	)
	if err != nil {
		return nil, nil, err
	}

	object, err = crdStore.Load(ctx, key)

	return object, crdStore, err
}

func (r *rootState) newRestoreStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
		Short: "Show one restore or list restores",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				object, _, err := r.loadRestore(ctx, cmd, runtime, args[0])
				if err != nil {
					return err
				}

				return printRepositoryWorkflowResult(
					cmd,
					runtime,
					object,
					"restore",
					r.workflowStorageNamespace(cmd),
				)
			}

			namespace := r.workflowStorageNamespace(cmd)

			store, err := cliWorkflowStore(
				runtime,
				namespace,
				func() *v1alpha1.Restore { return &v1alpha1.Restore{} },
			)
			if err != nil {
				return err
			}

			items, err := store.List(ctx, namespace)
			if err != nil {
				return err
			}

			objects := make([]crclient.Object, len(items))
			for i, object := range items {
				objects[i] = object
			}

			if crdListable(runtime) && (len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindRestore)) {
				crdStore, err := cliCRDWorkflowStore(
					runtime,
					func() *v1alpha1.Restore { return &v1alpha1.Restore{} },
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

			return runtime.printer.Print(objects)
		},
	}
}

func (r *rootState) newRestoreResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Continue a restore from its persisted phase",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, err := r.loadRestore(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		namespace := r.workflowStorageNamespace(cmd)

		executor := r.restoreExecutor(runtime, namespace, store)
		if object.Status.Phase == domain.PhaseCompleted ||
			object.Status.Phase == domain.PhaseAborted {
			return printRepositoryWorkflowResult(cmd, runtime, object, "restore", namespace)
		}

		if dryRun {
			if object.Status.Phase == domain.PhaseAborting ||
				object.Status.ResumeFrom == domain.PhaseAborting ||
				object.Status.Phase == domain.PhaseWarmCopied ||
				object.Status.ResumeFrom == domain.PhaseWarmCopied {
				if err := executor.Validate(ctx, object); err != nil {
					return err
				}
			} else if object.Status.Plan != nil {
				connection, err := r.loadRestoreRepositoryConnection(ctx, runtime, object)
				if err != nil {
					return err
				}

				if err := executor.ValidateDestinationPlan(
					ctx,
					object,
					connection.Config(),
				); err != nil {
					return err
				}
			} else if err := executor.Validate(ctx, object); err != nil {
				return err
			}

			return printRepositoryWorkflowResult(
				cmd,
				runtime,
				object,
				"restore",
				r.workflowStorageNamespace(cmd),
			)
		}

		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		if err := executor.RequestResume(ctx, object); err != nil {
			return reportRepositoryWorkflowError(
				cmd,
				"restore",
				object.Namespace,
				object.Name,
				object.Status.Phase,
				err,
			)
		}

		if object.Status.Plan == nil {
			err = kube.WithWorkflowLease(
				ctx,
				store,
				cliWorkflowLocker(runtime),
				namespace,
				object,
				false,
				func(ctx context.Context, _ kube.SessionLock) error {
					before := object.Status.DeepCopy()
					if err := runtime.planner.PlanRestore(
						ctx,
						object,
						r.global.toolImage,
					); err != nil {
						return err
					}

					object.Status.Phase = domain.PhasePlanned
					if err := executor.Prepare(ctx, object); err != nil {
						object.Status = *before
						return err
					}

					if err := store.Save(ctx, object); err != nil {
						object.Status = *before
						return err
					}

					return nil
				},
			)
			if err != nil {
				return reportRepositoryWorkflowError(
					cmd,
					"restore",
					object.Namespace,
					object.Name,
					object.Status.Phase,
					err,
				)
			}
		}

		if err := executor.Run(ctx, object); err != nil {
			return reportRepositoryWorkflowError(
				cmd,
				"restore",
				object.Namespace,
				object.Name,
				object.Status.Phase,
				err,
			)
		}

		return printRepositoryWorkflowResult(
			cmd,
			runtime,
			object,
			"restore",
			r.workflowStorageNamespace(cmd),
		)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRestoreAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort SESSION",
		Short: "Abort a restore and retain destination data",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, err := r.loadRestore(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		executor := r.restoreExecutor(runtime, r.workflowStorageNamespace(cmd), store)
		if dryRun {
			err = executor.ValidateAbort(ctx, object)
		} else {
			if err := r.confirm(ctx, cmd, object.Name); err != nil {
				return reportApprovalError(cmd, err)
			}

			err = executor.Abort(ctx, object)
		}

		if err != nil {
			return reportRepositoryWorkflowError(
				cmd,
				"restore",
				object.Namespace,
				object.Name,
				object.Status.Phase,
				err,
			)
		}

		return printRepositoryWorkflowResult(
			cmd,
			runtime,
			object,
			"restore",
			r.workflowStorageNamespace(cmd),
		)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newRestoreCleanupCommand() *cobra.Command {
	var options backup.RestoreCleanupOptions

	var dryRun bool

	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Finalize restore resources and clean up workflow metadata",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, err := r.loadRestore(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		executor := r.restoreExecutor(runtime, r.workflowStorageNamespace(cmd), store)
		if dryRun {
			err = executor.ValidateCleanup(ctx, object, options)
		} else {
			if options.Finalize || options.DeleteSession {
				if err := r.confirm(ctx, cmd, object.Name); err != nil {
					return reportApprovalError(cmd, err)
				}
			}

			err = executor.Cleanup(ctx, object, options)
		}

		if err != nil {
			return reportRepositoryWorkflowError(
				cmd,
				"restore",
				object.Namespace,
				object.Name,
				object.Status.Phase,
				err,
			)
		}

		if options.DeleteSession && !dryRun {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "Deleted restore workflow %s.\n", object.Name)
			return err
		}

		return printRepositoryWorkflowResult(
			cmd,
			runtime,
			object,
			"restore",
			r.workflowStorageNamespace(cmd),
		)
	}
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Release session-owned repository and credential resources")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete workflow metadata after finalization")
	bindDryRun(command, &dryRun)

	return command
}
