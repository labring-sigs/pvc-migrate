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
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) loadRestore(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	name string,
) (*v1alpha1.Restore, kube.WorkflowStore[*v1alpha1.Restore], string, error) {
	namespace := r.workflowStorageNamespace(cmd)

	object, backend, err := r.loadWorkflowWithBackend(
		ctx,
		cmd,
		runtime,
		namespace,
		name,
		map[domain.ControllerKind]crclient.Object{
			domain.ControllerKindRestore: &v1alpha1.Restore{},
		},
	)
	if err != nil {
		return nil, nil, "", reportSessionLookupError(cmd, namespace, name, err)
	}

	restore, ok := object.(*v1alpha1.Restore)
	if !ok {
		return nil, nil, "", domain.NewError(
			domain.ErrorValidation,
			"restore",
			"stored workflow is not a restore",
		)
	}

	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		namespace,
		func() *v1alpha1.Restore { return &v1alpha1.Restore{} },
	)
	if err != nil {
		return nil, nil, "", err
	}

	return restore, store, backend, nil
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
				object, _, backend, err := r.loadRestore(ctx, cmd, runtime, args[0])
				if err != nil {
					return err
				}

				return printRepositoryWorkflowResult(
					cmd,
					runtime,
					object,
					"restore",
					workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object),
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

		object, store, backend, err := r.loadRestore(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		namespace := workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object)

		executor := r.restoreExecutor(runtime, namespace, store, backend)
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
				namespace,
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
				cliWorkflowLockerForBackend(runtime, backend),
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

					if err := saveCLIPlannedWorkflow(
						ctx,
						store,
						object,
						&object.Status.WorkflowStatus,
					); err != nil {
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
			namespace,
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

		object, store, backend, err := r.loadRestore(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		namespace := workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object)

		executor := r.restoreExecutor(runtime, namespace, store, backend)
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
			namespace,
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

		object, store, backend, err := r.loadRestore(ctx, cmd, runtime, args[0])
		if err != nil {
			return err
		}

		namespace := workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object)

		executor := r.restoreExecutor(runtime, namespace, store, backend)
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
	command.Flags().BoolVar(
		&options.Finalize,
		"finalize",
		false,
		"Release session-owned repository and credential resources. A completed restore's destination PVC is always kept; a failed restore keeps its created destination unless the workflow's recorded unusedStoragePolicy is Delete",
	)
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete workflow metadata after finalization")
	bindDryRun(command, &dryRun)

	return command
}
