package cli

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// loadBackup resolves one backup from the backend its command family
// addresses — ConfigMap session records for the session commands, namespaced
// Backup CRs for the cr commands — and returns the store bound to that backend.
func (r *rootState) loadBackup(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	name string,
	source workflowSource,
) (*v1alpha1.Backup, kube.WorkflowStore[*v1alpha1.Backup], string, error) {
	namespace := r.workflowStorageNamespace(cmd)

	object, backend, err := r.loadWorkflowWithBackend(
		ctx,
		cmd,
		runtime,
		namespace,
		name,
		map[domain.ControllerKind]crclient.Object{
			domain.ControllerKindBackup: &v1alpha1.Backup{},
		},
		source,
	)
	if err != nil {
		return nil, nil, "", reportSessionLookupError(cmd, namespace, name, err)
	}

	backup, ok := object.(*v1alpha1.Backup)
	if !ok {
		return nil, nil, "", domain.NewError(
			domain.ErrorValidation,
			"backup",
			"stored workflow is not a backup",
		)
	}

	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		namespace,
		func() *v1alpha1.Backup { return &v1alpha1.Backup{} },
	)
	if err != nil {
		return nil, nil, "", err
	}

	return backup, store, backend, nil
}

func (r *rootState) newBackupStatusCommand(source workflowSource) *cobra.Command {
	return &cobra.Command{
		Use:   "status [" + workflowArgLabel(source) + "]",
		Short: "Show one backup or list backups",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				object, _, backend, err := r.loadBackup(ctx, cmd, runtime, args[0], source)
				if err != nil {
					return err
				}

				return printRepositoryWorkflowResult(
					cmd,
					runtime,
					object,
					"backup",
					workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object),
				)
			}

			objects := []crclient.Object{}

			if source == sourceController {
				if !crdListable(runtime) {
					return runtime.printer.Print(objects)
				}

				kind := domain.ControllerKindBackup
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

			namespace := r.workflowStorageNamespace(cmd)

			store, err := cliWorkflowStore(
				runtime,
				namespace,
				func() *v1alpha1.Backup { return &v1alpha1.Backup{} },
			)
			if err != nil {
				return err
			}

			// Records live in the session namespace while carrying the tenant
			// namespace in metadata; the bare list shows every record the
			// store holds, so no tenant filter applies here.
			items, err := store.List(ctx, "")
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

func (r *rootState) newBackupResumeCommand(source workflowSource) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume " + workflowArgLabel(source),
		Short: "Continue a backup from its persisted phase",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, backend, err := r.loadBackup(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		namespace := workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object)

		executor := r.backupExecutor(runtime, namespace, store, backend)
		if object.Status.Phase == domain.PhaseCompleted ||
			object.Status.Phase == domain.PhaseAborted {
			return printRepositoryWorkflowResult(cmd, runtime, object, "backup", namespace)
		}

		if dryRun {
			if object.Status.Phase == domain.PhaseAborting ||
				object.Status.ResumeFrom == domain.PhaseAborting {
				if err := executor.ValidateAbort(ctx, object); err != nil {
					return err
				}
			} else if object.Status.Plan != nil {
				if err := executor.ValidateSourcePlan(ctx, object); err != nil {
					return err
				}
			} else if err := executor.Validate(ctx, object); err != nil {
				return err
			}

			if err := printRepositoryWorkflowResult(
				cmd,
				runtime,
				object,
				"backup",
				namespace,
			); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"backup",
					"resume",
					namespace,
					object.Name,
				),
			)
		}

		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		if err := executor.RequestResume(ctx, object); err != nil {
			return reportRepositoryWorkflowError(
				cmd,
				"backup",
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
					if err := runtime.planner.PlanBackup(
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
					"backup",
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
				"backup",
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
			"backup",
			namespace,
		)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newBackupAbortCommand(source workflowSource) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort " + workflowArgLabel(source),
		Short: "Abort a backup and retain published recovery points",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, backend, err := r.loadBackup(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		namespace := workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object)

		executor := r.backupExecutor(runtime, namespace, store, backend)
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
				"backup",
				object.Namespace,
				object.Name,
				object.Status.Phase,
				err,
			)
		}

		if err := printRepositoryWorkflowResult(
			cmd,
			runtime,
			object,
			"backup",
			namespace,
		); err != nil {
			return err
		}

		if dryRun {
			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"backup",
					"abort",
					namespace,
					object.Name,
				),
			)
		}

		return nil
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newBackupCleanupCommand(source workflowSource) *cobra.Command {
	var options backup.BackupCleanupOptions

	var dryRun bool

	command := &cobra.Command{
		Use:   "cleanup " + workflowArgLabel(source),
		Short: "Finalize backup resources and clean up workflow metadata",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, store, backend, err := r.loadBackup(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		namespace := workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object)

		executor := r.backupExecutor(runtime, namespace, store, backend)
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
				"backup",
				object.Namespace,
				object.Name,
				object.Status.Phase,
				err,
			)
		}

		if options.DeleteSession && !dryRun {
			return writeDeletedWorkflow(cmd, "backup workflow", object.Name)
		}

		if err := printRepositoryWorkflowResult(
			cmd,
			runtime,
			object,
			"backup",
			r.workflowStorageNamespace(cmd),
		); err != nil {
			return err
		}

		if dryRun {
			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				cleanupExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, r.workflowStorageNamespace(cmd)).pvcMigrate,
					"backup",
					r.workflowStorageNamespace(cmd),
					object.Name,
					"",
					options.Finalize,
					options.DeleteSession,
				),
			)
		}

		return nil
	}
	command.Flags().BoolVar(
		&options.Finalize,
		"finalize",
		false,
		"Release session-owned repository and credential resources. Published recovery points and the source PVC are never deleted; remove recovery points with your object-store tooling",
	)
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete workflow metadata after finalization")
	bindDryRun(command, &dryRun)

	return command
}
