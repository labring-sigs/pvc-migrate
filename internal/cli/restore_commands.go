package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/spf13/cobra"
)

type restoreBucketFlags struct {
	createPVC               bool
	destinationStorageClass string
	destinationAccessMode   string
	destinationCapacity     string
	targetNode              string
	allowMounted            bool
	deleteExtraneous        bool
}

func (r *rootState) newRestoreCommand() *cobra.Command {
	return r.newRestoreTransferCommand()
}

func (r *rootState) newRestoreTransferCommand() *cobra.Command {
	flags := &restoreFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "restore",
		Short: "Restore object-storage backup data into a PVC",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			flags.prefixExplicit = cmd.Flags().Changed("prefix")
			if err := validateBucketFlags(&flags.bucketFlags, "destination-pvc"); err != nil {
				return reportPreSessionError(cmd, err)
			}

			runtime, err := r.runtime()
			if err != nil {
				return reportRuntimeError(cmd, err)
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			flags.accessKeyExplicit = cmd.Flags().Changed("access-key")
			flags.secretKeyExplicit = cmd.Flags().Changed("secret-key")

			flags.sessionTokenExplicit = cmd.Flags().Changed("session-token")

			controllerWorkflow := controllerWorkflowAvailable(runtime, domain.SessionTypeRestore)
			if flags.backupRepository != "" {
				if !controllerWorkflow {
					return reportTransferError(
						cmd,
						"restore",
						flags.namespace,
						flags.pvc,
						domain.NewError(
							domain.ErrorPrecondition,
							"backup repository",
							"--backup-repository requires the Restore CRD and controller mode",
						),
					)
				}

				if !kube.BackupRepositoryAvailable(runtime.clients.Discovery) {
					return reportTransferError(
						cmd,
						"restore",
						flags.namespace,
						flags.pvc,
						domain.NewError(
							domain.ErrorPrecondition,
							"backup repository",
							"BackupRepository CRD is not served by this cluster; install deploy/crd.yaml",
						),
					)
				}

				if err := validateControllerRepositoryFlags(&flags.bucketFlags); err != nil {
					return reportTransferError(cmd, "restore", flags.namespace, flags.pvc, err)
				}
			}

			if runtime.controllerModeExplicit && controllerWorkflow &&
				flags.backupRepository == "" {
				return reportTransferError(
					cmd,
					"restore",
					flags.namespace,
					flags.pvc,
					domain.NewError(
						domain.ErrorPrecondition,
						"backup repository",
						"controller Restore workflows require --backup-repository",
					),
				)
			}

			var store *objectstore.Store
			if flags.backupRepository != "" {
				store, err = newControllerRepositoryStore(&flags.bucketFlags)
			} else {
				if err := loadS3Credentials(
					ctx,
					runtime.clients.Kubernetes,
					&flags.bucketFlags,
				); err != nil {
					return reportTransferError(cmd, "restore", flags.namespace, flags.pvc, err)
				}

				store, err = r.newObjectStore(ctx, &flags.bucketFlags)
			}

			if err != nil {
				return reportTransferError(cmd, "restore", flags.namespace, flags.pvc, err)
			}

			request := r.objectTransferRequest(runtime, &flags.bucketFlags, store, false, false)
			if flags.backupRepository != "" {
				request.SessionNamespace = r.controllerPlanSessionNamespace(
					runtime,
					domain.SessionTypeRestore,
					flags.namespace,
					flags.namespace,
				)
			}

			request.SkipManifestCheck = flags.backupRepository != ""
			request.ToolImageProber = kube.NewToolImageProber(runtime.clients.Kubernetes)
			applyRestoreRequest(&request, flags.restore)

			plan, err := backup.Preflight(ctx, runtime.clients.Kubernetes, request, true)
			if err != nil {
				return reportTransferError(cmd, "restore", flags.namespace, flags.pvc, err)
			}

			if dryRun {
				if err := printerFor(r).Print(plan); err != nil {
					return reportTransferError(cmd, "restore", flags.namespace, flags.pvc, err)
				}

				return writeTransferDryRunGuidance(
					cmd.ErrOrStderr(),
					"restore",
					flags.namespace,
					flags.pvc,
					kubectlCommandPrefixForCommand(cmd),
				)
			}

			if err := r.confirm(ctx, cmd, flags.name); err != nil {
				return reportApprovalError(cmd, err)
			}

			if err := requireControllerWorkflow(runtime, domain.SessionTypeRestore); err != nil {
				return reportTransferError(cmd, "restore", flags.namespace, flags.pvc, err)
			}

			// Profile-backed restores are submitted as CRs. Static-credential
			// restores keep the historical synchronous path in auto/session mode;
			// creating a ConfigMap session and then executing directly would leave
			// an orphaned planned record.
			if controllerWorkflowAvailable(runtime, domain.SessionTypeRestore) &&
				flags.backupRepository != "" {
				session, submitErr := backup.SubmitRestore(
					ctx,
					runtime.clients.Kubernetes,
					request,
					*plan,
				)
				if submitErr != nil {
					return reportTransferError(
						cmd,
						"restore",
						flags.namespace,
						flags.pvc,
						submitErr,
					)
				}

				if deferred, deferErr := deferControllerExecution(
					ctx, cmd, runtime, session,
				); deferred {
					return deferErr
				}

				if session.Backend == kube.SessionBackendCRD {
					return nil
				}
			}

			err = backup.Run(ctx, runtime.clients.Kubernetes, request, true)
			if err != nil {
				return reportTransferError(cmd, "restore", flags.namespace, flags.pvc, err)
			}

			return r.printObjectTransferResult(
				cmd,
				runtime,
				&flags.bucketFlags,
				"restore",
				true,
				false,
				plan,
				store,
			)
		},
	}
	bindRestoreFlags(command, flags)
	bindDryRun(command, &dryRun)
	command.AddCommand(
		r.newRestorePlanCommand(),
		r.newRestoreStatusCommand(),
		r.newRestoreResumeCommand(),
		r.newRestoreAbortCommand(),
		r.newRestoreCleanupCommand(),
	)

	return command
}

func (r *rootState) newRestorePlanCommand() *cobra.Command {
	flags := &restoreFlags{}
	command := r.newObjectTransferPlanCommand(
		"restore plan",
		"destination-pvc",
		false,
		true,
		&flags.bucketFlags,
		func(request *backup.Request) error {
			applyRestoreRequest(request, flags.restore)
			return nil
		},
	)
	bindRestoreFlags(command, flags)

	return command
}

func applyRestoreRequest(req *backup.Request, flags restoreBucketFlags) {
	req.CreatePVC = flags.createPVC
	req.DestinationStorageClass = flags.destinationStorageClass
	req.DestinationAccessMode = flags.destinationAccessMode
	req.DestinationCapacity = flags.destinationCapacity
	req.TargetNode = flags.targetNode
	req.AllowMounted = flags.allowMounted
	req.DeleteExtraneousFiles = flags.deleteExtraneous
}

func bindRestoreBucketFlags(command *cobra.Command, flags *restoreBucketFlags) {
	command.Flags().
		BoolVar(&flags.createPVC, "create-pvc", false, "Create the destination PVC when it does not exist")
	command.Flags().
		StringVar(&flags.destinationStorageClass, "destination-storage-class", "", "StorageClass for a destination PVC created by restore")
	command.Flags().
		StringVar(&flags.destinationAccessMode, "destination-access-mode", "", "Access mode for a destination PVC created by restore")
	command.Flags().
		StringVar(&flags.destinationCapacity, "destination-capacity", "", "Capacity for a destination PVC created by restore; defaults to the backup capacity")
	command.Flags().
		StringVar(&flags.targetNode, "target-node", "", "Node for restore tool scheduling and destination PVC binding")
	command.Flags().
		BoolVar(&flags.allowMounted, "allow-mounted", false, "Allow restore while the destination PVC has Pod consumers")
	command.Flags().
		BoolVar(&flags.deleteExtraneous, "delete-extraneous", false, "Delete destination files absent from the backup (destructive)")
}
