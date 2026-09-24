package cli

import (
	"context"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (r *rootState) newRestoreCommand() *cobra.Command {
	command := r.restoreSubmissionCommand(false)
	command.AddCommand(
		r.newRestorePlanCommand(),
		r.newRestoreStatusCommand(sourceSession),
		r.newRestoreResumeCommand(sourceSession),
		r.newRestoreAbortCommand(sourceSession),
		r.newRestoreCleanupCommand(sourceSession),
	)

	return command
}

func (r *rootState) newRestorePlanCommand() *cobra.Command {
	return r.restoreSubmissionCommand(true)
}

// restoreSubmissionCommand builds the session restore command and its plan-only
// preview. Controller submission lives in the cr restore create command.
func (r *rootState) restoreSubmissionCommand(planOnly bool) *cobra.Command {
	object := &v1alpha1.Restore{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Restore"},
	}
	flags := &s3RepositoryFlags{}
	dryRun := planOnly

	command := &cobra.Command{
		Use:   "restore",
		Short: "Restore an S3 recovery point into a PVC",
		Args:  cobra.NoArgs,
	}
	if planOnly {
		command.Use, command.Short = "plan", "Validate a restore without mutations"
	}

	command.RunE = func(cmd *cobra.Command, _ []string) error {
		return r.runRestoreObject(cmd, object.DeepCopy(), flags, dryRun)
	}
	f := command.Flags()
	f.StringVar(&object.Name, "id", "", "Workflow ID; generated when omitted")
	f.StringVarP(
		&object.Namespace,
		"namespace",
		"n",
		"default",
		"Destination PVC and repository namespace",
	)
	f.StringVar(&object.Spec.DestinationPVC.Name, "destination-pvc", "", "Destination PVC name")
	f.StringVar(&object.Spec.Name, "name", "", "Backup recovery point name")
	f.StringVar(&object.Spec.Path, "path", "", "PVC subdirectory")
	f.BoolVar(&object.Spec.CreatePVC, "create-pvc", false, "Create the destination PVC")
	f.StringVar(
		&object.Spec.DestinationStorageClass,
		"destination-storage-class",
		"",
		"StorageClass for a created PVC",
	)
	f.StringVar(
		&object.Spec.DestinationAccessMode,
		"destination-access-mode",
		"ReadWriteOnce",
		"Access mode for a created PVC",
	)
	f.StringVar(
		&object.Spec.DestinationCapacity,
		"destination-capacity",
		"",
		"Capacity for a created PVC; defaults to backup capacity",
	)
	f.StringVar(
		&object.Spec.TargetNode,
		"target-node",
		"",
		"Node for restore tool scheduling and PVC binding",
	)
	f.BoolVar(
		&object.Spec.AllowMounted,
		"allow-mounted",
		false,
		"Allow restore while the PVC has consumers",
	)
	f.BoolVar(
		&object.Spec.DeleteExtraneous,
		"delete-extraneous",
		false,
		"Delete destination files absent from the recovery point",
	)
	bindRepositoryFlags(command, flags, &object.Spec.RepositoryRef.Name, false)

	if !planOnly {
		bindDryRun(command, &dryRun)
	}

	return command
}

func (r *rootState) runRestoreObject(
	cmd *cobra.Command,
	object *v1alpha1.Restore,
	flags *s3RepositoryFlags,
	dryRun bool,
) error {
	if err := r.validateRestoreInput(cmd, object, flags, false); err != nil {
		return err
	}

	runtime, err := r.runtime()
	if err != nil {
		return reportRuntimeError(cmd, err)
	}

	if err := validateRepositoryFlags(
		cmd,
		flags,
		object.Spec.RepositoryRef.Name,
		false,
	); err != nil {
		return reportPreSessionError(cmd, err)
	}

	ctx, cancel := r.context(cmd.Context())
	defer cancel()

	// The session record ConfigMap lives in the session storage namespace,
	// next to every other session family; the command's -n addresses the
	// tenant namespace of the destination PVC and the repository resources,
	// never the storage location. The lifecycle verbs (status, resume,
	// abort, cleanup) resolve records the same way.
	storageNamespace := r.migrationRecordNamespace()

	store, err := cliWorkflowStore(
		runtime,
		storageNamespace,
		func() *v1alpha1.Restore { return &v1alpha1.Restore{} },
	)
	if err != nil {
		return err
	}

	executor := r.restoreExecutor(runtime, storageNamespace, store, backendConfigMap)

	repository, credentials, err := prepareInlineRepository(
		ctx,
		cmd,
		runtime.clients.Kubernetes,
		object.Namespace,
		object.Name,
		flags,
	)
	if err != nil {
		return err
	}

	object.Spec.RepositoryRef.Name = repository.Name

	if err := runtime.planner.PlanRestore(ctx, object, r.global.toolImage); err != nil {
		return reportPlanningError(cmd, err)
	}

	object.Status.WorkflowStatus = v1alpha1.WorkflowStatus{
		Phase:     domain.PhasePlanned,
		StartedAt: metav1.Now(),
		UpdatedAt: metav1.Now(),
	}

	connection, err := r.inlineRepositoryConnection(
		ctx,
		repository,
		object.Spec.Name,
		credentials,
	)
	if err != nil {
		return err
	}

	if err := executor.ValidateRepositoryPlan(ctx, object, connection); err != nil {
		return err
	}

	if dryRun {
		if err := runtime.printer.Print(object); err != nil {
			return err
		}

		return writeTransferDryRunGuidance(
			cmd.ErrOrStderr(),
			"restore",
			object.Namespace,
			object.Spec.DestinationPVC.Name,
			kubectlCommandPrefixForCommand(cmd),
		)
	}

	if err := r.confirm(ctx, cmd, object.Spec.Name); err != nil {
		return reportApprovalError(cmd, err)
	}

	if err := store.Create(ctx, object); err != nil {
		return reportSessionCreationError(cmd, storageNamespace, object.Name, err)
	}

	err = kube.WithWorkflowLease(
		ctx,
		store,
		cliWorkflowLocker(runtime),
		storageNamespace,
		object,
		false,
		func(ctx context.Context, _ kube.SessionLock) error {
			owner := v1alpha1.ObjectReference{
				Namespace: storageNamespace,
				Name:      object.Name,
				UID:       object.UID,
			}
			if err := persistInlineRepository(
				ctx,
				runtime,
				repository,
				owner,
				credentials,
			); err != nil {
				return err
			}

			before := object.Status.DeepCopy()
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
	if err == nil {
		err = executor.Run(ctx, object)
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
		storageNamespace,
	)
}

func (r *rootState) validateRestoreInput(
	cmd *cobra.Command,
	object *v1alpha1.Restore,
	flags *s3RepositoryFlags,
	requireReference bool,
) error {
	if object.Spec.DestinationPVC.Name == "" || object.Spec.Name == "" {
		return reportPreSessionError(
			cmd,
			domain.NewError(
				domain.ErrorValidation,
				"restore",
				"--destination-pvc and --name are required",
			),
		)
	}

	if object.Name == "" {
		id, err := domain.NewSessionID(time.Now())
		if err != nil {
			return err
		}

		object.Name = id
	}

	if err := domain.ValidateSessionID(object.Name); err != nil {
		return err
	}

	path, err := domain.NormalizeTransferPath(object.Spec.Path)
	if err != nil {
		return reportPreSessionError(cmd, err)
	}

	if path == domain.VolumeRootPath {
		path = ""
	}

	object.Spec.Path = path
	if err := validateRepositoryFlags(
		cmd,
		flags,
		object.Spec.RepositoryRef.Name,
		requireReference,
	); err != nil {
		return reportPreSessionError(cmd, err)
	}

	return nil
}
