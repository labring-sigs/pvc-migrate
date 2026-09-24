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

func (r *rootState) newBackupCommand() *cobra.Command {
	command := r.backupSubmissionCommand(false)
	command.AddCommand(
		r.newBackupPlanCommand(),
		r.newBackupStatusCommand(sourceSession),
		r.newBackupResumeCommand(sourceSession),
		r.newBackupAbortCommand(sourceSession),
		r.newBackupCleanupCommand(sourceSession),
	)

	return command
}

func (r *rootState) newBackupPlanCommand() *cobra.Command {
	return r.backupSubmissionCommand(true)
}

// backupSubmissionCommand builds the session backup command and its plan-only
// preview. Controller submission lives in the cr backup create command.
func (r *rootState) backupSubmissionCommand(planOnly bool) *cobra.Command {
	object := &v1alpha1.Backup{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Backup"},
	}
	flags := &s3RepositoryFlags{}
	dryRun := planOnly

	command := &cobra.Command{
		Use:   "backup",
		Short: "Back up PVC data to an S3 repository",
		Args:  cobra.NoArgs,
	}
	if planOnly {
		command.Use, command.Short = "plan", "Validate a backup without mutations"
	}

	command.RunE = func(cmd *cobra.Command, _ []string) error {
		return r.runBackupObject(cmd, object.DeepCopy(), flags, dryRun)
	}
	f := command.Flags()
	f.StringVar(&object.Name, "id", "", "Workflow ID; generated when omitted")
	f.StringVarP(
		&object.Namespace,
		"namespace",
		"n",
		"default",
		"Source PVC and repository namespace",
	)
	f.StringVar(&object.Spec.SourcePVC.Name, "source-pvc", "", "Source PVC name")
	f.StringVar(&object.Spec.Name, "name", "", "Backup recovery point name")
	f.StringVar(&object.Spec.Path, "path", "", "PVC subdirectory")
	f.BoolVar(
		&object.Spec.Online,
		"online",
		false,
		"Copy from an active source without pausing consumers",
	)
	f.BoolVar(
		&object.Spec.OpenEBSLVMEnableShared,
		"openebs-lvm-enable-shared",
		false,
		"Temporarily enable shared mounts for online OpenEBS LVM backup",
	)
	bindRepositoryFlags(command, flags, &object.Spec.RepositoryRef.Name, false)

	if !planOnly {
		bindDryRun(command, &dryRun)
	}

	return command
}

func (r *rootState) runBackupObject(
	cmd *cobra.Command,
	object *v1alpha1.Backup,
	flags *s3RepositoryFlags,
	dryRun bool,
) error {
	if err := r.validateBackupInput(cmd, object, flags, false); err != nil {
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

	storageNamespace := object.Namespace

	store, err := cliWorkflowStore(
		runtime,
		storageNamespace,
		func() *v1alpha1.Backup { return &v1alpha1.Backup{} },
	)
	if err != nil {
		return err
	}

	executor := r.backupExecutor(runtime, storageNamespace, store, backendConfigMap)

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

	if err := runtime.planner.PlanBackup(ctx, object, r.global.toolImage); err != nil {
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
			"backup",
			object.Namespace,
			object.Spec.SourcePVC.Name,
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
		storageNamespace,
	)
}

func (r *rootState) validateBackupInput(
	cmd *cobra.Command,
	object *v1alpha1.Backup,
	flags *s3RepositoryFlags,
	requireReference bool,
) error {
	if object.Spec.SourcePVC.Name == "" || object.Spec.Name == "" {
		return reportPreSessionError(
			cmd,
			domain.NewError(
				domain.ErrorValidation,
				"backup",
				"--source-pvc and --name are required",
			),
		)
	}

	if object.Spec.OpenEBSLVMEnableShared && !object.Spec.Online {
		return reportPreSessionError(
			cmd,
			domain.NewError(
				domain.ErrorValidation,
				"backup",
				"--openebs-lvm-enable-shared requires --online",
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
