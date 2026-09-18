package backup

import (
	"context"
	"errors"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"k8s.io/client-go/kubernetes"
)

type BackupExecutorConfig struct {
	RepositoryResources RepositoryResources
	Tools               ToolRuntime
	Repository          S3RepositoryResolver
	SharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
	TrustedToolImage    string
}

// BackupExecutor owns Backup lifecycle and checkpoints the same CRD object in
// either storage backend. The data plane receives only the resolved BackupPlan.
type BackupExecutor struct {
	client           kubernetes.Interface
	store            kube.WorkflowStore[*v1alpha1.Backup]
	locker           kube.SessionLocker
	storageNamespace string
	config           BackupExecutorConfig
}

func NewBackupExecutor(
	client kubernetes.Interface,
	store kube.WorkflowStore[*v1alpha1.Backup],
	locker kube.SessionLocker,
	storageNamespace string,
	config BackupExecutorConfig,
) *BackupExecutor {
	return &BackupExecutor{
		client:           client,
		store:            store,
		locker:           locker,
		storageNamespace: storageNamespace,
		config:           config,
	}
}

func (b *BackupExecutor) lockNamespace(object *v1alpha1.Backup) string {
	if b.storageNamespace != "" {
		return b.storageNamespace
	}
	return object.Namespace
}

func (b *BackupExecutor) Run(ctx context.Context, object *v1alpha1.Backup) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(ctx, b.store, b.locker, b.lockNamespace(object), object, false,
		func(ctx context.Context, _ kube.SessionLock) error { return b.run(ctx, object) })
}

func (b *BackupExecutor) run(ctx context.Context, object *v1alpha1.Backup) (result error) {
	if object.Status.Phase == domain.PhaseCompleted || object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if object.Status.Phase == domain.PhaseAborting ||
		(object.Status.Phase == domain.PhaseFailed && object.Status.ResumeFrom == domain.PhaseAborting) {
		return b.abort(ctx, object)
	}

	if object.Status.Plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup",
			"backup requires execution planning",
		)
	}

	save := func(ctx context.Context) error { return b.save(ctx, object) }
	defer func() {
		if kube.LeaseFenceError(ctx) != nil {
			result = errors.Join(result, kube.LeaseFenceError(ctx))
			return
		}

		cleanupErr := runWithPreservedCleanupTimeout(
			ctx,
			lockReleaseTimeout,
			func(cleanupCtx context.Context) error {
				if err := b.toolsStopped(cleanupCtx, object); err != nil {
					return err
				}

				return kube.RestoreSharedMounts(
					cleanupCtx,
					b.config.SharedVolumeManager,
					object.Name,
					&object.Status.OpenEBSLVMSharedMounts,
					save,
					b.config.Tools.Logger,
				)
			},
		)

		result = errors.Join(result, cleanupErr)
		if result == nil || errors.Is(ctx.Err(), context.Canceled) ||
			kube.LeaseFenceError(ctx) != nil {
			return
		}

		failureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lockReleaseTimeout)
		defer cancel()

		result = errors.Join(
			result,
			checkpointRepositoryFailure(failureCtx, &object.Status.WorkflowStatus, save, result),
		)
	}()

	repository, binding, err := resolveTransferRepository(
		ctx, b.config.Repository,
		object.Namespace,
		object.Status.Plan.RepositoryRef.Name,
		object.Status.Plan.Name,
	)
	if err != nil {
		return err
	}

	if err := PinRepository(ctx, binding, &object.Status.Repository, save); err != nil {
		return err
	}

	manifest, err := repository.Manifest(ctx)
	if err != nil {
		return err
	}

	if manifest != nil {
		if manifest.SessionID != object.Name {
			// A different backup already published this recovery-point name.
			// Refuse clearly at admission instead of surfacing a confusing
			// resume conflict mid-run.
			return domain.NewError(
				domain.ErrorValidation,
				"backup",
				"recovery point "+object.Status.Plan.Name+
					" already exists in this repository (published by backup "+
					manifest.SessionID+"); choose a different recovery point name",
			)
		}

		if err := validatePublishedBackup(
			ctx,
			repository,
			object.Name,
			object.Namespace,
			*object.Status.Plan,
			manifest,
		); err != nil {
			return err
		}

		return b.complete(ctx, object)
	}

	plan := *object.Status.Plan.DeepCopy()
	if b.config.TrustedToolImage != "" {
		plan.ToolImage = b.config.TrustedToolImage
	}

	info, err := b.validateSource(
		ctx,
		object.Namespace,
		object.Name,
		plan,
		object.Status.OpenEBSLVMSharedMounts,
	)
	if err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseFailed {
		previous := object.Status.WorkflowStatus.DeepCopy()
		if err := domain.ReactivateWorkflow(
			&object.Status.WorkflowStatus,
			"backup resumed",
			time.Now(),
		); err != nil {
			return err
		}

		if err := save(ctx); err != nil {
			object.Status.WorkflowStatus = *previous
			return err
		}
	}

	if object.Status.Phase == domain.PhasePlanned {
		if err := checkpointRepositoryPhase(
			ctx,
			&object.Status.WorkflowStatus,
			save,
			domain.PhaseWarmCopying,
			"backup preparing source PVC",
		); err != nil {
			return err
		}
	}

	if err := prepareBackupSharedMounts(
		ctx,
		b.config.SharedVolumeManager,
		object.Name,
		plan.OpenEBSLVMEnableShared,
		&object.Status.OpenEBSLVMSharedMounts,
		save,
		info,
	); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseWarmCopying {
		if err := checkpointRepositoryPhase(
			ctx,
			&object.Status.WorkflowStatus,
			save,
			domain.PhaseWarmCopied,
			"backup source PVC prepared",
		); err != nil {
			return err
		}
	}

	transfer := backupTransfer{
		client: b.client,
		locker: b.locker,
		store:  repository,
		tools:  b.config.Tools,
	}

	writable := info.PV.Spec.CSI != nil && info.PV.Spec.CSI.Driver == kube.OpenEBSLVMCSIDriver &&
		len(info.Consumers) > 0
	if err := transfer.run(
		ctx,
		object.Namespace,
		object.Name,
		b.lockNamespace(object),
		object.Name,
		plan,
		writable,
	); err != nil {
		return err
	}

	return b.complete(ctx, object)
}

func (b *BackupExecutor) complete(ctx context.Context, object *v1alpha1.Backup) error {
	save := func(ctx context.Context) error { return b.save(ctx, object) }
	if err := b.toolsStopped(ctx, object); err != nil {
		return err
	}

	if err := kube.CleanupSessionToolProbePods(
		ctx,
		b.client,
		object.Name,
		[]string{object.Namespace},
	); err != nil {
		return err
	}

	if err := kube.RestoreSharedMounts(
		ctx,
		b.config.SharedVolumeManager,
		object.Name,
		&object.Status.OpenEBSLVMSharedMounts,
		save,
		b.config.Tools.Logger,
	); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseFailed {
		previous := object.Status.WorkflowStatus.DeepCopy()
		if err := domain.ReactivateWorkflow(
			&object.Status.WorkflowStatus,
			"published backup recovered",
			time.Now(),
		); err != nil {
			return err
		}

		if err := save(ctx); err != nil {
			object.Status.WorkflowStatus = *previous
			return err
		}
	}

	if object.Status.Phase == domain.PhasePlanned {
		if err := checkpointRepositoryPhase(
			ctx,
			&object.Status.WorkflowStatus,
			save,
			domain.PhaseWarmCopying,
			"published backup recovered",
		); err != nil {
			return err
		}
	}

	if object.Status.Phase == domain.PhaseWarmCopying {
		if err := checkpointRepositoryPhase(
			ctx,
			&object.Status.WorkflowStatus,
			save,
			domain.PhaseWarmCopied,
			"published backup verified",
		); err != nil {
			return err
		}
	}

	return checkpointRepositoryPhase(
		ctx,
		&object.Status.WorkflowStatus,
		save,
		domain.PhaseCompleted,
		"backup completed",
	)
}

func (b *BackupExecutor) Validate(ctx context.Context, object *v1alpha1.Backup) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil || object.Status.Phase == domain.PhaseCompleted ||
		object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if object.Status.Phase == domain.PhaseAborting ||
		(object.Status.Phase == domain.PhaseFailed && object.Status.ResumeFrom == domain.PhaseAborting) {
		return b.ValidateAbort(ctx, object)
	}

	_, err := b.validatePlan(ctx, object)

	return err
}

// Prepare validates a newly resolved plan and captures its repository identity.
// The caller persists the plan and binding together as one status transaction.
func (b *BackupExecutor) Prepare(ctx context.Context, object *v1alpha1.Backup) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil || object.Status.Phase != domain.PhasePlanned ||
		len(object.Status.OpenEBSLVMSharedMounts) != 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"plan backup",
			"repository binding requires a newly planned backup",
		)
	}

	binding, err := b.validatePlan(ctx, object)
	if err != nil {
		return err
	}

	object.Status.Repository = binding.DeepCopy()

	return nil
}

func (b *BackupExecutor) validatePlan(
	ctx context.Context,
	object *v1alpha1.Backup,
) (*v1alpha1.BackupRepositoryBindingStatus, error) {
	repository, binding, err := resolveTransferRepository(
		ctx, b.config.Repository,
		object.Namespace,
		object.Status.Plan.RepositoryRef.Name,
		object.Status.Plan.Name,
	)
	if err != nil {
		return nil, err
	}

	if err := validateRepositoryMatch(binding, object.Status.Repository); err != nil {
		return nil, err
	}

	return binding, b.ValidateRepositoryPlan(ctx, object, repository)
}

// ValidateRepositoryPlan performs a read-only preview with an already resolved
// connection, before a session-owned repository has been persisted.
func (b *BackupExecutor) ValidateRepositoryPlan(
	ctx context.Context,
	object *v1alpha1.Backup,
	repository S3RepositoryStore,
) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return domain.NewError(domain.ErrorPrecondition, "backup", "execution plan is required")
	}

	manifest, err := repository.Manifest(ctx)
	if err != nil {
		return err
	}

	if manifest != nil {
		if manifest.SessionID != object.Name {
			return domain.NewError(
				domain.ErrorValidation,
				"backup",
				"recovery point "+object.Status.Plan.Name+
					" already exists in this repository (published by backup "+
					manifest.SessionID+"); choose a different recovery point name",
			)
		}

		err = validatePublishedBackup(
			ctx,
			repository,
			object.Name,
			object.Namespace,
			*object.Status.Plan,
			manifest,
		)

		return err
	}

	_, err = b.validateSource(
		ctx,
		object.Namespace,
		object.Name,
		*object.Status.Plan,
		object.Status.OpenEBSLVMSharedMounts,
	)

	return err
}

// ValidateSourcePlan checks local resources when the controller owns repository
// credentials and the submitting user can only inspect repository routing.
func (b *BackupExecutor) ValidateSourcePlan(ctx context.Context, object *v1alpha1.Backup) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return domain.NewError(domain.ErrorPrecondition, "backup", "execution plan is required")
	}

	_, err := b.validateSource(
		ctx,
		object.Namespace,
		object.Name,
		*object.Status.Plan,
		object.Status.OpenEBSLVMSharedMounts,
	)

	return err
}

func (b *BackupExecutor) validateSource(
	ctx context.Context,
	namespace, id string,
	plan v1alpha1.BackupPlan,
	mounts []v1alpha1.SharedMountStatus,
) (*PVCInfo, error) {
	if _, err := normalizeObjectTransferPath(plan.Path); err != nil {
		return nil, err
	}

	if err := objectstore.ValidatePath(plan.Path); err != nil {
		return nil, err
	}

	image := plan.ToolImage
	if b.config.TrustedToolImage != "" {
		image = b.config.TrustedToolImage
	}

	if _, err := kube.NormalizeToolImage(image); err != nil {
		return nil, err
	}

	info, err := inspectBackupPVC(ctx, b.client, namespace, plan.SourcePVC.Name, plan.Online)
	if err != nil {
		return nil, err
	}

	if info.PVC.UID != plan.SourcePVC.UID || info.PV.UID != plan.SourcePV.UID ||
		info.PV.Name != plan.SourcePV.Name {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"backup",
			"source PVC or PV identity changed",
		)
	}

	if err := validateBackupSharedSource(
		ctx,
		b.config.SharedVolumeManager,
		id,
		plan.OpenEBSLVMEnableShared,
		mounts,
		true,
		info,
	); err != nil {
		return nil, err
	}

	if err := checkBackupQuota(
		ctx,
		b.client,
		namespace,
		"",
		domain.ResourceEstimate{},
	); err != nil {
		return nil, err
	}

	return info, nil
}

func (b *BackupExecutor) save(ctx context.Context, object *v1alpha1.Backup) error {
	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	return b.store.Save(ctx, object)
}
