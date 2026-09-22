package backup

import (
	"context"
	"errors"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

type RestoreExecutorConfig struct {
	RepositoryResources RepositoryResources
	Tools               ToolRuntime
	Repository          S3RepositoryResolver
	TrustedToolImage    string
}

// RestoreExecutor owns destination creation, identity checkpoints and restore
// lifecycle. Both entry points persist this same concrete CRD object.
type RestoreExecutor struct {
	client           kubernetes.Interface
	store            kube.WorkflowStore[*v1alpha1.Restore]
	locker           kube.SessionLocker
	storageNamespace string
	config           RestoreExecutorConfig
}

func NewRestoreExecutor(
	client kubernetes.Interface,
	store kube.WorkflowStore[*v1alpha1.Restore],
	locker kube.SessionLocker,
	storageNamespace string,
	config RestoreExecutorConfig,
) *RestoreExecutor {
	return &RestoreExecutor{
		client:           client,
		store:            store,
		locker:           locker,
		storageNamespace: storageNamespace,
		config:           config,
	}
}

func (r *RestoreExecutor) lockNamespace(object *v1alpha1.Restore) string {
	if r.storageNamespace != "" {
		return r.storageNamespace
	}
	return object.Namespace
}

func (r *RestoreExecutor) Run(ctx context.Context, object *v1alpha1.Restore) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(ctx, r.store, r.locker, r.lockNamespace(object), object, false,
		func(ctx context.Context, _ kube.SessionLock) error { return r.run(ctx, object) })
}

func (r *RestoreExecutor) run(ctx context.Context, object *v1alpha1.Restore) (result error) {
	if object.Status.Phase == domain.PhaseCompleted || object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if object.Status.Phase == domain.PhaseAborting ||
		(object.Status.Phase == domain.PhaseFailed && object.Status.ResumeFrom == domain.PhaseAborting) {
		return r.abort(ctx, object)
	}

	if object.Status.Plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"restore",
			"restore requires execution planning",
		)
	}

	save := func(ctx context.Context) error { return r.save(ctx, object) }
	defer func() {
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

	if object.Status.Phase == domain.PhaseWarmCopied ||
		(object.Status.Phase == domain.PhaseFailed && object.Status.ResumeFrom == domain.PhaseWarmCopied) {
		return r.complete(ctx, object)
	}

	repository, binding, err := resolveTransferRepository(
		ctx,
		r.config.Repository,
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

	manifest, err := readRestoreManifest(ctx, repository, object.Status.Plan.Path)
	if err != nil {
		return err
	}

	plan := r.executionPlan(object)

	info, err := r.validateDestination(
		ctx,
		object.Namespace,
		object.Name,
		plan,
		object.Status.DestinationPV,
		repository.Config(),
		*manifest,
	)
	if err != nil {
		return err
	}

	if err := r.reactivate(ctx, object, "restore resumed"); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhasePlanned {
		if err := checkpointRepositoryPhase(
			ctx,
			&object.Status.WorkflowStatus,
			save,
			domain.PhaseWarmCopying,
			"restore started",
		); err != nil {
			return err
		}
	}

	if err := kube.CleanupSessionToolProbePods(
		ctx,
		r.client,
		object.Name,
		[]string{object.Namespace},
	); err != nil {
		return err
	}

	if info == nil || info.PV == nil {
		if err := createRestorePVC(
			ctx,
			r.client,
			object.Namespace,
			object.Name,
			plan,
			repository.Config(),
			*manifest,
			func(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
				return r.checkpointPVC(ctx, object, pvc)
			},
			func(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
				return r.probeDestination(ctx, object.Name, plan, pvc)
			},
		); err != nil {
			return err
		}

		plan = r.executionPlan(object)

		info, err = r.validateDestination(
			ctx,
			object.Namespace,
			object.Name,
			plan,
			object.Status.DestinationPV,
			repository.Config(),
			*manifest,
		)
		if err != nil {
			return err
		}

		if info == nil || info.PV == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"restore",
				"destination PVC is not bound after creation",
			)
		}
	}

	if err := r.checkpointBoundDestination(ctx, object, info); err != nil {
		return err
	}

	plan = r.executionPlan(object)

	transfer := restoreTransfer{client: r.client, store: repository, tools: r.config.Tools}
	if err := transfer.run(
		ctx,
		object.Namespace,
		object.Name,
		plan,
		info.PV.UID,
		*manifest,
	); err != nil {
		return err
	}

	if err := checkpointRepositoryPhase(
		ctx,
		&object.Status.WorkflowStatus,
		save,
		domain.PhaseWarmCopied,
		"restore transfer completed",
	); err != nil {
		return err
	}

	return r.complete(ctx, object)
}

func (r *RestoreExecutor) executionPlan(object *v1alpha1.Restore) v1alpha1.RestorePlan {
	plan := *object.Status.Plan.DeepCopy()
	if r.config.TrustedToolImage != "" {
		plan.ToolImage = r.config.TrustedToolImage
	}

	if object.Status.DestinationPVC != nil {
		plan.DestinationPVC.UID = object.Status.DestinationPVC.UID
	}

	return plan
}

func (r *RestoreExecutor) complete(ctx context.Context, object *v1alpha1.Restore) error {
	if err := r.validateTransferredDestination(ctx, object); err != nil {
		return err
	}

	if err := r.reactivate(ctx, object, "restore completion resumed"); err != nil {
		return err
	}

	return checkpointRepositoryPhase(
		ctx,
		&object.Status.WorkflowStatus,
		func(ctx context.Context) error { return r.save(ctx, object) },
		domain.PhaseCompleted,
		"restore completed",
	)
}

func (r *RestoreExecutor) reactivate(
	ctx context.Context,
	object *v1alpha1.Restore,
	message string,
) error {
	if object.Status.Phase != domain.PhaseFailed {
		return nil
	}

	before := object.Status.WorkflowStatus.DeepCopy()
	if err := domain.ReactivateWorkflow(
		&object.Status.WorkflowStatus,
		message,
		time.Now(),
	); err != nil {
		return err
	}

	if err := r.save(ctx, object); err != nil {
		object.Status.WorkflowStatus = *before
		return err
	}

	return nil
}

func (r *RestoreExecutor) checkpointPVC(
	ctx context.Context,
	object *v1alpha1.Restore,
	pvc *corev1.PersistentVolumeClaim,
) error {
	if pvc.UID == "" || pvc.Name != object.Status.Plan.DestinationPVC.Name ||
		pvc.Namespace != object.Namespace {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"created PVC identity differs from the execution plan",
		)
	}

	if expected := object.Status.DestinationPVC; expected != nil && expected.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"destination PVC changed after its checkpoint",
		)
	}

	if expected := object.Status.Plan.DestinationPVC.UID; expected != "" && expected != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"destination PVC differs from its planned identity",
		)
	}

	before := object.Status.DestinationPVC
	reference := kube.PVCReference(pvc)

	object.Status.DestinationPVC = &reference
	if err := r.save(ctx, object); err != nil {
		object.Status.DestinationPVC = before
		return err
	}

	return nil
}

func (r *RestoreExecutor) checkpointBoundDestination(
	ctx context.Context,
	object *v1alpha1.Restore,
	info *PVCInfo,
) error {
	before := object.Status.DeepCopy()
	if object.Status.DestinationPVC == nil {
		object.Status.DestinationPVC = &v1alpha1.ObjectReference{}
	}

	if object.Status.DestinationPV == nil {
		object.Status.DestinationPV = &v1alpha1.ObjectReference{}
	}

	err := checkpointRestoreDestinationIdentity(
		ctx,
		r.client,
		kube.PVCReference(info.PVC),
		info.PV.UID,
		object.Status.DestinationPVC,
		object.Status.DestinationPV,
		func(ctx context.Context) error { return r.save(ctx, object) },
	)
	if err != nil {
		object.Status = *before
	}

	return err
}

func (r *RestoreExecutor) probeDestination(
	ctx context.Context,
	id string,
	plan v1alpha1.RestorePlan,
	pvc *corev1.PersistentVolumeClaim,
) error {
	return probeCreatedRestorePVC(ctx, r.config.Tools.ToolImageProber, kube.ToolImageProbeOptions{
		OperationID: id,
		Image:       plan.ToolImage,
		Targets: []kube.ToolProbeTarget{
			{
				Namespace:        pvc.Namespace,
				PVCName:          pvc.Name,
				NodeName:         plan.TargetNode,
				RequiredPath:     plan.Path,
				CreatePath:       plan.Path != "",
				WritablePVCMount: true,
				Components:       []string{kube.ToolComponentRclone},
			},
		},
		Timeout: toolHelmTimeout(
			r.config.Tools.HelmTimeout,
		),
		Writer: r.config.Tools.Writer,
		Logger: r.config.Tools.Logger,
	})
}

func (r *RestoreExecutor) save(ctx context.Context, object *v1alpha1.Restore) error {
	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}
	return r.store.Save(ctx, object)
}

func readRestoreManifest(
	ctx context.Context,
	store S3RepositoryStore,
	path string,
) (*objectstore.Manifest, error) {
	manifest, err := store.Manifest(ctx)
	if err != nil {
		return nil, err
	}

	if manifest == nil {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"restore",
			"S3 completion manifest is missing; the backup is not a published recovery point",
		)
	}

	if manifest.Path != path {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"restore",
			"restore path differs from the published backup",
		)
	}

	if err := store.VerifyInventory(ctx, *manifest); err != nil {
		return nil, err
	}

	snapshot := *manifest

	return &snapshot, nil
}
