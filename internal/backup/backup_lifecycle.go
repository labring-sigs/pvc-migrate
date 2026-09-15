package backup

import (
	"context"
	"errors"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type BackupCleanupOptions struct {
	Finalize      bool
	DeleteSession bool
}

func (b *BackupExecutor) RequestResume(ctx context.Context, object *v1alpha1.Backup) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(ctx, b.store, b.locker, b.lockNamespace(object), object, false,
		func(ctx context.Context, _ kube.SessionLock) error {
			if object.Status.Phase != domain.PhaseFailed {
				return nil
			}

			before := object.Status.WorkflowStatus.DeepCopy()
			if err := domain.ReactivateWorkflow(
				&object.Status.WorkflowStatus,
				"backup resume requested",
				time.Now(),
			); err != nil {
				return err
			}

			if err := b.save(ctx, object); err != nil {
				object.Status.WorkflowStatus = *before
				return err
			}

			return nil
		})
}

func (b *BackupExecutor) ValidateAbort(ctx context.Context, object *v1alpha1.Backup) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseCompleted {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort backup",
			"completed backup cannot be aborted",
		)
	}

	if err := b.toolsStopped(ctx, object); err != nil {
		return err
	}

	return b.validateSharedRestore(ctx, object.Name, object.Status.OpenEBSLVMSharedMounts)
}

func (b *BackupExecutor) Abort(ctx context.Context, object *v1alpha1.Backup) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(ctx, b.store, b.locker, b.lockNamespace(object), object, false,
		func(ctx context.Context, _ kube.SessionLock) error { return b.abort(ctx, object) })
}

func (b *BackupExecutor) abort(ctx context.Context, object *v1alpha1.Backup) error {
	if err := b.ValidateAbort(ctx, object); err != nil {
		return err
	}

	save := func(ctx context.Context) error { return b.save(ctx, object) }
	if object.Status.Phase != domain.PhaseAborted {
		if object.Status.Plan == nil {
			before := object.Status.WorkflowStatus.DeepCopy()
			domain.RecordWorkflowTransition(
				&object.Status.WorkflowStatus,
				domain.PhaseAborted,
				"backup aborted before planning",
				time.Now(),
			)

			if err := save(ctx); err != nil {
				object.Status.WorkflowStatus = *before
				return err
			}
		} else if err := checkpointRepositoryPhase(ctx, &object.Status.WorkflowStatus, save, domain.PhaseAborting, "aborting backup"); err != nil {
			return err
		}
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

	if object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	return checkpointRepositoryPhase(
		ctx,
		&object.Status.WorkflowStatus,
		save,
		domain.PhaseAborted,
		"backup aborted; any published recovery point is retained",
	)
}

func (b *BackupExecutor) ValidateCleanup(
	ctx context.Context,
	object *v1alpha1.Backup,
	options BackupCleanupOptions,
) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	if options.DeleteSession && !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup backup",
			"deleting workflow metadata requires --finalize",
		)
	}

	if object.Status.Plan != nil && object.Status.Phase != domain.PhaseCompleted &&
		object.Status.Phase != domain.PhaseAborted {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup backup",
			"active backup must be aborted before cleanup",
		)
	}

	if err := b.toolsStopped(ctx, object); err != nil {
		return err
	}

	if err := b.validateSharedRestore(
		ctx,
		object.Name,
		object.Status.OpenEBSLVMSharedMounts,
	); err != nil {
		return err
	}

	if options.Finalize && b.config.RepositoryResources != nil {
		key, owner := b.repositoryCleanupIdentity(object)
		return b.config.RepositoryResources.ValidateOwnedCleanup(ctx, key, owner)
	}

	return nil
}

func (b *BackupExecutor) Cleanup(
	ctx context.Context,
	object *v1alpha1.Backup,
	options BackupCleanupOptions,
) error {
	if err := validateBackupObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(
		ctx,
		b.store,
		b.locker,
		b.lockNamespace(object),
		object,
		false,
		func(ctx context.Context, lock kube.SessionLock) error { return b.cleanup(ctx, object, lock, options) },
	)
}

func (b *BackupExecutor) cleanup(
	ctx context.Context,
	object *v1alpha1.Backup,
	lock kube.SessionLock,
	options BackupCleanupOptions,
) error {
	if err := b.ValidateCleanup(ctx, object, options); err != nil {
		return err
	}

	save := func(ctx context.Context) error { return b.save(ctx, object) }
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

	if options.Finalize && b.config.RepositoryResources != nil {
		key, owner := b.repositoryCleanupIdentity(object)
		if err := b.config.RepositoryResources.CleanupOwned(ctx, key, owner); err != nil {
			return err
		}
	}

	if options.DeleteSession {
		if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
			return err
		}

		if err := lock.Delete(ctx); err != nil {
			return err
		}

		return b.store.Delete(ctx, object)
	}

	return nil
}

func (b *BackupExecutor) FinalizeDeleted(ctx context.Context, object *v1alpha1.Backup) error {
	if object == nil || object.DeletionTimestamp == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize backup",
			"workflow deletion is required",
		)
	}

	if err := validateBackupObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(ctx, b.store, b.locker, b.lockNamespace(object), object, true,
		func(ctx context.Context, lock kube.SessionLock) error {
			if object.Status.Phase != domain.PhaseCompleted &&
				object.Status.Phase != domain.PhaseAborted {
				if err := b.abort(ctx, object); err != nil {
					return err
				}
			}

			return b.cleanup(
				ctx,
				object,
				lock,
				BackupCleanupOptions{Finalize: true, DeleteSession: true},
			)
		})
}

func (b *BackupExecutor) toolsStopped(ctx context.Context, object *v1alpha1.Backup) error {
	if object.Status.Plan == nil {
		return nil
	}

	return kube.RequireTransferToolsStopped(
		ctx,
		b.client,
		object.Namespace,
		object.Status.Plan.SourcePVC.Name,
	)
}

func (b *BackupExecutor) repositoryCleanupIdentity(
	object *v1alpha1.Backup,
) (crclient.ObjectKey, v1alpha1.ObjectReference) {
	name := object.Spec.RepositoryRef.Name
	if object.Status.Plan != nil {
		name = object.Status.Plan.RepositoryRef.Name
	}

	key := crclient.ObjectKey{Namespace: object.Namespace, Name: name}

	return key, v1alpha1.ObjectReference{
		Namespace: b.lockNamespace(object),
		Name:      object.Name,
		UID:       object.UID,
	}
}
