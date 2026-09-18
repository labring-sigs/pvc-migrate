package backup

import (
	"context"
	"errors"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type RestoreCleanupOptions struct {
	// UnusedStoragePolicy overrides the recorded policy; empty uses the plan's.
	UnusedStoragePolicy string
	Finalize            bool
	DeleteSession       bool
}

func (r *RestoreExecutor) RequestResume(ctx context.Context, object *v1alpha1.Restore) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(ctx, r.store, r.locker, r.lockNamespace(object), object, false,
		func(ctx context.Context, _ kube.SessionLock) error {
			return r.reactivate(ctx, object, "restore resume requested")
		})
}

func (r *RestoreExecutor) ValidateAbort(ctx context.Context, object *v1alpha1.Restore) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseCompleted {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort restore",
			"completed restore cannot be aborted",
		)
	}

	return r.toolsStopped(ctx, object)
}

func (r *RestoreExecutor) Abort(ctx context.Context, object *v1alpha1.Restore) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(ctx, r.store, r.locker, r.lockNamespace(object), object, false,
		func(ctx context.Context, _ kube.SessionLock) error { return r.abort(ctx, object) })
}

func (r *RestoreExecutor) abort(ctx context.Context, object *v1alpha1.Restore) error {
	if err := r.ValidateAbort(ctx, object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	save := func(ctx context.Context) error { return r.save(ctx, object) }
	if object.Status.Plan == nil {
		before := object.Status.WorkflowStatus.DeepCopy()
		domain.RecordWorkflowTransition(
			&object.Status.WorkflowStatus,
			domain.PhaseAborted,
			"restore aborted before planning",
			time.Now(),
		)

		if err := save(ctx); err != nil {
			object.Status.WorkflowStatus = *before
			return err
		}

		return nil
	}

	if err := checkpointRepositoryPhase(
		ctx,
		&object.Status.WorkflowStatus,
		save,
		domain.PhaseAborting,
		"aborting restore",
	); err != nil {
		return err
	}

	if err := kube.CleanupSessionToolProbePods(
		ctx,
		r.client,
		object.Name,
		[]string{object.Namespace},
	); err != nil {
		return err
	}

	return checkpointRepositoryPhase(
		ctx,
		&object.Status.WorkflowStatus,
		save,
		domain.PhaseAborted,
		"restore aborted; destination data is retained",
	)
}

func (r *RestoreExecutor) ValidateCleanup(
	ctx context.Context,
	object *v1alpha1.Restore,
	options RestoreCleanupOptions,
) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	if options.DeleteSession && !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup restore",
			"deleting workflow metadata requires --finalize",
		)
	}

	if object.Status.Plan != nil && object.Status.Phase != domain.PhaseCompleted &&
		object.Status.Phase != domain.PhaseAborted {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup restore",
			"active restore must be aborted before cleanup",
		)
	}

	if err := r.toolsStopped(ctx, object); err != nil {
		return err
	}

	if options.Finalize {
		_, err := r.createdDestination(ctx, object)
		if err != nil {
			return err
		}

		if r.config.RepositoryResources != nil {
			key, owner := r.repositoryCleanupIdentity(object)
			return r.config.RepositoryResources.ValidateOwnedCleanup(ctx, key, owner)
		}
	}

	return nil
}

func (r *RestoreExecutor) Cleanup(
	ctx context.Context,
	object *v1alpha1.Restore,
	options RestoreCleanupOptions,
) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(
		ctx,
		r.store,
		r.locker,
		r.lockNamespace(object),
		object,
		false,
		func(ctx context.Context, lock kube.SessionLock) error { return r.cleanup(ctx, object, lock, options) },
	)
}

func (r *RestoreExecutor) cleanup(
	ctx context.Context,
	object *v1alpha1.Restore,
	lock kube.SessionLock,
	options RestoreCleanupOptions,
) error {
	if err := r.ValidateCleanup(ctx, object, options); err != nil {
		return err
	}

	if err := kube.CleanupSessionToolProbePods(
		ctx,
		r.client,
		object.Name,
		[]string{object.Namespace},
	); err != nil {
		return err
	}

	policy := v1alpha1.UnusedStoragePolicy("")
	if plan := object.Status.Plan; plan != nil {
		policy = plan.UnusedStoragePolicy
	}
	if options.UnusedStoragePolicy != "" {
		policy = v1alpha1.UnusedStoragePolicy(options.UnusedStoragePolicy)
	}
	if err := domain.ValidateUnusedStoragePolicy(policy); err != nil {
		return err
	}

	// A completed restore keeps its deliverable; only an aborted restore has
	// an unused created destination, and only then the policy may remove it.
	deleteUnused := domain.DeletesUnusedStorage(policy) &&
		object.Status.Phase == domain.PhaseAborted

	if options.Finalize {
		ref, err := r.createdDestination(ctx, object)
		if err != nil {
			return err
		}

		if ref != nil {
			// Pin an uncheckpointed creation before removing its ownership labels,
			// so a failed cleanup remains safe to retry.
			if object.Status.DestinationPVC == nil {
				object.Status.DestinationPVC = ref.DeepCopy()
				if err := r.save(ctx, object); err != nil {
					object.Status.DestinationPVC = nil
					return err
				}
			}

			if deleteUnused {
				if err := r.deleteCreatedDestination(ctx, *ref); err != nil {
					return err
				}
			} else if err := kube.FinalizePVC(
				ctx,
				r.client,
				*ref,
				object.Name,
				v1alpha1.PVCMetadata{},
			); err != nil {
				return err
			}
		}
	}

	if options.Finalize && r.config.RepositoryResources != nil {
		key, owner := r.repositoryCleanupIdentity(object)
		if err := r.config.RepositoryResources.CleanupOwned(ctx, key, owner); err != nil {
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

		return r.store.Delete(ctx, object)
	}

	return nil
}

// deleteCreatedDestination removes a workflow-created destination PVC after
// an aborted restore. Ownership is re-checked by the caller via
// createdDestination; the PV follows its StorageClass reclaim policy.
func (r *RestoreExecutor) deleteCreatedDestination(
	ctx context.Context,
	ref v1alpha1.ObjectReference,
) error {
	pvc, err := r.client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes, "cleanup restore", "read destination PVC", err,
		)
	}

	if pvc.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup restore",
			"destination PVC identity changed",
		)
	}

	uid, resourceVersion := pvc.UID, pvc.ResourceVersion
	if err := r.client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Delete(ctx, ref.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion},
		}); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes, "cleanup restore", "delete destination PVC", err,
		)
	}

	return nil
}

// createdDestination also recovers creation followed by a failed first save.
// Repository credentials are unnecessary for releasing local PVC ownership.
func (r *RestoreExecutor) createdDestination(
	ctx context.Context,
	object *v1alpha1.Restore,
) (*v1alpha1.ObjectReference, error) {
	plan := object.Status.Plan
	if plan == nil || !plan.CreatePVC {
		return nil, nil
	}

	pvc, err := r.client.CoreV1().
		PersistentVolumeClaims(object.Namespace).
		Get(ctx, plan.DestinationPVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	expected := plan.DestinationPVC.UID
	if object.Status.DestinationPVC != nil {
		expected = object.Status.DestinationPVC.UID
	}

	if pvc.UID == "" || (expected != "" && pvc.UID != expected) {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"cleanup restore",
			"destination PVC identity changed",
		)
	}

	for _, owner := range []string{pvc.Labels[kube.SessionKey], pvc.Annotations[kube.SessionKey]} {
		if owner != "" && owner != object.Name {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"cleanup restore",
				"destination PVC belongs to another workflow",
			)
		}
	}

	if expected == "" && pvc.Labels[kube.SessionKey] != object.Name {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"cleanup restore",
			"unrecorded destination PVC is not owned by this workflow",
		)
	}

	ref := kube.PVCReference(pvc)

	return &ref, nil
}

func (r *RestoreExecutor) FinalizeDeleted(ctx context.Context, object *v1alpha1.Restore) error {
	if object == nil || object.DeletionTimestamp == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize restore",
			"workflow deletion is required",
		)
	}

	if err := validateRestoreObject(object); err != nil {
		return err
	}

	return kube.WithWorkflowLease(ctx, r.store, r.locker, r.lockNamespace(object), object, true,
		func(ctx context.Context, lock kube.SessionLock) error {
			if object.Status.Phase != domain.PhaseCompleted &&
				object.Status.Phase != domain.PhaseAborted {
				if err := r.abort(ctx, object); err != nil {
					return err
				}
			}

			return r.cleanup(
				ctx,
				object,
				lock,
				RestoreCleanupOptions{Finalize: true, DeleteSession: true},
			)
		})
}

func (r *RestoreExecutor) toolsStopped(ctx context.Context, object *v1alpha1.Restore) error {
	if object.Status.Plan == nil {
		return nil
	}

	return kube.RequireTransferToolsStopped(
		ctx,
		r.client,
		object.Namespace,
		object.Status.Plan.DestinationPVC.Name,
	)
}

func (r *RestoreExecutor) repositoryCleanupIdentity(
	object *v1alpha1.Restore,
) (crclient.ObjectKey, v1alpha1.ObjectReference) {
	name := object.Spec.RepositoryRef.Name
	if object.Status.Plan != nil {
		name = object.Status.Plan.RepositoryRef.Name
	}

	key := crclient.ObjectKey{Namespace: object.Namespace, Name: name}

	return key, v1alpha1.ObjectReference{
		Namespace: r.lockNamespace(object),
		Name:      object.Name,
		UID:       object.UID,
	}
}
