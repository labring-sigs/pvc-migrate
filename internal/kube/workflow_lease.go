package kube

import (
	"context"
	"errors"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// WithWorkflowLease fences a concrete workflow against its stored UID and
// resourceVersion. The caller owns operation admission and resource changes.
func WithWorkflowLease[T crclient.Object](
	ctx context.Context,
	store WorkflowStore[T],
	locker SessionLocker,
	namespace string,
	object T,
	allowDeleting bool,
	run func(context.Context, SessionLock) error,
) error {
	if object.GetUID() == "" || object.GetResourceVersion() == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"workflow",
			"persisted workflow UID and resourceVersion are required",
		)
	}

	if object.GetDeletionTimestamp() != nil && !allowDeleting {
		return domain.NewError(
			domain.ErrorPrecondition,
			"workflow",
			"workflow is being deleted; controller recovery owns execution",
		)
	}

	lock, err := AcquireRequiredSessionLock(ctx, locker, namespace, object.GetName())
	if err != nil {
		// Deletion convergence must survive its own namespace terminating:
		// lease creation is forbidden there, and no concurrent writer exists
		// for a deleting workflow. Release the finalizer directly.
		if allowDeleting && object.GetDeletionTimestamp() != nil &&
			isSessionNamespaceTerminating(err) {
			return store.Delete(ctx, object)
		}

		return err
	}

	bound, cancel := lock.Bind(ctx)
	defer cancel()

	bound = WithLeaseFence(bound, lock)

	latest, operationErr := store.Load(bound, crclient.ObjectKeyFromObject(object))
	if apierrors.IsNotFound(operationErr) && allowDeleting {
		operationErr = lock.Delete(bound)
	} else if operationErr == nil {
		switch {
		case latest.GetUID() != object.GetUID() || latest.GetResourceVersion() != object.GetResourceVersion():
			operationErr = domain.NewError(
				domain.ErrorConflict,
				"workflow",
				"workflow changed while acquiring its lock; reload before retrying",
			)
		case latest.GetDeletionTimestamp() != nil && !allowDeleting:
			operationErr = domain.NewError(
				domain.ErrorPrecondition,
				"workflow",
				"workflow is being deleted",
			)
		default:
			operationErr = errors.Join(bound.Err(), LeaseFenceError(bound))
			if operationErr == nil {
				operationErr = run(bound, lock)
			}
		}
	}

	operationErr = errors.Join(operationErr, LeaseFenceError(bound))

	releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer releaseCancel()

	return errors.Join(operationErr, lock.Release(releaseCtx))
}
