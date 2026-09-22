package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// withStoredWorkflowLock fences one concrete object's persistence identity.
// Operation-specific validation and resource changes stay in the callback.
func withStoredWorkflowLock[T crclient.Object](
	ctx context.Context,
	store kube.WorkflowStore[T],
	locker kube.SessionLocker,
	namespace string,
	object T,
	run func(context.Context) error,
) error {
	return withStoredWorkflowLease(ctx, store, locker, namespace, object,
		func(ctx context.Context) error {
			if err := kube.RequireWorkflowHandoffComplete(object); err != nil {
				return err
			}

			return run(ctx)
		})
}

// withStoredWorkflowLease supplies fencing for both normal execution and
// recovery of a pending handoff. Callers own the operation-specific admission.
func withStoredWorkflowLease[T crclient.Object](
	ctx context.Context, store kube.WorkflowStore[T], locker kube.SessionLocker,
	namespace string, object T, run func(context.Context) error,
) error {
	return kube.WithWorkflowLease(
		ctx,
		store,
		locker,
		namespace,
		object,
		ctx.Value(workflowDeletionContextKey{}) == true,
		func(bound context.Context, lock kube.SessionLock) error {
			bound = withHeldSessionLock(
				bound,
				heldSessionLock{lock: lock, namespace: namespace, id: object.GetName()},
			)

			return run(bound)
		},
	)
}
