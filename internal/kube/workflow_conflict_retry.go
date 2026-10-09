package kube

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// StaleWorkflowWrite reports whether a workflow pass lost an optimistic
// concurrency race: the raw API conflict, or the store's stale-load rejection.
// Both mean "reload and recompute", never "the request was wrong".
func StaleWorkflowWrite(err error) bool {
	return apierrors.IsConflict(err) || errors.Is(err, ErrWorkflowStaleLoad)
}

// RetryStaleWorkflowWrite re-runs a convergent workflow pass when the workflow
// changed after the caller loaded it. Cleanup passes recompute from live
// cluster state — the deletion path already re-runs them across reconciles —
// so a reload converges instead of surfacing a raw conflict to the operator.
// The pass must already hold the session lease; identity is re-verified so a
// name reused after deletion never receives the cleanup.
func RetryStaleWorkflowWrite[T crclient.Object](
	ctx context.Context,
	store WorkflowStore[T],
	object T,
	run func(context.Context, T) error,
) error {
	const attempts = 3

	for attempt := 1; ; attempt++ {
		err := run(ctx, object)
		if err == nil || attempt >= attempts || !StaleWorkflowWrite(err) {
			return err
		}

		fresh, loadErr := store.Load(ctx, crclient.ObjectKeyFromObject(object))
		if loadErr != nil {
			return errors.Join(err, loadErr)
		}

		if fresh.GetUID() != object.GetUID() {
			return err
		}

		object = fresh
	}
}
