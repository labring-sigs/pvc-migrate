package controller

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/events"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// abortRequestTransient reports whether an abort execution lost a race it
// should retry: session-lease contention or optimistic-concurrency conflicts
// clear on their own, and Kubernetes failures may clear with the API server.
// The request stays recorded and the next reconcile runs the abort again.
func abortRequestTransient(err error) bool {
	return kube.IsSessionLockContention(err) ||
		apierrors.IsConflict(err) ||
		domain.CategoryOf(err) == domain.ErrorKubernetes
}

// finishAbortRequest consumes the abort request after the executor handled it
// — successfully or with a durable rejection — so a workflow cannot re-abort
// forever. A rejection is surfaced as an event instead of a reconcile error:
// the request itself was invalid, not the controller's state.
func finishAbortRequest[T crclient.Object](
	ctx context.Context,
	store *kube.CRDWorkflowStore[T],
	recorder events.EventRecorder,
	object T,
	cause error,
) (reconcile.Result, error) {
	if err := store.SetAbortRequest(ctx, object, ""); err != nil {
		return workflowReconcileResult(err)
	}

	if cause != nil && recorder != nil {
		recorder.Eventf(object, nil, "Warning", "AbortRejected", "Abort", "%s", cause.Error())
	}

	return reconcile.Result{}, nil
}
