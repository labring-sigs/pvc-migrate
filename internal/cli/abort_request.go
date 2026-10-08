package cli

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// controllerAbortRequestTimeout bounds how long an abort request waits
	// for the controller to converge the workflow. The request outlives the
	// wait: it stays recorded and the controller aborts when its current
	// pass yields.
	controllerAbortRequestTimeout = 2 * time.Minute
	controllerAbortRequestPoll    = time.Second
)

// requestControllerAbort records the declarative abort request the controller
// executes and waits for the workflow to converge. A CR-mode abort runs the
// executor in this process, but an actively reconciling controller holds the
// session lease for whole transfer attempts; instead of failing against that
// lease, the request is delegated. The converged workflow is returned so the
// caller prints the final state.
func requestControllerAbort[T crclient.Object](
	ctx context.Context,
	cmd *cobra.Command,
	store kube.WorkflowStore[T],
	object T,
) (T, error) {
	requests, ok := store.(kube.AbortRequestStore[T])
	if !ok {
		return object, domain.NewError(
			domain.ErrorInternal,
			"abort",
			"workflow backend cannot carry an abort request",
		)
	}

	requested := metav1.Now().UTC().Format(time.RFC3339)
	if err := recordAbortRequest(ctx, requests, object, requested); err != nil {
		return object, err
	}

	cmd.Println(
		"session is busy; abort requested, waiting for the controller to stop the workflow...",
	)

	deadline := time.Now().Add(controllerAbortRequestTimeout)
	for {
		latest, err := store.Load(ctx, crclient.ObjectKeyFromObject(object))
		if err != nil {
			return object, err
		}

		switch {
		case abortRequestPhase(latest) == domain.PhaseAborted:
			cmd.Println("workflow aborted")
			return latest, nil
		case !kube.WorkflowAbortRequested(latest):
			return latest, domain.NewError(
				domain.ErrorPrecondition,
				"abort",
				fmt.Sprintf(
					"the controller rejected the abort request while the workflow is %s; check the workflow events for the reason",
					abortRequestPhase(latest),
				),
			)
		}

		if time.Now().After(deadline) {
			return latest, domain.NewError(
				domain.ErrorConflict,
				"abort",
				fmt.Sprintf(
					"abort request recorded at %s but the workflow is still %s; the request stays recorded and the controller aborts when its current pass yields",
					requested,
					abortRequestPhase(latest),
				),
			)
		}

		select {
		case <-ctx.Done():
			return latest, ctx.Err()
		case <-time.After(controllerAbortRequestPoll):
		}
	}
}

// recordAbortRequest writes the request annotation, retrying the optimistic
// concurrency races a concurrently reconciling controller produces.
func recordAbortRequest[T crclient.Object](
	ctx context.Context,
	store kube.AbortRequestStore[T],
	object T,
	value string,
) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := store.SetAbortRequest(ctx, object, value)
		if err == nil || !apierrors.IsConflict(err) || time.Now().After(deadline) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// abortRequestPhase reads the workflow phase across the CR kinds that carry
// abort requests.
func abortRequestPhase(object crclient.Object) v1alpha1.WorkflowPhase {
	switch typed := object.(type) {
	case *v1alpha1.Copy:
		return typed.Status.Phase
	case *v1alpha1.ClusterCopy:
		return typed.Status.Phase
	case *v1alpha1.Migration:
		return typed.Status.Phase
	case *v1alpha1.ClusterMigration:
		return typed.Status.Phase
	case *v1alpha1.PodMigration:
		return typed.Status.Phase
	default:
		return ""
	}
}
