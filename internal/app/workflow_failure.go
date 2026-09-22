package app

import (
	"context"
	"errors"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func recordWorkflowFailure(
	ctx context.Context,
	status *v1alpha1.WorkflowStatus,
	cause error,
	reason string,
	now time.Time,
	save func(context.Context) error,
	transition func(context.Context, v1alpha1.WorkflowPhase, string) error,
) error {
	if err := kube.LeaseFenceError(ctx); err != nil {
		return errors.Join(cause, err)
	}

	previous := status.DeepCopy()
	status.ErrorCategory = string(domain.CategoryOf(cause))
	status.FailureReason = reason

	checkpointCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	var err error
	if errors.Is(ctx.Err(), context.Canceled) {
		// Driver cancellation leaves the operation available for another worker.
		status.FailureReason = ""
		status.Message = domain.BoundWorkflowMessage(cause.Error())
		status.UpdatedAt = metav1.NewTime(now.UTC())
		err = persistCheckpoint(checkpointCtx, save)
	} else {
		err = transition(checkpointCtx, domain.PhaseFailed, cause.Error())
	}

	if err != nil {
		*status = *previous
		return errors.Join(cause, err)
	}

	return cause
}
