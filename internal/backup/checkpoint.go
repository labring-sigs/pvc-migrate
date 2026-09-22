package backup

import (
	"context"
	"errors"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func checkpointRepositoryPhase(
	ctx context.Context,
	status *v1alpha1.WorkflowStatus,
	persist func(context.Context) error,
	phase v1alpha1.WorkflowPhase,
	message string,
) error {
	if err := validateRepositoryCheckpoint(ctx, status, persist); err != nil {
		return err
	}

	previous := status.DeepCopy()
	if err := domain.TransitionRepository(status, phase, message, time.Now()); err != nil {
		return err
	}

	status.Message = domain.BoundWorkflowMessage(message)
	if err := persist(ctx); err != nil {
		*status = *previous
		return err
	}

	return kube.LeaseFenceError(ctx)
}

func checkpointRepositoryFailure(
	ctx context.Context,
	status *v1alpha1.WorkflowStatus,
	persist func(context.Context) error,
	cause error,
) error {
	if err := validateRepositoryCheckpoint(ctx, status, persist); err != nil {
		return err
	}

	previous := status.DeepCopy()

	message := "repository transfer failed"
	if cause != nil {
		message = cause.Error()
	}

	if status.Phase != domain.PhaseFailed {
		if err := domain.TransitionRepository(
			status,
			domain.PhaseFailed,
			message,
			time.Now(),
		); err != nil {
			return err
		}
	}

	status.Message = domain.BoundWorkflowMessage(message)
	status.ErrorCategory = string(domain.CategoryOf(cause))

	err := persist(ctx)
	if err != nil {
		*status = *previous
	}

	return errors.Join(err, kube.LeaseFenceError(ctx))
}

func validateRepositoryCheckpoint(
	ctx context.Context,
	status *v1alpha1.WorkflowStatus,
	persist func(context.Context) error,
) error {
	if status == nil || persist == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"repository checkpoint",
			"status and checkpoint writer are required",
		)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return kube.LeaseFenceError(ctx)
}
