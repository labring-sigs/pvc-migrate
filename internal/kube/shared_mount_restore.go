package kube

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

type SharedMountRestorer interface {
	RestoreShared(ctx context.Context, id string, mount v1alpha1.SharedMountStatus) error
}

// RestoreSharedMounts compensates in reverse preparation order and checkpoints
// only the mounts that still need recovery. Failed writes preserve the previous
// checkpoint so idempotent compensation can be retried safely.
func RestoreSharedMounts(
	ctx context.Context,
	manager SharedMountRestorer,
	id string,
	mounts *[]v1alpha1.SharedMountStatus,
	save func(context.Context) error,
	logger *slog.Logger,
) error {
	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	if mounts == nil || len(*mounts) == 0 {
		return nil
	}

	if manager == nil || save == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore shared mounts",
			"shared mount manager and checkpoint writer are required",
		)
	}

	previous := slices.Clone(*mounts)
	remaining := make([]v1alpha1.SharedMountStatus, 0, len(previous))

	var result error
	for _, mount := range slices.Backward(previous) {
		if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
			return errors.Join(result, err)
		}

		if err := manager.RestoreShared(ctx, id, mount); err != nil {
			result = errors.Join(result, err)

			remaining = append(remaining, mount)
		} else if logger != nil {
			logger.Info("OpenEBS LVM shared mount restored",
				"session", id,
				"sourcePV", mount.SourcePV.Name,
				"previousShared", mount.PreviousShared,
				"previousSharedSet", mount.PreviousSharedSet,
			)
		}

		if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
			return errors.Join(result, err)
		}
	}

	slices.Reverse(remaining)

	if len(remaining) == 0 {
		remaining = nil
	}

	persistCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	*mounts = remaining
	if err := errors.Join(save(persistCtx), ctx.Err(), LeaseFenceError(ctx)); err != nil {
		*mounts = previous
		result = errors.Join(result, err)
	}

	return result
}
