package app

import (
	"context"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func persistCheckpoint(ctx context.Context, save func(context.Context) error) error {
	if save == nil {
		return domain.NewError(domain.ErrorInternal, "checkpoint", "session store is required")
	}

	if err := checkpointFenceError(ctx); err != nil {
		return err
	}

	persistCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := save(persistCtx); err != nil {
		return err
	}

	return checkpointFenceError(ctx)
}

func checkpointFenceError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return kube.LeaseFenceError(ctx)
}
