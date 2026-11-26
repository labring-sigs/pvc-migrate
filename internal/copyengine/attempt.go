package copyengine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

// RunAttempt reports transfer progress and owns the log streams for this attempt.
// The caller keeps the operation context for checkpointing and resource cleanup.
func RunAttempt(
	ctx context.Context,
	engine Engine,
	request CopyRequest,
	timeout time.Duration,
	logs *kube.ToolLogStream,
) (err error) {
	logger := request.Runtime.Logger
	if logger != nil {
		logger = logger.With(
			"session",
			request.SessionID,
			"pvc",
			request.AttemptIdentity.Source.Name,
			"mode",
			request.Mode,
			"attempt",
			request.Attempt,
		)
		logger.InfoContext(
			ctx,
			"copy started",
			"source",
			request.AttemptIdentity.Source.Namespace+"/"+request.AttemptIdentity.Source.Name,
			"sourcePath",
			request.Source.Path,
			"destination",
			request.Destination.Reference.Namespace+"/"+request.Destination.Reference.Name,
			"destinationPath",
			request.Destination.Path,
		)
	}

	attemptCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc

		attemptCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	defer func() {
		logs.Stop()

		if observed := logs.ObservedError(); err != nil && observed != nil {
			err = errors.Join(err, observed)
		}
	}()

	err = engine.Copy(attemptCtx, request, func(p Progress) {
		if logger != nil {
			logger.InfoContext(ctx, "copy progress", "state", p.State, "message", p.Message)
		}
	})

	expired := errors.Is(attemptCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
	if err != nil && expired {
		return domain.WrapError(
			domain.ErrorTimeout,
			domain.ErrorOperationCopyAttempt,
			fmt.Sprintf(
				"copy attempt %d exceeded --copy-timeout %s",
				request.Attempt,
				timeout,
			),
			err,
		)
	}

	return err
}

// RetryBackoffDelay returns the exponential backoff before the next copy
// attempt. The multiple saturates instead of overflowing: a float-to-int
// conversion of 2^63 wraps negative and turned late retries into a tight
// loop, and even a correct 2^63 multiple overflows against hour-scale bases.
func RetryBackoffDelay(base time.Duration, retryIndex int) time.Duration {
	if base <= 0 {
		return 0
	}

	multiple := uint64(1) << uint(min(retryIndex, 62))
	if uint64(base) > math.MaxInt64/multiple {
		return time.Duration(math.MaxInt64)
	}

	// The guard above bounds the product to math.MaxInt64.
	return base * time.Duration(multiple) //nolint:gosec // bounded by the saturation guard
}
