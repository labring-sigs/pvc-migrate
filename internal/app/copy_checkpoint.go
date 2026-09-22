package app

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func checkpointWarmCopy(
	ctx context.Context,
	completedAt **metav1.Time,
	lastError *string,
	now time.Time,
	save func(context.Context) error,
) error {
	previousTime, previousError := *completedAt, *lastError
	completed := metav1.NewTime(now.UTC())
	*completedAt, *lastError = &completed, ""

	if err := persistCheckpoint(ctx, save); err != nil {
		*completedAt, *lastError = previousTime, previousError
		return err
	}

	return nil
}

// checkpointPodMigrationWarmPass commits the pass count with its phase change.
// A failed phase checkpoint must leave the pass eligible for retry.
func checkpointPodMigrationWarmPass(
	ctx context.Context,
	completed *int,
	finish func(context.Context) error,
) error {
	previous := *completed
	*completed++

	if err := persistCheckpoint(ctx, finish); err != nil {
		*completed = previous
		return err
	}

	return nil
}

func checkpointFinalCopy(
	ctx context.Context,
	completedAt **metav1.Time,
	checksumVerified *bool,
	lastError *string,
	verifyChecksum bool,
	now time.Time,
	save func(context.Context) error,
) error {
	previousTime, previousChecksum, previousError := *completedAt, *checksumVerified, *lastError
	completed := metav1.NewTime(now.UTC())
	*completedAt, *checksumVerified, *lastError = &completed, verifyChecksum, ""

	if err := persistCheckpoint(ctx, save); err != nil {
		*completedAt, *checksumVerified, *lastError = previousTime, previousChecksum, previousError
		return err
	}

	return nil
}
