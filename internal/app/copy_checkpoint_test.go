package app

import (
	"context"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCopyCompletionCheckpointsRestoreFailedWrites(t *testing.T) {
	writeErr := errors.New("checkpoint failed")

	now := time.Unix(100, 0)
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		if canceled {
			cancel()
		}

		writes := 0
		save := func(context.Context) error { writes++; return writeErr }
		warm := v1alpha1.CopySyncStatus{LastError: "previous warm failure"}

		err := checkpointWarmCopy(ctx, &warm.WarmCompletedAt, &warm.LastError, now, save)
		if err == nil || warm.WarmCompletedAt != nil || warm.LastError != "previous warm failure" {
			t.Fatalf("warm checkpoint advanced after failure: %+v, %v", warm, err)
		}

		previousTime := metav1.NewTime(time.Unix(50, 0))
		final := v1alpha1.MigrationSyncStatus{
			FinalCompletedAt: &previousTime,
			ChecksumVerified: true,
			LastError:        "previous final failure",
		}

		err = checkpointFinalCopy(
			ctx,
			&final.FinalCompletedAt,
			&final.ChecksumVerified,
			&final.LastError,
			false,
			now,
			save,
		)
		if err == nil || final.FinalCompletedAt != &previousTime || !final.ChecksumVerified ||
			final.LastError != "previous final failure" {
			t.Fatalf("final checkpoint advanced after failure: %+v, %v", final, err)
		}

		if canceled && writes != 0 {
			t.Fatal("canceled operation wrote a completion checkpoint")
		}

		cancel()
	}
}

func TestPodMigrationCompletionCheckpointsPersistSeparatePhases(t *testing.T) {
	state := v1alpha1.PodMigrationSyncStatus{LastError: "retry"}
	now := time.Unix(100, 0)
	writes := 0

	save := func(context.Context) error { writes++; return nil }
	if err := checkpointWarmCopy(
		t.Context(),
		&state.WarmCompletedAt,
		&state.LastError,
		now,
		save,
	); err != nil {
		t.Fatal(err)
	}

	if state.WarmCompletedAt == nil || state.FinalCompletedAt != nil || state.ChecksumVerified ||
		state.LastError != "" {
		t.Fatalf("warm checkpoint changed final-copy state: %+v", state)
	}

	if err := checkpointFinalCopy(
		t.Context(),
		&state.FinalCompletedAt,
		&state.ChecksumVerified,
		&state.LastError,
		true,
		now,
		save,
	); err != nil {
		t.Fatal(err)
	}

	if writes != 2 || state.FinalCompletedAt == nil || !state.ChecksumVerified ||
		!state.WarmCompletedAt.Time.Equal(now) {
		t.Fatalf("incomplete final checkpoint: %+v, writes=%d", state, writes)
	}
}
