package app

import (
	"reflect"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestPendingHandoffBlocksCopyExecutionAbortAndCleanup(t *testing.T) {
	executor, object, store, engine := copyExecutorFixture(t)
	object.Annotations = map[string]string{
		"migrate.sealos.io/reservation-copy-pending": "source/token",
	}
	store.object = object.DeepCopy()
	before := object.DeepCopy()

	for _, operation := range []func() error{
		func() error { return executor.Run(t.Context(), object) },
		func() error { return executor.Abort(t.Context(), object) },
		func() error { return executor.Cleanup(t.Context(), object, CopyCleanupOptions{Finalize: true}) },
	} {
		if err := operation(); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("pending handoff error = %v", err)
		}
	}

	if !reflect.DeepEqual(object, before) || len(engine.requests) != 0 ||
		len(engine.cleanups) != 0 {
		t.Fatal("ordinary execution advanced a pending handoff")
	}
}
