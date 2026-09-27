package controller

import (
	"testing"
)

// TestWorkflowReconcilerMaxConcurrentReconciles pins the worker cap contract:
// the serial default cannot be zeroed out by an unset or invalid option, and
// a positive override is carried through to the watch queues.
func TestWorkflowReconcilerMaxConcurrentReconciles(t *testing.T) {
	t.Parallel()

	if got := NewWorkflowReconciler().maxConcurrentReconciles; got != 1 {
		t.Fatalf("default workers = %d, want 1", got)
	}

	for _, tc := range []struct {
		option int
		want   int
	}{
		{option: 0, want: 1},
		{option: -3, want: 1},
		{option: 1, want: 1},
		{option: 4, want: 4},
	} {
		reconciler := NewWorkflowReconciler().WithMaxConcurrentReconciles(tc.option)
		if got := reconciler.maxConcurrentReconciles; got != tc.want {
			t.Fatalf("option %d: workers = %d, want %d", tc.option, got, tc.want)
		}
	}
}
