package controller

import (
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestRenameMoveReconcileResultDefersConflicts pins the result mapping shared
// by every family: a raw HTTP 409 from the status update must defer with a
// fast one-second requeue instead of falling into controller-runtime's
// exponential backoff, which delayed identity cutovers by minutes per
// collision.
func TestRenameMoveReconcileResultDefersConflicts(t *testing.T) {
	conflict := apierrors.NewConflict(
		schema.GroupResource{Group: "migrate.sealos.io", Resource: "renames"},
		"workflow",
		nil,
	)

	for name, mapper := range map[string]func(error) (reconcile.Result, error){
		"rename": renameReconcileResult,
		"move":   moveReconcileResult,
	} {
		result, err := mapper(conflict)
		if err != nil {
			t.Fatalf("%s mapper surfaced the conflict: %v", name, err)
		}

		if result.RequeueAfter != time.Second {
			t.Fatalf("%s mapper requeue = %v, want the one-second defer", name, result.RequeueAfter)
		}
	}
}
