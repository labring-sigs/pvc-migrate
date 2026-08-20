package testutil

import (
	"fmt"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

// MustType fails the current test when a fake client action or object does not
// satisfy the contract exercised by the test.
func MustType[T any](tb testing.TB, value any) T {
	tb.Helper()

	typed, ok := value.(T)
	if !ok {
		tb.Fatalf("expected value of type %v, got %T", reflect.TypeFor[T](), value)
	}

	return typed
}

// MustActionObject extracts the typed object from a Kubernetes fake create or
// update action.
func MustActionObject[T runtime.Object](tb testing.TB, action any) T {
	tb.Helper()

	typed, err := ActionObject[T](action)
	if err != nil {
		tb.Fatal(err)
	}

	return typed
}

// ActionObject extracts an object from a Kubernetes fake action for callbacks
// that report contract violations as errors instead of test failures.
func ActionObject[T runtime.Object](action any) (T, error) {
	type objectAction interface {
		GetObject() runtime.Object
	}

	var zero T

	typedAction, ok := action.(objectAction)
	if !ok {
		return zero, fmt.Errorf("expected Kubernetes object action, got %T", action)
	}

	typed, ok := typedAction.GetObject().(T)
	if !ok {
		return zero, fmt.Errorf(
			"expected action object of type %v, got %T",
			reflect.TypeFor[T](),
			typedAction.GetObject(),
		)
	}

	return typed, nil
}
