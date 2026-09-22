package kube

import (
	"context"
	"errors"
	"testing"
)

type testLeaseFence struct{ err error }

func (f *testLeaseFence) Err() error { return f.err }

func TestLeaseFenceRetainsNestedOwnershipDuringCleanup(t *testing.T) {
	outer, inner := &testLeaseFence{}, &testLeaseFence{}
	ctx, cancel := context.WithCancel(t.Context())
	ctx = WithLeaseFence(WithLeaseFence(ctx, outer), inner)

	cancel()

	cleanup := context.WithoutCancel(ctx)
	if cleanup.Err() != nil || LeaseFenceError(cleanup) != nil {
		t.Fatal("cancellation invalidated a healthy ownership fence")
	}

	outer.err = errors.New("outer lease lost")
	if !errors.Is(LeaseFenceError(cleanup), outer.err) {
		t.Fatal("nested lease hid the outer lease failure")
	}

	inner.err = errors.New("inner lease lost")

	err := LeaseFenceError(cleanup)
	if !errors.Is(err, outer.err) || !errors.Is(err, inner.err) {
		t.Fatalf("missing ownership failures: %v", err)
	}
}

func TestLeaseFenceWithoutLease(t *testing.T) {
	if err := LeaseFenceError(t.Context()); err != nil {
		t.Fatal(err)
	}
}
