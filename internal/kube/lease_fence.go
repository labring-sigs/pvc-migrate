package kube

import (
	"context"
	"errors"
)

type LeaseFence interface {
	Err() error
}

type leaseFenceKey struct{}

type leaseFences struct {
	current LeaseFence
	parent  *leaseFences
}

// WithLeaseFence retains every enclosing lease, including during detached cleanup.
func WithLeaseFence(ctx context.Context, fence LeaseFence) context.Context {
	parent, _ := ctx.Value(leaseFenceKey{}).(*leaseFences)
	return context.WithValue(ctx, leaseFenceKey{}, &leaseFences{current: fence, parent: parent})
}

// LeaseFenceError checks ownership independently of cancellation so cleanup can
// use context.WithoutCancel without losing the lease's write protection.
func LeaseFenceError(ctx context.Context) error {
	var result error
	for fences, _ := ctx.Value(leaseFenceKey{}).(*leaseFences); fences != nil; fences = fences.parent {
		if fences.current != nil {
			result = errors.Join(result, fences.current.Err())
		}
	}

	return result
}
