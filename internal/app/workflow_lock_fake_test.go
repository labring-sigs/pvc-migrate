package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

// fakeSessionLock is a workflow lease double for executor tests.
type fakeSessionLock struct {
	err      error
	released bool
	deleted  bool
}

func (l *fakeSessionLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

func (l *fakeSessionLock) Err() error { return l.err }

func (l *fakeSessionLock) Release(context.Context) error {
	l.released = true
	return nil
}

func (l *fakeSessionLock) Delete(context.Context) error {
	l.deleted = true
	return nil
}

// fakeSessionLocker hands out one prepared lock.
type fakeSessionLocker struct {
	lock kube.SessionLock
}

func (f *fakeSessionLocker) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	if f.lock == nil {
		return &fakeSessionLock{}, nil
	}
	return f.lock, nil
}
