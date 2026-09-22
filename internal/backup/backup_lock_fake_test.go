package backup

import (
	"context"
)

// recordingBackupSessionLock is a workflow lease double that records whether
// the holder observed a fence error and whether the lock was released.
type recordingBackupSessionLock struct {
	err      error
	released bool
	bound    bool
}

func (l *recordingBackupSessionLock) Bind(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	l.bound = true
	return context.WithCancel(ctx)
}

func (l *recordingBackupSessionLock) Err() error { return l.err }

func (l *recordingBackupSessionLock) Release(context.Context) error {
	l.released = true
	return nil
}

func (l *recordingBackupSessionLock) Delete(context.Context) error { return nil }
