//nolint:testpackage // these tests verify unexported lease-context fencing.
package crosscluster

import (
	"context"
	"errors"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

type testSessionLocker struct{ lock kube.SessionLock }

func (l testSessionLocker) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return l.lock, nil
}

type testSessionLock struct{ err error }

func (l testSessionLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}

func (l testSessionLock) Err() error { return l.err }

func (testSessionLock) Release(context.Context) error { return nil }

func (testSessionLock) Delete(context.Context) error { return nil }

type deletionSessionLock struct {
	deleteErr error
	deletes   int
}

func (l *deletionSessionLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}

func (l *deletionSessionLock) Err() error { return nil }

func (l *deletionSessionLock) Release(context.Context) error { return nil }

func (l *deletionSessionLock) Delete(context.Context) error {
	l.deletes++
	return l.deleteErr
}

func TestWithLockExposesSessionLeaseFenceToOperation(t *testing.T) {
	lost := errors.New("session lease lost")
	service := &Service{
		locker: testSessionLocker{lock: testSessionLock{err: lost}},
	}
	session := &CopySession{
		ID:   "workflow",
		Spec: CopySpec{SessionContext: SessionContext{SessionNamespace: "sessions"}},
	}

	var observed error

	err := service.withLock(context.Background(), session, func(ctx context.Context) error {
		observed = requireSessionLease(ctx)
		return nil
	})

	if !errors.Is(observed, lost) {
		t.Fatalf("operation context fence = %v, want lease loss", observed)
	}

	if !errors.Is(err, lost) {
		t.Fatalf("withLock error = %v, want lease loss", err)
	}
}

func TestDeleteSessionKeepsRecordWhenLeaseDeletionFails(t *testing.T) {
	service := &Service{}
	lock := &deletionSessionLock{deleteErr: errors.New("lease deletion failed")}
	ctx := context.WithValue(t.Context(), sessionLockContextKey{}, kube.SessionLock(lock))
	deleted := false

	if err := service.deleteSession(ctx, "sessions", "workflow", func() error {
		deleted = true
		return nil
	}); !errors.Is(err, lock.deleteErr) {
		t.Fatalf("deleteSession error = %v, want lease deletion failure", err)
	}

	if deleted || lock.deletes != 1 {
		t.Fatalf(
			"record deletion after failed lease cleanup: deleted=%v attempts=%d",
			deleted,
			lock.deletes,
		)
	}

	lock.deleteErr = nil

	if err := service.deleteSession(ctx, "sessions", "workflow", func() error {
		deleted = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if !deleted || lock.deletes != 2 {
		t.Fatalf("record deletion did not converge: deleted=%v attempts=%d", deleted, lock.deletes)
	}
}
