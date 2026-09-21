package crosscluster

import (
	"context"
	"errors"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

type testSessionLocker struct{ lock kube.SessionLock }

func (l testSessionLocker) AcquireSessionLock(context.Context, string, string) (kube.SessionLock, error) {
	return l.lock, nil
}

type testSessionLock struct{ err error }

func (l testSessionLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}

func (l testSessionLock) Err() error { return l.err }

func (testSessionLock) Release(context.Context) error { return nil }

func (testSessionLock) Delete(context.Context) error { return nil }

func TestWithLockExposesSessionLeaseFenceToOperation(t *testing.T) {
	lost := errors.New("session lease lost")
	service := &Service{
		locker: testSessionLocker{lock: testSessionLock{err: lost}},
	}
	session := &Session{ID: "workflow", Spec: Spec{SessionNamespace: "sessions"}}

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
