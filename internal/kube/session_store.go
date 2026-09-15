package kube

import (
	"context"
	"errors"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

const (
	SessionBackendConfigMap = "configmap"
	SessionBackendCRD       = "crd"
)

// SessionLocker acquires the renewable, process-owned lease that fences every
// mutation of one workflow identity. Workflow persistence and locking are
// separate concerns: executors only need this narrow contract.
type SessionLocker interface {
	AcquireSessionLock(ctx context.Context, namespace, sessionID string) (SessionLock, error)
}

var ErrSessionLockContention = errors.New("session lock contention")

func IsSessionLockContention(err error) bool {
	return errors.Is(err, ErrSessionLockContention)
}

// SessionLock is a renewable, process-owned lock for one workflow identity.
type SessionLock interface {
	Bind(ctx context.Context) (context.Context, context.CancelFunc)
	Err() error
	Release(ctx context.Context) error
	Delete(ctx context.Context) error
}

// AcquireRequiredSessionLock enforces the SessionLocker contract at the
// boundary so an invalid implementation cannot turn a fencing operation into
// a nil-pointer panic.
func AcquireRequiredSessionLock(
	ctx context.Context,
	locker SessionLocker,
	namespace, sessionID string,
) (SessionLock, error) {
	if locker == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"session lock",
			"session locker is required",
		)
	}

	lock, err := locker.AcquireSessionLock(ctx, namespace, sessionID)
	if err != nil {
		return nil, err
	}

	if lock == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"session lock",
			"session locker returned a nil lock",
		)
	}

	return lock, nil
}

// SessionConfigMapName derives the ConfigMap name that persists a workflow
// object when a cluster runs the ConfigMap-backed workflow store.
func SessionConfigMapName(id string) string {
	return "pvc-migrate-session-" + id
}
