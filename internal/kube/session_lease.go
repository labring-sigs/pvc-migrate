package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

const (
	sessionLeasePrefix                = "pvc-migrate-lock-"
	sessionLeaseDurationSeconds int32 = 30
)

var (
	sessionLeaseDuration   = 30 * time.Second
	sessionLeaseRenewEvery = 10 * time.Second
)

// SessionLockName is deterministic for a namespace/session pair while
// keeping arbitrary session IDs within Kubernetes DNS label limits.
func SessionLockName(id string) string {
	digest := sha256.Sum256([]byte(id))
	return sessionLeasePrefix + hex.EncodeToString(digest[:])[:32]
}

// AcquireSessionLock acquires a renewable Kubernetes Lease. A live holder is
// never overwritten; an expired holder may be fenced with an optimistic
// resourceVersion update.
func (s *ConfigMapSessionStore) AcquireSessionLock(ctx context.Context, namespace, id string) (SessionLock, error) {
	if namespace == "" || id == "" {
		return nil, domain.NewError(domain.ErrorValidation, "acquire session lock", "session namespace and ID are required")
	}
	leases := s.client.CoordinationV1().Leases(namespace)
	holder := string(uuid.NewUUID())
	name := SessionLockName(id)
	durationSeconds := sessionLeaseDurationSeconds
	now := metav1.NewMicroTime(time.Now().UTC())
	labelsForLease := map[string]string{
		ManagedByLabel: ManagedByValue,
		SessionKey:     id,
	}

	for attempt := 0; attempt < 2; attempt++ {
		lease, err := leases.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, createErr := leases.Create(ctx, &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
					Labels:    labelsForLease,
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       ptr(holder),
					LeaseDurationSeconds: &durationSeconds,
					AcquireTime:          &now,
					RenewTime:            &now,
					LeaseTransitions:     ptr(int32(1)),
				},
			}, metav1.CreateOptions{})
			if createErr == nil {
				return newSessionLease(ctx, leases, namespace, name, id, holder), nil
			}
			if apierrors.IsAlreadyExists(createErr) {
				continue
			}
			return nil, domain.WrapError(domain.ErrorKubernetes, "acquire session lock", fmt.Sprintf("create Lease %s/%s", namespace, name), createErr)
		}
		if err != nil {
			return nil, domain.WrapError(domain.ErrorKubernetes, "acquire session lock", fmt.Sprintf("read Lease %s/%s", namespace, name), err)
		}
		if lease.Labels[ManagedByLabel] != ManagedByValue || lease.Labels[SessionKey] != id {
			return nil, domain.NewError(domain.ErrorConflict, "acquire session lock", fmt.Sprintf("Lease %s/%s is owned by another resource", namespace, name))
		}
		if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" && !sessionLeaseExpired(lease, time.Now().UTC()) {
			return nil, domain.NewError(domain.ErrorConflict, "acquire session lock", fmt.Sprintf("session %s is already being changed", id))
		}
		updated := lease.DeepCopy()
		updated.Spec.HolderIdentity = ptr(holder)
		updated.Spec.LeaseDurationSeconds = &durationSeconds
		updated.Spec.AcquireTime = &now
		updated.Spec.RenewTime = &now
		transitions := int32(1)
		if updated.Spec.LeaseTransitions != nil {
			transitions = *updated.Spec.LeaseTransitions + 1
		}
		updated.Spec.LeaseTransitions = &transitions
		_, err = leases.Update(ctx, updated, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			return nil, domain.NewError(domain.ErrorConflict, "acquire session lock", fmt.Sprintf("session %s changed while acquiring its lock", id))
		}
		if err != nil {
			return nil, domain.WrapError(domain.ErrorKubernetes, "acquire session lock", fmt.Sprintf("claim Lease %s/%s", namespace, name), err)
		}
		return newSessionLease(ctx, leases, namespace, name, id, holder), nil
	}
	return nil, domain.NewError(domain.ErrorConflict, "acquire session lock", fmt.Sprintf("session %s lock acquisition raced with another process", id))
}

// DeleteSessionLease removes a lock owned by the session. Cleanup holds the
// same lock while deleting the session, so a concurrent mutator cannot race
// this operation. The resourceVersion precondition also protects the small
// window where an expired holder is replaced before its cleanup call runs.
func (s *ConfigMapSessionStore) DeleteSessionLease(ctx context.Context, namespace, id string) error {
	if namespace == "" || id == "" {
		return domain.NewError(domain.ErrorValidation, "delete session lock", "session namespace and ID are required")
	}
	leases := s.client.CoordinationV1().Leases(namespace)
	name := SessionLockName(id)
	lease, err := leases.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "delete session lock", fmt.Sprintf("read Lease %s/%s", namespace, name), err)
	}
	if lease.Labels[ManagedByLabel] != ManagedByValue || lease.Labels[SessionKey] != id {
		return domain.NewError(domain.ErrorConflict, "delete session lock", fmt.Sprintf("Lease %s/%s is owned by another resource", namespace, name))
	}
	preconditions := &metav1.Preconditions{ResourceVersion: &lease.ResourceVersion}
	if lease.UID != "" {
		preconditions.UID = &lease.UID
	}
	err = leases.Delete(ctx, name, metav1.DeleteOptions{Preconditions: preconditions})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if apierrors.IsConflict(err) {
		return domain.WrapError(domain.ErrorConflict, "delete session lock", fmt.Sprintf("Lease %s/%s changed while deleting", namespace, name), err)
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "delete session lock", fmt.Sprintf("delete Lease %s/%s", namespace, name), err)
	}
	return nil
}

type sessionLease struct {
	leases     coordinationclient.LeaseInterface
	namespace  string
	name       string
	sessionID  string
	holder     string
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	once       sync.Once
	mu         sync.RWMutex
	err        error
	releaseErr error
	deleted    bool
}

func newSessionLease(parent context.Context, leases coordinationclient.LeaseInterface, namespace, name, sessionID, holder string) *sessionLease {
	ctx, cancel := context.WithCancel(parent)
	lock := &sessionLease{
		leases: leases, namespace: namespace, name: name, sessionID: sessionID,
		holder: holder, ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}
	go lock.renewLoop()
	return lock
}

func (l *sessionLease) Context() context.Context { return l.ctx }

func (l *sessionLease) Err() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.err
}

func (l *sessionLease) renewLoop() {
	ticker := time.NewTicker(sessionLeaseRenewEvery)
	defer ticker.Stop()
	defer close(l.done)
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			if err := l.renew(); err != nil {
				l.markLost(err)
				return
			}
		}
	}
}

func (l *sessionLease) renew() error {
	renewCtx, cancel := context.WithTimeout(context.Background(), sessionLeaseRenewTimeout())
	defer cancel()
	lease, err := l.leases.Get(renewCtx, l.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return domain.NewError(domain.ErrorConflict, "renew session lock", "session lock disappeared")
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "renew session lock", "read session Lease", err)
	}
	if lease.Labels[ManagedByLabel] != ManagedByValue || lease.Labels[SessionKey] != l.sessionID || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != l.holder {
		return domain.NewError(domain.ErrorConflict, "renew session lock", "session lock ownership was fenced")
	}
	now := metav1.NewMicroTime(time.Now().UTC())
	lease.Spec.RenewTime = &now
	_, err = l.leases.Update(renewCtx, lease, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return domain.NewError(domain.ErrorConflict, "renew session lock", "session lock resourceVersion changed")
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "renew session lock", "update session Lease", err)
	}
	return nil
}

func sessionLeaseRenewTimeout() time.Duration {
	timeout := sessionLeaseRenewEvery
	if timeout <= 0 || timeout > sessionLeaseDuration/3 {
		timeout = sessionLeaseDuration / 3
	}
	if timeout <= 0 {
		return time.Second
	}
	return timeout
}

func (l *sessionLease) markLost(err error) {
	l.mu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.mu.Unlock()
	l.cancel()
}

func (l *sessionLease) Release(ctx context.Context) error {
	l.once.Do(func() {
		l.cancel()
		<-l.done
		l.mu.RLock()
		deleted := l.deleted
		l.mu.RUnlock()
		if deleted {
			return
		}
		if l.Err() != nil {
			l.setReleaseErr(l.Err())
			return
		}
		lease, err := l.leases.Get(ctx, l.name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil {
			l.setReleaseErr(domain.WrapError(domain.ErrorKubernetes, "release session lock", "read session Lease", err))
			return
		}
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != l.holder {
			l.setReleaseErr(domain.NewError(domain.ErrorConflict, "release session lock", "session lock ownership was fenced"))
			return
		}
		lease.Spec.HolderIdentity = nil
		lease.Spec.RenewTime = nil
		if _, err := l.leases.Update(ctx, lease, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
			if apierrors.IsConflict(err) {
				l.setReleaseErr(domain.NewError(domain.ErrorConflict, "release session lock", "session lock resourceVersion changed"))
			} else {
				l.setReleaseErr(domain.WrapError(domain.ErrorKubernetes, "release session lock", "clear session Lease", err))
			}
		}
	})
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.releaseErr
}

// Delete stops renewal and deletes the Lease only when it is still held by
// this lock. It is used when the protected session resources are removed and
// the lock must disappear as one operation.
func (l *sessionLease) Delete(ctx context.Context) error {
	l.cancel()
	<-l.done
	lease, err := l.leases.Get(ctx, l.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		l.mu.Lock()
		l.deleted = true
		l.mu.Unlock()
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "delete session lock", "read session Lease", err)
	}
	if lease.Labels[ManagedByLabel] != ManagedByValue || lease.Labels[SessionKey] != l.sessionID || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != l.holder {
		return domain.NewError(domain.ErrorConflict, "delete session lock", "session lock ownership was fenced")
	}
	preconditions := &metav1.Preconditions{ResourceVersion: &lease.ResourceVersion}
	if lease.UID != "" {
		preconditions.UID = &lease.UID
	}
	err = l.leases.Delete(ctx, l.name, metav1.DeleteOptions{Preconditions: preconditions})
	if apierrors.IsNotFound(err) {
		err = nil
	}
	if apierrors.IsConflict(err) {
		return domain.WrapError(domain.ErrorConflict, "delete session lock", "session lock changed while deleting", err)
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "delete session lock", "delete session Lease", err)
	}
	l.mu.Lock()
	l.deleted = true
	l.mu.Unlock()
	return nil
}

func (l *sessionLease) setReleaseErr(err error) {
	l.mu.Lock()
	l.releaseErr = err
	l.mu.Unlock()
}

func sessionLeaseExpired(lease *coordinationv1.Lease, now time.Time) bool {
	var renewed time.Time
	switch {
	case lease.Spec.RenewTime != nil:
		renewed = lease.Spec.RenewTime.Time
	case lease.Spec.AcquireTime != nil:
		renewed = lease.Spec.AcquireTime.Time
	default:
		return true
	}
	duration := sessionLeaseDuration
	if lease.Spec.LeaseDurationSeconds != nil && *lease.Spec.LeaseDurationSeconds > 0 {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	return now.Sub(renewed) >= duration
}

func ptr[T any](value T) *T { return &value }
