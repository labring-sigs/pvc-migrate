package kube

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestConfigMapSessionStoreSessionLeaseContendsAndReleases(t *testing.T) {
	ctx := context.Background()
	store := NewConfigMapSessionStore(fake.NewClientset())
	first, err := store.AcquireSessionLock(ctx, "system", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.AcquireSessionLock(ctx, "system", "session-1")
	if second != nil || domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("second holder = %v, error = %v", second, err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	third, err := store.AcquireSessionLock(ctx, "system", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConfigMapSessionStoreDeletesSessionLease(t *testing.T) {
	ctx := context.Background()
	store := NewConfigMapSessionStore(fake.NewClientset())
	lock, err := store.AcquireSessionLock(ctx, "system", "session-delete")
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSessionLease(ctx, "system", "session-delete"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSessionLease(ctx, "system", "session-delete"); err != nil {
		t.Fatalf("idempotent lease deletion: %v", err)
	}
}

func TestSessionLeaseRenewFailureFencesContext(t *testing.T) {
	oldRenewEvery := sessionLeaseRenewEvery
	oldDuration := sessionLeaseDuration
	sessionLeaseRenewEvery = 5 * time.Millisecond
	sessionLeaseDuration = 20 * time.Millisecond
	defer func() {
		sessionLeaseRenewEvery = oldRenewEvery
		sessionLeaseDuration = oldDuration
	}()

	client := fake.NewClientset()
	client.PrependReactor("update", "leases", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("injected lease API failure")
	})
	store := NewConfigMapSessionStore(client)
	lock, err := store.AcquireSessionLock(context.Background(), "system", "session-2")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-lock.Context().Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("lease context was not canceled after renewal failure")
	}
	if lock.Err() == nil || domain.CategoryOf(lock.Err()) != domain.ErrorKubernetes {
		t.Fatalf("renewal error = %v", lock.Err())
	}
	if err := lock.Release(context.Background()); domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("release after fencing error = %v", err)
	}
}

func TestSessionLeaseRecoversExpiredHolder(t *testing.T) {
	oldDuration := sessionLeaseDuration
	sessionLeaseDuration = time.Second
	defer func() { sessionLeaseDuration = oldDuration }()
	now := metav1.NewMicroTime(time.Now().Add(-time.Minute))
	duration := int32(1)
	client := fake.NewClientset(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "system",
			Name:      SessionLockName("session-expired"),
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     "session-expired",
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr("abandoned-holder"),
			LeaseDurationSeconds: &duration,
			RenewTime:            &now,
		},
	})
	store := NewConfigMapSessionStore(client)
	lock, err := store.AcquireSessionLock(context.Background(), "system", "session-expired")
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
