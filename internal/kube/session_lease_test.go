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

func TestConfigMapSessionStoreDeletesSessionLeaseWithPreconditions(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	store := NewConfigMapSessionStore(client)
	lock, err := store.AcquireSessionLock(ctx, "system", "session-preconditions")
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatal(err)
	}
	var deleteOptions metav1.DeleteOptions
	client.PrependReactor("delete", "leases", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteOptions = action.(clienttesting.DeleteAction).GetDeleteOptions()
		return true, nil, nil
	})
	if err := store.DeleteSessionLease(ctx, "system", "session-preconditions"); err != nil {
		t.Fatal(err)
	}
	if deleteOptions.Preconditions == nil || deleteOptions.Preconditions.ResourceVersion == nil || *deleteOptions.Preconditions.ResourceVersion == "" {
		t.Fatalf("missing resourceVersion precondition: %#v", deleteOptions.Preconditions)
	}
	if deleteOptions.Preconditions.UID == nil || *deleteOptions.Preconditions.UID == "" {
		t.Fatalf("missing UID precondition: %#v", deleteOptions.Preconditions)
	}
}

func TestConfigMapSessionStorePreservesReplacedLeaseOnDeleteRace(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	store := NewConfigMapSessionStore(client)
	lock, err := store.AcquireSessionLock(ctx, "system", "session-delete-race")
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("delete", "leases", func(action clienttesting.Action) (bool, runtime.Object, error) {
		name := action.(clienttesting.DeleteAction).GetName()
		lease, getErr := client.CoordinationV1().Leases("system").Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return true, nil, getErr
		}
		lease = lease.DeepCopy()
		lease.Spec.HolderIdentity = ptr("new-holder")
		if _, updateErr := client.CoordinationV1().Leases("system").Update(ctx, lease, metav1.UpdateOptions{}); updateErr != nil {
			return true, nil, updateErr
		}
		return false, nil, nil
	})
	if err := store.DeleteSessionLease(ctx, "system", "session-delete-race"); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("delete race category=%s error=%v", domain.CategoryOf(err), err)
	}
	lease, err := client.CoordinationV1().Leases("system").Get(ctx, SessionLockName("session-delete-race"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "new-holder" {
		t.Fatalf("replacement Lease was changed: %#v", lease.Spec.HolderIdentity)
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
