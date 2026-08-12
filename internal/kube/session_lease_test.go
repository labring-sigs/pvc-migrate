package kube

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	clienttesting "k8s.io/client-go/testing"
)

type cancelAwareLeaseClient struct {
	coordinationclient.LeaseInterface
	started chan struct{}
	lease   *coordinationv1.Lease
	calls   int
}

func (c *cancelAwareLeaseClient) Get(ctx context.Context, _ string, _ metav1.GetOptions) (*coordinationv1.Lease, error) {
	c.calls++
	if c.calls == 1 {
		close(c.started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.lease.DeepCopy(), nil
}

func (c *cancelAwareLeaseClient) Update(_ context.Context, lease *coordinationv1.Lease, _ metav1.UpdateOptions) (*coordinationv1.Lease, error) {
	c.lease = lease.DeepCopy()
	return lease.DeepCopy(), nil
}

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
	if err := lock.Delete(ctx); err != nil {
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

func TestSessionLeaseDeleteFencesReplacement(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	store := NewConfigMapSessionStore(client)
	lock, err := store.AcquireSessionLock(ctx, "system", "session-delete-fence")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := client.CoordinationV1().Leases("system").Get(ctx, SessionLockName("session-delete-fence"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	replacement := lease.DeepCopy()
	replacement.Spec.HolderIdentity = ptr("new-holder")
	if _, err := client.CoordinationV1().Leases("system").Update(ctx, replacement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deleteErr := lock.Delete(ctx)
	if domain.CategoryOf(deleteErr) != domain.ErrorConflict {
		t.Fatalf("delete error=%v", deleteErr)
	}
	current, err := client.CoordinationV1().Leases("system").Get(ctx, lease.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != "new-holder" {
		t.Fatalf("replacement holder=%v", current.Spec.HolderIdentity)
	}
	_ = lock.Release(ctx)
}

func TestConfigMapSessionStoreDeletesSessionLeaseWithPreconditions(t *testing.T) {
	ctx := context.Background()
	const sessionID = "session-preconditions"
	client := fake.NewClientset(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "system",
			Name:            SessionLockName(sessionID),
			UID:             types.UID("lease-uid"),
			ResourceVersion: "lease-resource-version",
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     sessionID,
			},
		},
	})
	store := NewConfigMapSessionStore(client)
	var deleteOptions metav1.DeleteOptions
	client.PrependReactor("delete", "leases", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteOptions = action.(clienttesting.DeleteAction).GetDeleteOptions()
		return true, nil, nil
	})
	if err := store.DeleteSessionLease(ctx, "system", sessionID); err != nil {
		t.Fatal(err)
	}
	if deleteOptions.Preconditions == nil || deleteOptions.Preconditions.ResourceVersion == nil || *deleteOptions.Preconditions.ResourceVersion != "lease-resource-version" {
		t.Fatalf("missing resourceVersion precondition: %#v", deleteOptions.Preconditions)
	}
	if deleteOptions.Preconditions.UID == nil || *deleteOptions.Preconditions.UID != types.UID("lease-uid") {
		t.Fatalf("missing UID precondition: %#v", deleteOptions.Preconditions)
	}
}

func TestConfigMapSessionStorePreservesReplacedLeaseOnDeleteRace(t *testing.T) {
	ctx := context.Background()
	const sessionID = "session-delete-race"
	client := fake.NewClientset(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "system",
			Name:            SessionLockName(sessionID),
			UID:             types.UID("replacement-lease-uid"),
			ResourceVersion: "replacement-resource-version",
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     sessionID,
			},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr("new-holder")},
	})
	store := NewConfigMapSessionStore(client)
	client.PrependReactor("delete", "leases", func(action clienttesting.Action) (bool, runtime.Object, error) {
		name := action.(clienttesting.DeleteAction).GetName()
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Group: coordinationv1.GroupName, Resource: "leases"},
			name,
			errors.New("lease changed after it was read"),
		)
	})
	if err := store.DeleteSessionLease(ctx, "system", sessionID); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("delete race category=%s error=%v", domain.CategoryOf(err), err)
	}
	lease, err := client.CoordinationV1().Leases("system").Get(ctx, SessionLockName(sessionID), metav1.GetOptions{})
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

func TestSessionLeaseCancellationStopsInFlightRenewal(t *testing.T) {
	oldRenewEvery := sessionLeaseRenewEvery
	oldDuration := sessionLeaseDuration
	sessionLeaseRenewEvery = 5 * time.Millisecond
	sessionLeaseDuration = 30 * time.Second
	defer func() {
		sessionLeaseRenewEvery = oldRenewEvery
		sessionLeaseDuration = oldDuration
	}()

	const (
		sessionID = "session-cancel-renew"
		holder    = "holder"
	)
	leaseClient := &cancelAwareLeaseClient{
		started: make(chan struct{}),
		lease: &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "system",
				Name:      SessionLockName(sessionID),
				Labels: map[string]string{
					ManagedByLabel: ManagedByValue,
					SessionKey:     sessionID,
				},
			},
			Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr(holder)},
		},
	}
	lock := newSessionLease(context.Background(), leaseClient, "system", leaseClient.lease.Name, sessionID, holder)
	select {
	case <-leaseClient.started:
	case <-time.After(time.Second):
		t.Fatal("lease renewal did not start")
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatalf("release after cancellation: %v", err)
	}
	if err := lock.Err(); err != nil {
		t.Fatalf("normal cancellation recorded as lease loss: %v", err)
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
