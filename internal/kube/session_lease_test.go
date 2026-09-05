package kube

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
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

type nilLockSessionLocker struct{}

func (nilLockSessionLocker) AcquireSessionLock(
	context.Context,
	string,
	string,
) (SessionLock, error) {
	return nil, nil
}

func TestAcquireRequiredSessionLockRejectsMissingImplementations(t *testing.T) {
	for name, locker := range map[string]SessionLocker{
		"missing locker": nil,
		"nil lock":       nilLockSessionLocker{},
	} {
		t.Run(name, func(t *testing.T) {
			lock, err := AcquireRequiredSessionLock(
				context.Background(),
				locker,
				"system",
				"session",
			)
			if lock != nil || domain.CategoryOf(err) != domain.ErrorInternal {
				t.Fatalf("lock=%v category=%s error=%v", lock, domain.CategoryOf(err), err)
			}
		})
	}
}

type cancelAwareLeaseClient struct {
	coordinationclient.LeaseInterface
	started chan struct{}
	lease   *coordinationv1.Lease
	calls   int
}

func (c *cancelAwareLeaseClient) Get(
	ctx context.Context,
	_ string,
	_ metav1.GetOptions,
) (*coordinationv1.Lease, error) {
	c.calls++
	if c.calls == 1 {
		close(c.started)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	return c.lease.DeepCopy(), nil
}

func (c *cancelAwareLeaseClient) Update(
	_ context.Context,
	lease *coordinationv1.Lease,
	_ metav1.UpdateOptions,
) (*coordinationv1.Lease, error) {
	c.lease = lease.DeepCopy()
	return lease.DeepCopy(), nil
}

func newSessionLeaseTestClient() *fake.Clientset {
	client := fake.NewClientset()
	client.PrependReactor(
		"create",
		"leases",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			lease, err := testutil.ActionObject[*coordinationv1.Lease](action)
			if err != nil {
				return true, nil, err
			}

			if lease.UID == "" {
				lease.UID = types.UID("lease-" + lease.Name)
			}

			return false, nil, nil
		},
	)

	return client
}

func TestLeaseDurationSecondsRoundsAndClamps(t *testing.T) {
	for _, test := range []struct {
		name     string
		duration time.Duration
		want     int32
	}{
		{name: "zero", duration: 0, want: 1},
		{name: "negative", duration: -time.Second, want: 1},
		{name: "subsecond", duration: 1, want: 1},
		{name: "round up", duration: time.Second + time.Millisecond, want: 2},
		{name: "exact", duration: 30 * time.Second, want: 30},
		{name: "clamp", duration: time.Duration(maxLeaseDurationSeconds+1) * time.Second, want: int32(maxLeaseDurationSeconds)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := leaseDurationSeconds(test.duration); got != test.want {
				t.Fatalf("leaseDurationSeconds(%s)=%d, want %d", test.duration, got, test.want)
			}
		})
	}
}

func TestConfigMapSessionStoreSessionLeaseContendsAndReleases(t *testing.T) {
	ctx := context.Background()
	store := NewConfigMapSessionStore(newSessionLeaseTestClient())

	first, err := store.AcquireSessionLock(ctx, "system", "session-1")
	if err != nil {
		t.Fatal(err)
	}

	second, err := store.AcquireSessionLock(ctx, "system", "session-1")
	if second != nil || domain.CategoryOf(err) != domain.ErrorConflict || !IsSessionLockContention(err) {
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

func TestCompositeSessionLeaseKeepsQueryableSessionLabel(t *testing.T) {
	ctx := context.Background()
	client := newSessionLeaseTestClient()
	store := NewConfigMapSessionStore(client)

	lockID := "workflow/PodMigration"

	lock, err := store.acquireSessionLock(ctx, "system", lockID, "workflow")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := lock.Release(ctx); err != nil {
			t.Errorf("release lock: %v", err)
		}
	}()

	lease, err := client.CoordinationV1().Leases("system").Get(
		ctx,
		SessionLockName(lockID),
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if got := lease.Labels[SessionKey]; got != "workflow" {
		t.Fatalf("session label=%q, want workflow", got)
	}
}

func TestConfigMapSessionStoreDeletesSessionLease(t *testing.T) {
	ctx := context.Background()
	store := NewConfigMapSessionStore(newSessionLeaseTestClient())

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
	client := newSessionLeaseTestClient()
	store := NewConfigMapSessionStore(client)

	lock, err := store.AcquireSessionLock(ctx, "system", "session-delete-fence")
	if err != nil {
		t.Fatal(err)
	}

	lease, err := client.CoordinationV1().
		Leases("system").
		Get(ctx, SessionLockName("session-delete-fence"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	replacement := lease.DeepCopy()

	replacement.Spec.HolderIdentity = new("new-holder")
	if _, err := client.CoordinationV1().
		Leases("system").
		Update(ctx, replacement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	deleteErr := lock.Delete(ctx)
	if domain.CategoryOf(deleteErr) != domain.ErrorConflict {
		t.Fatalf("delete error=%v", deleteErr)
	}

	current, err := client.CoordinationV1().
		Leases("system").
		Get(ctx, lease.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != "new-holder" {
		t.Fatalf("replacement holder=%v", current.Spec.HolderIdentity)
	}

	_ = lock.Release(ctx)
}

func TestSessionLeaseFencesReplacementWithCopiedOwnership(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*sessionLease) error
	}{
		{name: "renew", run: func(lock *sessionLease) error {
			err := lock.renew(context.Background())
			lock.cancel()
			<-lock.done
			return err
		}},
		{name: "release", run: func(lock *sessionLease) error { return lock.Release(context.Background()) }},
		{name: "delete", run: func(lock *sessionLease) error { return lock.Delete(context.Background()) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			const sessionID, holder = "session-replaced", "holder"

			replacement := &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "system",
					Name:      SessionLockName(sessionID),
					UID:       types.UID("replacement-lease-uid"),
					Labels: map[string]string{
						ManagedByLabel: ManagedByValue,
						SessionKey:     sessionID,
					},
				},
				Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr(holder)},
			}
			client := fake.NewClientset(replacement)

			lock := newSessionLease(
				context.Background(),
				client.CoordinationV1().Leases("system"),
				"system",
				replacement.Name,
				sessionID,
				holder,
				types.UID("original-lease-uid"),
			)
			if err := test.run(lock); domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			current, err := client.CoordinationV1().
				Leases("system").
				Get(context.Background(), replacement.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("replacement Lease was changed: %v", err)
			}

			if current.UID != replacement.UID || current.Spec.HolderIdentity == nil ||
				*current.Spec.HolderIdentity != holder {
				t.Fatalf("replacement Lease=%#v", current)
			}
		})
	}
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
	client.PrependReactor(
		"delete",
		"leases",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			deleteOptions = testutil.MustType[clienttesting.DeleteAction](
				t,
				action,
			).GetDeleteOptions()

			return true, nil, nil
		},
	)

	if err := store.DeleteSessionLease(ctx, "system", sessionID); err != nil {
		t.Fatal(err)
	}

	if deleteOptions.Preconditions == nil || deleteOptions.Preconditions.ResourceVersion == nil ||
		*deleteOptions.Preconditions.ResourceVersion != "lease-resource-version" {
		t.Fatalf("missing resourceVersion precondition: %#v", deleteOptions.Preconditions)
	}

	if deleteOptions.Preconditions.UID == nil ||
		*deleteOptions.Preconditions.UID != types.UID("lease-uid") {
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
		Spec: coordinationv1.LeaseSpec{HolderIdentity: new("new-holder")},
	})
	store := NewConfigMapSessionStore(client)
	client.PrependReactor(
		"delete",
		"leases",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			name := testutil.MustType[clienttesting.DeleteAction](t, action).GetName()

			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Group: coordinationv1.GroupName, Resource: "leases"},
				name,
				errors.New("lease changed after it was read"),
			)
		},
	)

	if err := store.DeleteSessionLease(
		ctx,
		"system",
		sessionID,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("delete race category=%s error=%v", domain.CategoryOf(err), err)
	}

	lease, err := client.CoordinationV1().
		Leases("system").
		Get(ctx, SessionLockName(sessionID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "new-holder" {
		t.Fatalf("replacement Lease was changed: %#v", lease.Spec.HolderIdentity)
	}
}

func TestSessionLeaseRenewFailureFencesContext(t *testing.T) {
	client := newSessionLeaseTestClient()
	client.PrependReactor(
		"update",
		"leases",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("injected lease API failure")
		},
	)
	store := NewConfigMapSessionStore(client).WithLeaseTiming(
		20*time.Millisecond,
		5*time.Millisecond,
	)

	lock, err := store.AcquireSessionLock(context.Background(), "system", "session-2")
	if err != nil {
		t.Fatal(err)
	}

	bound, cancelBound := lock.Bind(context.Background())
	defer cancelBound()

	select {
	case <-bound.Done():
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

func TestLeaseTimingClampsRenewalBeforeExpiry(t *testing.T) {
	store := NewConfigMapSessionStore(newSessionLeaseTestClient()).WithLeaseTiming(
		30*time.Second,
		45*time.Second,
	)

	duration, renewEvery := store.leaseTiming()
	if duration != 30*time.Second || renewEvery != 10*time.Second {
		t.Fatalf("lease timing duration=%s renewEvery=%s", duration, renewEvery)
	}

	duration, renewEvery = normalizeLeaseTiming(20*time.Millisecond, 50*time.Millisecond)
	if duration != 20*time.Millisecond || renewEvery != 20*time.Millisecond/3 {
		t.Fatalf("normalized timing duration=%s renewEvery=%s", duration, renewEvery)
	}
}

func TestSessionLeaseCancellationStopsInFlightRenewal(t *testing.T) {
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
				UID:       types.UID("lease-uid"),
				Labels: map[string]string{
					ManagedByLabel: ManagedByValue,
					SessionKey:     sessionID,
				},
			},
			Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr(holder)},
		},
	}

	lock := newSessionLeaseWithTiming(
		context.Background(),
		leaseClient,
		"system",
		leaseClient.lease.Name,
		sessionID,
		holder,
		leaseClient.lease.UID,
		30*time.Second,
		5*time.Millisecond,
	)
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
	now := metav1.NewMicroTime(time.Now().Add(-time.Minute))
	duration := int32(1)
	client := fake.NewClientset(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "system",
			Name:      SessionLockName("session-expired"),
			UID:       types.UID("expired-lease-uid"),
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     "session-expired",
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       new("abandoned-holder"),
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

func TestAcquireSessionLockRejectsMissingLeaseUID(t *testing.T) {
	store := NewConfigMapSessionStore(fake.NewClientset())

	lock, err := store.AcquireSessionLock(context.Background(), "system", "session-missing-uid")
	if lock != nil || domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("lock=%v category=%s error=%v", lock, domain.CategoryOf(err), err)
	}
}
