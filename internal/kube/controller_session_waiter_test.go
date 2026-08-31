package kube

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestControllerSessionWaiterReturnsTerminalWatchUpdate(t *testing.T) {
	initial := newControllerWaitTestSession(domain.PhasePlanned, "planned")
	initialObject := controllerWaitTestObject(t, initial)
	completed := newControllerWaitTestSession(domain.PhaseCompleted, "completed")
	completedObject := controllerWaitTestObject(t, completed)
	client := newControllerWaitDynamicClient(initialObject)
	watcher := watch.NewRaceFreeFake()
	watchStarted := make(chan struct{})
	client.PrependWatchReactor("migrations", func(action clienttesting.Action) (
		bool, watch.Interface, error,
	) {
		watchAction, ok := action.(clienttesting.WatchAction)
		if !ok {
			t.Errorf("watch action has type %T", action)

			return true, watcher, nil
		}

		restrictions := watchAction.GetWatchRestrictions()
		if got := restrictions.Fields.String(); got != "metadata.name=workflow-test" {
			t.Errorf("field selector=%q", got)
		}

		close(watchStarted)

		return true, watcher, nil
	})

	result := make(chan controllerWaitResult, 1)
	go waitForControllerTestResult(
		context.Background(), NewControllerSessionWaiter(client), initial, result,
	)

	<-watchStarted
	watcher.Modify(completedObject)

	got := <-result
	if got.err != nil || got.session == nil || got.session.Status.Phase != domain.PhaseCompleted {
		t.Fatalf("session=%#v error=%v", got.session, got.err)
	}
}

func TestControllerSessionWaiterReconnectsAfterClosedWatch(t *testing.T) {
	initial := newControllerWaitTestSession(domain.PhasePlanned, "planned")
	initialObject := controllerWaitTestObject(t, initial)
	completed := newControllerWaitTestSession(domain.PhaseCompleted, "completed")
	completedObject := controllerWaitTestObject(t, completed)
	client := newControllerWaitDynamicClient(initialObject)
	first := watch.NewRaceFreeFake()
	second := watch.NewRaceFreeFake()
	watchStarted := make(chan int, 2)
	calls := &atomic.Int32{}
	client.PrependWatchReactor("migrations", func(clienttesting.Action) (
		bool, watch.Interface, error,
	) {
		call := calls.Add(1)
		watchStarted <- int(call)

		if call == 1 {
			return true, first, nil
		}

		return true, second, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := make(chan controllerWaitResult, 1)
	go waitForControllerTestResult(ctx, NewControllerSessionWaiter(client), initial, result)

	if call := <-watchStarted; call != 1 {
		t.Fatalf("first watch call=%d", call)
	}

	first.Stop()

	if call := <-watchStarted; call != 2 {
		t.Fatalf("second watch call=%d", call)
	}

	second.Modify(completedObject)

	got := <-result
	if got.err != nil || got.session == nil || got.session.Status.Phase != domain.PhaseCompleted {
		t.Fatalf("session=%#v calls=%d error=%v", got.session, calls.Load(), got.err)
	}
}

func TestControllerSessionWaiterRelistsAfterExpiredWatch(t *testing.T) {
	initial := newControllerWaitTestSession(domain.PhasePlanned, "planned")
	initialObject := controllerWaitTestObject(t, initial)
	completed := newControllerWaitTestSession(domain.PhaseCompleted, "completed")
	completedObject := controllerWaitTestObject(t, completed)
	client := newControllerWaitDynamicClient(initialObject)
	first := watch.NewRaceFreeFake()
	second := watch.NewRaceFreeFake()
	watchStarted := make(chan int, 2)
	calls := &atomic.Int32{}
	client.PrependWatchReactor("migrations", func(clienttesting.Action) (
		bool, watch.Interface, error,
	) {
		call := calls.Add(1)
		watchStarted <- int(call)

		if call == 1 {
			return true, first, nil
		}

		return true, second, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := make(chan controllerWaitResult, 1)
	go waitForControllerTestResult(ctx, NewControllerSessionWaiter(client), initial, result)

	<-watchStarted
	first.Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Reason: metav1.StatusReasonExpired,
		Code:   410,
	})

	if call := <-watchStarted; call != 2 {
		t.Fatalf("watch call after expiration=%d", call)
	}

	second.Modify(completedObject)

	got := <-result
	if got.err != nil || got.session == nil || got.session.Status.Phase != domain.PhaseCompleted {
		t.Fatalf("session=%#v calls=%d error=%v", got.session, calls.Load(), got.err)
	}
}

func TestControllerSessionWaiterReportsDeletion(t *testing.T) {
	initial := newControllerWaitTestSession(domain.PhasePlanned, "planned")
	initialObject := controllerWaitTestObject(t, initial)
	client := newControllerWaitDynamicClient(initialObject)
	watcher := watch.NewRaceFreeFake()
	watchStarted := make(chan struct{})
	client.PrependWatchReactor("migrations", func(clienttesting.Action) (
		bool, watch.Interface, error,
	) {
		close(watchStarted)

		return true, watcher, nil
	})

	result := make(chan controllerWaitResult, 1)
	go waitForControllerTestResult(
		context.Background(), NewControllerSessionWaiter(client), initial, result,
	)

	<-watchStarted
	watcher.Delete(initialObject)

	got := <-result
	if domain.CategoryOf(got.err) != domain.ErrorConflict ||
		!strings.Contains(got.err.Error(), "deleted before completion") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(got.err), got.err)
	}
}

func TestControllerSessionWaiterHonorsDeadline(t *testing.T) {
	initial := newControllerWaitTestSession(domain.PhasePlanned, "planned")
	initialObject := controllerWaitTestObject(t, initial)
	client := newControllerWaitDynamicClient(initialObject)
	watcher := watch.NewRaceFreeFake()
	client.PrependWatchReactor("migrations", func(clienttesting.Action) (
		bool, watch.Interface, error,
	) {
		return true, watcher, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := NewControllerSessionWaiter(client).Wait(
		ctx,
		initial,
		func(session *domain.Session) (bool, error) {
			return session.Status.Phase == domain.PhaseCompleted, nil
		},
	)
	if domain.CategoryOf(err) != domain.ErrorTimeout {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

type controllerWaitResult struct {
	session *domain.Session
	err     error
}

func waitForControllerTestResult(
	ctx context.Context,
	waiter *ControllerSessionWaiter,
	initial *domain.Session,
	result chan<- controllerWaitResult,
) {
	session, err := waiter.Wait(
		ctx,
		initial,
		func(update *domain.Session) (bool, error) {
			return update.Status.Phase == domain.PhaseCompleted, nil
		},
	)
	result <- controllerWaitResult{session: session, err: err}
}

func newControllerWaitTestSession(phase domain.Phase, message string) *domain.Session {
	spec := domain.NewOfflineMigrationSessionSpec(
		domain.SessionCommon{
			SourceNamespace:      "app",
			TemporaryNamespace:   "system",
			DestinationNamespace: "app",
			SessionNamespace:     "system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Name: "data", Namespace: "app", UID: "source-pvc-uid",
					},
					SourcePV:      domain.ObjectReference{Name: "source-pv", UID: "source-pv-uid"},
					SourcePVCSpec: corev1.PersistentVolumeClaimSpec{},
					DestinationPVC: domain.ObjectReference{
						Name: "destination", Namespace: "system",
					},
				},
			},
		},
		domain.SessionWorkflowOptions{},
	)
	session := domain.NewSession("workflow-test", spec, time.Now())
	session.Backend = SessionBackendCRD
	session.Status.Phase = phase
	session.Status.Message = message

	return session
}

func controllerWaitTestObject(
	t *testing.T,
	session *domain.Session,
) *unstructured.Unstructured {
	t.Helper()

	object := sessionObjectFor(session)
	object.SetUID("workflow-uid")
	object.SetGeneration(1)
	object.SetResourceVersion("1")

	if !setWorkflowStatus(object, session.Spec, session.Status) {
		t.Fatal("failed to set workflow status")
	}

	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		t.Fatal(err)
	}

	unstructuredObject := &unstructured.Unstructured{Object: content}
	if _, err := decodeControllerWatchObject(
		unstructuredObject,
		workflowCRDKind(session.Spec.Type),
	); err != nil {
		t.Fatalf("test workflow object does not decode: %v", err)
	}

	return unstructuredObject
}

func newControllerWaitDynamicClient(
	object *unstructured.Unstructured,
) *fake.FakeDynamicClient {
	gvr := schema.GroupVersionResource{
		Group: domain.SessionAPIGroup, Version: "v1alpha1", Resource: domain.MigrationResource,
	}

	return fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{gvr: "MigrationList"},
		object,
	)
}
