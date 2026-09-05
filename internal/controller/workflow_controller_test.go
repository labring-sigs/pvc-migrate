package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestCacheReadinessDoesNotWaitForLeaderElection(t *testing.T) {
	readiness := &cacheReadiness{}

	if readiness.NeedLeaderElection() {
		t.Fatal("cache readiness must run before leader election")
	}

	if err := readiness.Check(nil); err == nil {
		t.Fatal("cache readiness started ready")
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- readiness.Start(ctx) }()

	deadline := time.Now().Add(time.Second)
	for readiness.Check(nil) != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if err := readiness.Check(nil); err != nil {
		t.Fatalf("cache readiness did not become ready: %v", err)
	}

	cancel()

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if err := readiness.Check(nil); err == nil {
		t.Fatal("cache readiness remained ready after shutdown")
	}
}

func TestWorkflowEventPredicateAllowsDeletionTimestampTransition(t *testing.T) {
	base := &v1alpha1.Copy{ObjectMeta: metav1.ObjectMeta{
		Name: "workflow", Namespace: "system", Generation: 1,
	}}

	tests := []struct {
		name string
		old  *v1alpha1.Copy
		new  *v1alpha1.Copy
		want bool
	}{
		{name: "status only", old: base, new: base.DeepCopy()},
		{name: "generation", old: base, new: func() *v1alpha1.Copy {
			updated := base.DeepCopy()
			updated.Generation++
			return updated
		}(), want: true},
		{name: "deletion starts", old: base, new: func() *v1alpha1.Copy {
			updated := base.DeepCopy()
			updated.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
			return updated
		}(), want: true},
		{name: "failed workflow resumes", old: func() *v1alpha1.Copy {
			updated := base.DeepCopy()
			updated.Status.Phase = v1alpha1.WorkflowPhase(domain.PhaseFailed)
			return updated
		}(), new: func() *v1alpha1.Copy {
			updated := base.DeepCopy()
			updated.Status.Phase = v1alpha1.WorkflowPhase(domain.PhasePlanned)
			return updated
		}(), want: true},
		{name: "active status remains filtered", old: func() *v1alpha1.Copy {
			updated := base.DeepCopy()
			updated.Status.Phase = v1alpha1.WorkflowPhase(domain.PhasePlanned)
			return updated
		}(), new: func() *v1alpha1.Copy {
			updated := base.DeepCopy()
			updated.Status.Phase = v1alpha1.WorkflowPhase(domain.PhaseReserving)
			return updated
		}()},
		{name: "already deleting", old: func() *v1alpha1.Copy {
			updated := base.DeepCopy()
			updated.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
			return updated
		}(), new: func() *v1alpha1.Copy {
			updated := base.DeepCopy()
			updated.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
			return updated
		}()},
	}

	predicate := workflowEventPredicate()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := predicate.Update(event.UpdateEvent{ObjectOld: test.old, ObjectNew: test.new})
			if got != test.want {
				t.Fatalf("update admitted=%t want=%t", got, test.want)
			}
		})
	}
}

func TestReconcileDeletingWorkflowReleasesOnlyTerminatingNamespace(t *testing.T) {
	tests := []struct {
		name        string
		namespace   *corev1.Namespace
		request     reconcile.Request
		kind        domain.ControllerKind
		wantDeleted bool
	}{
		{
			name:      "active namespace keeps explicit cleanup protection",
			namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
			request: reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: "system",
				Name:      "workflow",
			}},
			kind: domain.ControllerKindCopy,
		},
		{
			name: "terminating namespace releases protection",
			namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
				Name:              "system",
				DeletionTimestamp: func() *metav1.Time { value := metav1.Now(); return &value }(),
			}},
			request: reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: "system",
				Name:      "workflow",
			}},
			kind:        domain.ControllerKindCopy,
			wantDeleted: true,
		},
		{
			name: "missing namespace releases stale protection",
			request: reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: "system",
				Name:      "workflow",
			}},
			kind:        domain.ControllerKindCopy,
			wantDeleted: true,
		},
		{
			name:      "cluster workflow keeps explicit cleanup protection while namespace is active",
			namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
			request:   reconcile.Request{NamespacedName: types.NamespacedName{Name: "workflow"}},
			kind:      domain.ControllerKindClusterCopy,
		},
		{
			name:        "cluster workflow releases stale protection when namespace is missing",
			request:     reconcile.Request{NamespacedName: types.NamespacedName{Name: "workflow"}},
			kind:        domain.ControllerKindClusterCopy,
			wantDeleted: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &runnerSessionStore{}

			reconciler := NewWorkflowReconciler(nil, store)
			if test.namespace != nil {
				reconciler.WithKubernetesClient(clientfake.NewSimpleClientset(test.namespace))
			} else {
				reconciler.WithKubernetesClient(clientfake.NewSimpleClientset())
			}

			session := newRunnerSession("workflow")
			session.Deleting = true

			session.BackendResource = test.kind
			if err := reconciler.reconcileDeletingWorkflow(
				context.Background(),
				test.request,
				session,
			); err != nil {
				t.Fatal(err)
			}

			if got := len(store.deleted) == 1; got != test.wantDeleted {
				t.Fatalf("workflow deleted=%t, want %t", got, test.wantDeleted)
			}
		})
	}
}

func TestStartManagerRequiresPinnedToolImage(t *testing.T) {
	for name, image := range map[string]string{
		"missing":    "",
		"whitespace": "   ",
		"digest":     "registry.example/pvc-migrate@sha256:abc",
		"untagged":   "registry.example/pvc-migrate",
	} {
		t.Run(name, func(t *testing.T) {
			err := StartManager(
				context.Background(),
				&rest.Config{},
				nil,
				nil,
				ManagerOptions{
					Namespace:        "controller-system",
					TrustedToolImage: image,
				},
			)
			if err == nil {
				t.Fatal("manager accepted an untrusted tool image")
			}
		})
	}
}

func TestValidateTrustedToolImageAcceptsOnlyPinnedReferences(t *testing.T) {
	if err := ValidateTrustedToolImage("registry.example/pvc-migrate:v1"); err != nil {
		t.Fatalf("valid image rejected: %v", err)
	}

	for _, image := range []string{"", "   ", "registry.example/pvc-migrate", "registry.example/pvc-migrate@sha256:abc"} {
		if err := ValidateTrustedToolImage(image); err == nil {
			t.Fatalf("image %q accepted", image)
		}
	}
}

func TestRunnerWithKubeconfigTrimsAndStoresConnection(t *testing.T) {
	runner := NewRunner(nil, nil, "system").WithKubeconfig(
		"  /etc/pvc-migrate/kubeconfig  ",
		"  tenant-context  ",
	)

	if runner.kubeconfigPath != "/etc/pvc-migrate/kubeconfig" {
		t.Fatalf("kubeconfig path=%q", runner.kubeconfigPath)
	}

	if runner.kubeContext != "tenant-context" {
		t.Fatalf("kube context=%q", runner.kubeContext)
	}
}

func TestWorkflowSpecMutationError(t *testing.T) {
	for name, session := range map[string]*domain.Session{
		"nil": nil,
		"unobserved": func() *domain.Session {
			s := newRunnerSession("unobserved")
			s.Generation = 2
			return s
		}(),
		"same generation": func() *domain.Session {
			s := newRunnerSession("same")
			s.Generation = 3
			s.Status.ObservedGeneration = 3
			return s
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := workflowSpecMutationError(session); err != nil {
				t.Fatalf("unexpected mutation error: %v", err)
			}
		})
	}

	session := newRunnerSession("changed")
	session.Generation = 4

	session.Status.ObservedGeneration = 3
	if err := workflowSpecMutationError(session); err == nil {
		t.Fatal("spec generation drift was accepted")
	}
}

func TestWorkflowReconcilerKeepsBusinessFailureOutOfReconcilerError(t *testing.T) {
	session := newRunnerSession("controller-business-failure")
	session.Status.Phase = domain.PhaseReserved
	session.Status.ObservedGeneration = 1
	session.Generation = 1
	store := &runnerSessionStore{
		listed: session,
		latest: cloneRunnerSession(session),
		lock:   &runnerSessionLock{},
	}

	reconciler := NewWorkflowReconciler(&recordingWorkflowResumer{}, store)
	request := reconcile.Request{NamespacedName: types.NamespacedName{
		Namespace: session.Spec.SessionNamespace,
		Name:      session.ID,
	}}
	kindReconciler := &kindWorkflowReconciler{
		parent: reconciler,
		kind:   domain.ControllerKindCopy,
	}
	result := reconcileWorkflowForTest(t, kindReconciler, request)

	if result != (reconcile.Result{}) {
		t.Fatalf("result=%#v, want no requeue after terminal failure", result)
	}

	if store.latest.Status.Phase != domain.PhaseFailed {
		t.Fatalf("phase=%s, want %s", store.latest.Status.Phase, domain.PhaseFailed)
	}
}

func TestWorkflowReconcilerRequeuesSessionLockContention(t *testing.T) {
	session := newRunnerSession("controller-lock-contention")
	session.Status.Phase = domain.PhaseReserved
	session.Status.ObservedGeneration = 1
	session.Generation = 1
	store := &runnerSessionStore{
		listed: session,
		latest: cloneRunnerSession(session),
		lock:   &runnerSessionLock{},
	}
	reconciler := NewWorkflowReconciler(
		&errorWorkflowResumer{err: domain.WrapError(
			domain.ErrorConflict,
			"acquire session lock",
			"session is already being changed",
			kube.ErrSessionLockContention,
		)},
		store,
	)
	reconciler.requeueAfter = 250 * time.Millisecond

	request := reconcile.Request{NamespacedName: types.NamespacedName{
		Namespace: session.Spec.SessionNamespace,
		Name:      session.ID,
	}}

	result, err := (&kindWorkflowReconciler{
		parent: reconciler,
		kind:   domain.ControllerKindCopy,
	}).Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("lock contention escaped reconcile: %v", err)
	}

	if result.RequeueAfter != 250*time.Millisecond {
		t.Fatalf("requeueAfter=%s, want 250ms", result.RequeueAfter)
	}

	if len(store.updates) != 0 {
		t.Fatalf("lock contention checkpointed as failure: %d updates", len(store.updates))
	}
}

func TestWorkflowReconcilerRequeuesFailureCheckpointLockContention(t *testing.T) {
	session := newRunnerSession("controller-failure-lock-contention")
	session.Status.Phase = domain.PhaseReserved
	session.Status.ObservedGeneration = 1
	session.Generation = 1
	store := &runnerSessionStore{
		listed: session,
		latest: cloneRunnerSession(session),
		acquireErr: domain.WrapError(
			domain.ErrorConflict,
			"acquire session lock",
			"session is already being changed",
			kube.ErrSessionLockContention,
		),
	}
	reconciler := NewWorkflowReconciler(
		&errorWorkflowResumer{err: errors.New("destination PVC is unavailable")},
		store,
	)
	reconciler.requeueAfter = 250 * time.Millisecond

	request := reconcile.Request{NamespacedName: types.NamespacedName{
		Namespace: session.Spec.SessionNamespace,
		Name:      session.ID,
	}}

	result, err := (&kindWorkflowReconciler{
		parent: reconciler,
		kind:   domain.ControllerKindCopy,
	}).Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("failure checkpoint lock contention escaped reconcile: %v", err)
	}

	if result.RequeueAfter != 250*time.Millisecond {
		t.Fatalf("requeueAfter=%s, want 250ms", result.RequeueAfter)
	}

	if len(store.updates) != 0 {
		t.Fatalf(
			"failure checkpoint updated session despite lock contention: %d updates",
			len(store.updates),
		)
	}
}

func reconcileWorkflowForTest(
	t *testing.T,
	reconciler *kindWorkflowReconciler,
	request reconcile.Request,
) reconcile.Result {
	t.Helper()

	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("business failure escaped reconcile: %v", err)
	}

	return result
}

type errorWorkflowResumer struct {
	err error
}

func (r *errorWorkflowResumer) ResumeReserve(context.Context, *domain.Session) error {
	return r.err
}

func (r *errorWorkflowResumer) ResumeOfflineMigration(context.Context, *domain.Session) error {
	return r.err
}

func (r *errorWorkflowResumer) ResumePodMigration(context.Context, *domain.Session) error {
	return r.err
}

func (r *errorWorkflowResumer) ResumeCopy(context.Context, *domain.Session) error {
	return r.err
}

func (r *errorWorkflowResumer) ResumeRename(context.Context, *domain.Session) error {
	return r.err
}

func (r *errorWorkflowResumer) ResumeMove(context.Context, *domain.Session) error {
	return r.err
}

func TestInitializeUnobservedStatus(t *testing.T) {
	session := newRunnerSession("unobserved-status")
	session.Status.Phase = domain.PhaseCompleted
	session.Status.ObservedGeneration = 0
	store := &runnerSessionStore{latest: session}

	if err := initializeUnobservedStatus(
		context.Background(), store, session,
	); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhasePlanned {
		t.Fatalf("phase=%s, want %s", session.Status.Phase, domain.PhasePlanned)
	}

	if len(store.updates) != 1 {
		t.Fatalf("updates=%d, want one status checkpoint", len(store.updates))
	}
}

func TestWorkflowReconcilerFiltersInstalledKinds(t *testing.T) {
	all := NewWorkflowReconciler(nil, nil)
	if !all.supportsKind("Migration") || !all.supportsKind("Backup") {
		t.Fatal("default reconciler must support the complete workflow set")
	}

	partial := NewWorkflowReconciler(nil, nil).WithSupportedKinds([]domain.ControllerKind{
		domain.ControllerKindMigration,
		domain.ControllerKindCopy,
	})
	if !partial.supportsKind("Migration") || !partial.supportsKind("Copy") {
		t.Fatal("partial reconciler omitted an installed kind")
	}

	if partial.supportsKind("Backup") {
		t.Fatal("partial reconciler included a missing kind")
	}
}

func TestKubeBlocksProtocolMappings(t *testing.T) {
	if got := kubeBlocksClusterField(
		"apps.kubeblocks.io/v1alpha1",
	); got != kubeBlocksFieldClusterRef {
		t.Fatalf("cluster field=%q", got)
	}

	if got := kubeBlocksClusterField(kubeBlocksOpsAPIVersion); got != kubeBlocksFieldClusterName {
		t.Fatalf("component-scoped cluster field=%q", got)
	}

	for _, phase := range []kubeBlocksPhase{
		kubeBlocksPhaseFailed,
		kubeBlocksPhaseCancelled,
		kubeBlocksPhaseAborted,
	} {
		if !kubeBlocksOpsFailed(string(phase)) {
			t.Fatalf("phase %q must be retryable failure", phase)
		}
	}

	if kubeBlocksOpsFailed(string(kubeBlocksPhaseSucceeded)) {
		t.Fatal("successful phase classified as failure")
	}
}
