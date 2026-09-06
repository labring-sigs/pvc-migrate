package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

func TestDeletingWorkflowRequiresSuccessfulFinalization(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(strconv.FormatBool(blocked), func(t *testing.T) {
			session := newRunnerSession("workflow")
			session.Generation = 1
			session.Status.ObservedGeneration = 1
			session.Deleting = true
			session.BackendResource = domain.ControllerKindClusterCopy
			store := &runnerSessionStore{latest: session}

			finalizer := &deletionWorkflowService{}
			if blocked {
				finalizer.err = errors.New("injected cleanup failure")
			}

			reconciler := NewWorkflowReconciler(finalizer, store)

			err := reconciler.reconcileDeletingWorkflow(t.Context(), reconcile.Request{}, session)
			if (err != nil) != blocked || finalizer.calls != 1 {
				t.Fatalf("error=%v calls=%d", err, finalizer.calls)
			}

			if len(store.deleted) != 0 {
				t.Fatal("reconciler bypassed service cleanup")
			}

			if blocked &&
				(len(session.Status.Conditions) == 0 || session.Status.Conditions[len(session.Status.Conditions)-1].Type != "DeletionBlocked") {
				t.Fatal("cleanup failure was not visible")
			}
		})
	}
}

type deletionWorkflowService struct {
	recordingWorkflowResumer
	calls int
	err   error
}

func (s *deletionWorkflowService) FinalizeDeletedWorkflow(context.Context, *domain.Session) error {
	s.calls++
	return s.err
}

func TestFailedWorkflowWaitsForResumeEventWithoutPolling(t *testing.T) {
	session := newRunnerSession("resume-event")
	session.Generation = 1
	session.Status.ObservedGeneration = 1
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseWarmCopying
	store := &runnerSessionStore{latest: session, lock: &runnerSessionLock{}}
	resumer := &recordingWorkflowResumer{}
	reconciler := NewWorkflowReconciler(resumer, store)
	request := reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "system", Name: session.ID},
	}

	result, err := reconciler.reconcile(t.Context(), request, domain.ControllerKindCopy)
	if err != nil || result != (reconcile.Result{}) || resumer.called != "" {
		t.Fatalf(
			"failed workflow should be idle: result=%+v called=%q err=%v",
			result,
			resumer.called,
			err,
		)
	}

	before := &v1alpha1.Copy{
		Status: v1alpha1.CopyStatusFromDomain(session.Status, session.Spec.Volumes),
	}
	if err := session.Reactivate("controller resume requested", time.Now()); err != nil {
		t.Fatal(err)
	}

	after := &v1alpha1.Copy{
		Status: v1alpha1.CopyStatusFromDomain(session.Status, session.Spec.Volumes),
	}
	if !workflowEventPredicate().Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}) {
		t.Fatal("status-only resume did not enqueue the workflow")
	}

	if _, err := reconciler.reconcile(t.Context(), request, domain.ControllerKindCopy); err != nil {
		t.Fatal(err)
	}

	if resumer.called != "copy" {
		t.Fatalf("resume event dispatched %q, want copy", resumer.called)
	}
}

func TestResumeEventsForEveryWorkflowKind(t *testing.T) {
	for _, workflow := range domain.ControllerWorkflows() {
		for _, kind := range []domain.ControllerKind{workflow.Kind, workflow.ClusterKind} {
			if kind == "" {
				continue
			}

			t.Run(string(kind), func(t *testing.T) {
				before := kube.WorkflowObjectForKind(kind)
				if err := json.Unmarshal(
					[]byte(`{"status":{"phase":"Failed","resumeFrom":"WarmCopying"}}`),
					before,
				); err != nil {
					t.Fatal(err)
				}

				for _, phase := range []domain.Phase{
					domain.PhaseWarmCopying, domain.PhaseRollingBack, domain.PhaseAborting, domain.PhaseFailed, "",
				} {
					after := kube.WorkflowObjectForKind(kind)

					payload, err := json.Marshal(
						map[string]any{"status": map[string]any{"phase": phase}},
					)
					if err != nil {
						t.Fatal(err)
					}

					if err := json.Unmarshal(payload, after); err != nil {
						t.Fatal(err)
					}

					want := phase == domain.PhaseWarmCopying
					if got := workflowEventPredicate().Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}); got != want {
						t.Fatalf("Failed -> %s: enqueued=%v want=%v", phase, got, want)
					}
				}
			})
		}
	}
}

func TestDeletionEventCancelsOnlyMatchingUID(t *testing.T) {
	r := NewWorkflowReconciler(nil, nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	uid := types.UID("running-workflow")
	r.activeWorkflows.Store(uid, cancel)
	before := &v1alpha1.Copy{ObjectMeta: metav1.ObjectMeta{UID: uid}}
	after := before.DeepCopy()
	now := metav1.Now()
	after.DeletionTimestamp = &now
	p := workflowEventPredicate(r.cancelWorkflow)
	replacement := after.DeepCopy()
	replacement.UID = "replacement"
	p.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: replacement})

	if ctx.Err() != nil {
		t.Fatal("replacement UID canceled existing worker")
	}

	if !p.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}) ||
		ctx.Err() != context.Canceled {
		t.Fatal("deletion did not cancel and enqueue matching worker")
	}
}

type blockingCopyService struct {
	recordingWorkflowResumer
	started chan struct{}
}

func (s *blockingCopyService) ResumeCopy(ctx context.Context, _ *domain.Session) error {
	close(s.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestDeleteInterruptsRunningCopyWithoutFailingCheckpoint(t *testing.T) {
	session := newRunnerSession("cancel-copy")
	session.Generation = 1
	session.Status.ObservedGeneration = 1
	session.Status.Phase = domain.PhaseWarmCopying
	session.BackendUID = "running-uid"
	store := &runnerSessionStore{latest: session}
	service := &blockingCopyService{started: make(chan struct{})}
	r := NewWorkflowReconciler(service, store)

	done := make(chan error, 1)
	go func() {
		_, err := r.reconcile(
			t.Context(),
			reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "system", Name: session.ID},
			},
			domain.ControllerKindCopy,
		)
		done <- err
	}()

	select {
	case <-service.started:
	case <-time.After(5 * time.Second):
		t.Fatal("copy did not start")
	}

	before := &v1alpha1.Copy{ObjectMeta: metav1.ObjectMeta{UID: session.BackendUID}}
	after := before.DeepCopy()
	now := metav1.Now()
	after.DeletionTimestamp = &now
	workflowEventPredicate(
		r.cancelWorkflow,
	).Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after})

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("copy ignored cancellation")
	}

	if len(store.updates) != 0 || session.Status.Phase != domain.PhaseWarmCopying {
		t.Fatal("cancellation overwrote recovery checkpoint")
	}
}

func TestDeletionRetriesCleanupAfterFailure(t *testing.T) {
	session := newRunnerSession("deletion-retry")
	session.Generation = 1
	session.Status.ObservedGeneration = 1
	session.Deleting = true
	session.BackendResource = domain.ControllerKindClusterCopy
	store := &runnerSessionStore{latest: session}
	service := &deletionWorkflowService{err: errors.New("cleanup blocked")}
	r := NewWorkflowReconciler(service, store)

	request := reconcile.Request{NamespacedName: types.NamespacedName{Name: session.ID}}
	if _, err := r.reconcile(t.Context(), request, session.BackendResource); err == nil {
		t.Fatal("expected failure")
	}

	service.err = nil
	if _, err := r.reconcile(t.Context(), request, session.BackendResource); err != nil {
		t.Fatal(err)
	}

	if service.calls != 2 {
		t.Fatalf("cleanup calls=%d", service.calls)
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

func TestDeletingWorkflowGenerationFence(t *testing.T) {
	for _, test := range []struct {
		name       string
		generation int64
		observed   int64
		condition  string
		wantError  bool
	}{
		{name: "initial API deletion bump", generation: 2, observed: 1},
		{name: "checkpointed deletion", generation: 2, observed: 2, condition: "Deleting"},
		{name: "spec changed before deletion", generation: 3, observed: 1, wantError: true},
		{name: "spec changed after deletion checkpoint", generation: 3, observed: 2, condition: "Deleting", wantError: true},
		{name: "spec changed after blocked checkpoint", generation: 3, observed: 2, condition: "DeletionBlocked", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := newRunnerSession("deletion-generation")
			session.Deleting = true
			session.Generation = test.generation

			session.Status.ObservedGeneration = test.observed
			if test.condition != "" {
				session.SetCondition(domain.Condition{Type: test.condition})
			}

			if err := workflowSpecMutationError(session); (err != nil) != test.wantError {
				t.Fatalf("error=%v", err)
			}
		})
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
