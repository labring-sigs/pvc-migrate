package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func planningRetryStatus(
	category domain.ErrorCategory,
	startedAt, updatedAt time.Time,
) *v1alpha1.WorkflowStatus {
	return &v1alpha1.WorkflowStatus{
		Phase:         domain.PhaseFailed,
		ResumeFrom:    domain.PhasePlanned,
		ErrorCategory: string(category),
		StartedAt:     metav1.NewTime(startedAt),
		UpdatedAt:     metav1.NewTime(updatedAt),
	}
}

func TestEvaluatePlanningRetrySchedule(t *testing.T) {
	now := time.Now()

	for _, test := range []struct {
		name       string
		status     *v1alpha1.WorkflowStatus
		wantRetry  bool
		wantDue    bool
		wantDelay  time.Duration
		wantExpire bool
	}{
		{
			name:      "fresh failure waits for the floor",
			status:    planningRetryStatus(domain.ErrorPrecondition, now, now),
			wantRetry: true,
			wantDelay: planningRetryFloor,
		},
		{
			name:      "attempt older than the interval is due now",
			status:    planningRetryStatus(domain.ErrorKubernetes, now.Add(-planningRetryFloor-time.Second), now.Add(-planningRetryFloor)),
			wantRetry: true,
			wantDue:   true,
		},
		{
			name:      "backoff grows with workflow age",
			status:    planningRetryStatus(domain.ErrorPrecondition, now.Add(-2*time.Minute), now.Add(-10*time.Second)),
			wantRetry: true,
			wantDelay: 20 * time.Second,
		},
		{
			name:      "backoff is capped near the window edge by the remaining time",
			status:    planningRetryStatus(domain.ErrorPrecondition, now.Add(-planningRetryWindow+30*time.Second), now.Add(-5*time.Second)),
			wantRetry: true,
			wantDelay: 25 * time.Second,
		},
		{
			name:       "window elapsed",
			status:     planningRetryStatus(domain.ErrorPrecondition, now.Add(-planningRetryWindow-time.Minute), now.Add(-planningRetryCap)),
			wantRetry:  true,
			wantExpire: true,
		},
		{
			name:   "internal failures never retry",
			status: planningRetryStatus(domain.ErrorInternal, now, now),
		},
		{
			name:   "validation failures never retry",
			status: planningRetryStatus(domain.ErrorValidation, now, now),
		},
		{
			name:   "conflict failures never retry",
			status: planningRetryStatus(domain.ErrorConflict, now, now),
		},
		{
			name:   "execution failures never retry",
			status: &v1alpha1.WorkflowStatus{Phase: domain.PhaseFailed, ResumeFrom: domain.PhaseFinalSyncing, ErrorCategory: string(domain.ErrorPrecondition), StartedAt: metav1.NewTime(now), UpdatedAt: metav1.NewTime(now)},
		},
		{
			name:   "missing started timestamp never retries",
			status: &v1alpha1.WorkflowStatus{Phase: domain.PhaseFailed, ResumeFrom: domain.PhasePlanned, ErrorCategory: string(domain.ErrorPrecondition), UpdatedAt: metav1.NewTime(now)},
		},
		{
			name:   "planned workflows never retry",
			status: &v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned, ResumeFrom: domain.PhasePlanned, ErrorCategory: string(domain.ErrorPrecondition), StartedAt: metav1.NewTime(now), UpdatedAt: metav1.NewTime(now)},
		},
		{
			name:   "nil status never retries",
			status: nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision := evaluatePlanningRetry(test.status, now)

			if decision.retryable != test.wantRetry {
				t.Fatalf("retryable = %v, want %v", decision.retryable, test.wantRetry)
			}

			if decision.due != test.wantDue {
				t.Fatalf("due = %v, want %v", decision.due, test.wantDue)
			}

			if decision.expired != test.wantExpire {
				t.Fatalf("expired = %v, want %v", decision.expired, test.wantExpire)
			}

			if decision.delay != test.wantDelay {
				t.Fatalf("delay = %v, want %v", decision.delay, test.wantDelay)
			}

			if got := PlanningRetryActive(
				test.status,
				now,
			); got != (test.wantRetry && !test.wantExpire) {
				t.Fatalf(
					"PlanningRetryActive = %v, want %v",
					got,
					test.wantRetry && !test.wantExpire,
				)
			}
		})
	}
}

func TestRequeuePlanningFailureDelayAfterFreshAttempt(t *testing.T) {
	now := time.Now()
	status := planningRetryStatus(domain.ErrorPrecondition, now, now)

	delay, retry := requeuePlanningFailureDelay(status, now)
	if !retry {
		t.Fatal("fresh retryable failure did not schedule a requeue")
	}

	if delay < planningRetryFloor || delay > planningRetryCap {
		t.Fatalf("delay = %v, want within [%v, %v]", delay, planningRetryFloor, planningRetryCap)
	}

	_, retry = requeuePlanningFailureDelay(planningRetryStatus(domain.ErrorInternal, now, now), now)
	if retry {
		t.Fatal("internal failure scheduled a requeue")
	}
}

func TestRecordPlanningOutcomeResetsWindowOnSpecCorrection(t *testing.T) {
	first := metav1.NewTime(time.Now().Add(-planningRetryWindow))
	status := &v1alpha1.WorkflowStatus{
		Phase:              domain.PhaseFailed,
		ResumeFrom:         domain.PhasePlanned,
		ErrorCategory:      string(domain.ErrorPrecondition),
		StartedAt:          first,
		UpdatedAt:          first,
		ObservedGeneration: 1,
	}

	recordPlanningOutcome(
		status,
		2,
		domain.NewError(domain.ErrorPrecondition, "plan workflow", "still failing"),
	)

	if !status.StartedAt.After(first.Time) {
		t.Fatal("corrected spec did not restart the planning retry window")
	}

	if !PlanningRetryActive(status, time.Now()) {
		t.Fatal("corrected spec did not rearm planning retries")
	}
}

func newPlanningRetryStore(
	t *testing.T,
	object *v1alpha1.ClusterReservation,
) *kube.CRDWorkflowStore[*v1alpha1.ClusterReservation] {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.ClusterReservation{}).
		WithObjects(object).
		Build()

	store, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	return store
}

func planningRetryReservation(startedAt, updatedAt time.Time) *v1alpha1.ClusterReservation {
	status := planningRetryStatus(domain.ErrorPrecondition, startedAt, updatedAt)
	status.ObservedGeneration = 1
	status.Message = "source Pod ns/pod does not exist"
	status.Conditions = []v1alpha1.WorkflowCondition{{
		Type: "Planned", Status: metav1.ConditionFalse, Reason: "DiscoveryFailed",
		Message: status.Message,
	}}

	return &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "transfer",
			Namespace:  "",
			UID:        "workflow",
			Generation: 1,
		},
		Status: v1alpha1.ClusterReservationStatus{WorkflowStatus: *status},
	}
}

func TestPlanningFailureGateWaitsThenFallsThrough(t *testing.T) {
	now := time.Now()
	store := newPlanningRetryStore(t, planningRetryReservation(now, now))

	result, stop := planningFailureGate(
		t.Context(),
		store,
		nil,
		mustLoadReservation(t, store, "transfer"),
	)
	if !stop {
		t.Fatal("fresh failure fell through instead of waiting for the backoff")
	}

	if result.RequeueAfter <= 0 || result.RequeueAfter > planningRetryCap {
		t.Fatalf("requeue delay = %v, want bounded backoff", result.RequeueAfter)
	}

	// An attempt older than the interval is due immediately: the gate must
	// fall through so the controller replans now.
	object := planningRetryReservation(now.Add(-planningRetryFloor), now.Add(-planningRetryFloor))
	store = newPlanningRetryStore(t, object)

	result, stop = planningFailureGate(
		t.Context(),
		store,
		nil,
		mustLoadReservation(t, store, "transfer"),
	)
	if stop {
		t.Fatalf("due retry stopped: %+v", result)
	}
}

func TestPlanningFailureGateMarksExhaustedWindowOnce(t *testing.T) {
	now := time.Now()
	object := planningRetryReservation(
		now.Add(-planningRetryWindow-time.Minute),
		now.Add(-planningRetryCap),
	)
	store := newPlanningRetryStore(t, object)

	result, stop := planningFailureGate(
		t.Context(),
		store,
		nil,
		mustLoadReservation(t, store, "transfer"),
	)
	if !stop || result.RequeueAfter != 0 {
		t.Fatalf("exhausted window did not terminalize: %+v stop=%v", result, stop)
	}

	saved, err := store.Load(t.Context(), crclient.ObjectKey{Name: "transfer"})
	if err != nil {
		t.Fatal(err)
	}

	condition := plannedCondition(&saved.Status.WorkflowStatus)
	if condition == nil || condition.Reason != planningRetriesExhaustedReason {
		t.Fatalf("terminal marker missing: %+v", condition)
	}

	if !strings.Contains(saved.Status.Message, "planning retry window elapsed") {
		t.Fatalf("terminal message missing suffix: %q", saved.Status.Message)
	}

	// The marker write is idempotent: a second gate pass must not fail or
	// rewrite the condition.
	if _, stop := planningFailureGate(t.Context(), store, nil, saved); !stop {
		t.Fatal("marked workflow fell through to planning")
	}

	again, err := store.Load(t.Context(), crclient.ObjectKey{Name: "transfer"})
	if err != nil {
		t.Fatal(err)
	}

	if again.Status.Message != saved.Status.Message {
		t.Fatal("terminal marker was rewritten after already being recorded")
	}
}

func plannedCondition(status *v1alpha1.WorkflowStatus) *v1alpha1.WorkflowCondition {
	for index := range status.Conditions {
		if status.Conditions[index].Type == "Planned" {
			return &status.Conditions[index]
		}
	}

	return nil
}

func mustLoadReservation(
	t *testing.T,
	store *kube.CRDWorkflowStore[*v1alpha1.ClusterReservation],
	name string,
) *v1alpha1.ClusterReservation {
	t.Helper()

	object, err := store.Load(t.Context(), crclient.ObjectKey{Name: name})
	if err != nil {
		t.Fatal(err)
	}

	return object
}

func TestOneShotTreatsPlanningBackoffAsStable(t *testing.T) {
	passes := 0

	err := reconcileUntilStable(
		t.Context(),
		reconcile.Request{},
		func(context.Context, reconcile.Request) (reconcile.Result, error) {
			passes++
			return reconcile.Result{RequeueAfter: planningRetryFloor}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if passes != 1 {
		t.Fatalf("one-shot chased a time-based planning backoff: %d passes", passes)
	}
}
