package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// runActiveReconcile serializes a controller's local work while allowing a
// deletion queue to interrupt it. The durable Lease remains the execution fence.
func runActiveReconcile(
	ctx context.Context,
	active *sync.Map,
	uid types.UID,
	run func(context.Context) (reconcile.Result, error),
) (result reconcile.Result, resultErr error) {
	ctx, cancel := context.WithCancel(ctx)
	if _, running := active.LoadOrStore(uid, cancel); running {
		cancel()
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	defer func() {
		interrupted := errors.Is(ctx.Err(), context.Canceled)

		cancel()
		active.Delete(uid)

		if interrupted {
			result, resultErr = reconcile.Result{}, nil
		}
	}()

	return run(ctx)
}

func cancelActiveReconcile(active *sync.Map, uid types.UID) {
	if cancel, ok := active.Load(uid); ok {
		if cancel, ok := cancel.(context.CancelFunc); ok {
			cancel()
		}
	}
}

func workflowReconcileResult(err error) (reconcile.Result, error) {
	if kube.IsSessionLockContention(err) || domain.CategoryOf(err) == domain.ErrorConflict ||
		apierrors.IsConflict(err) {
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}

	return reconcile.Result{}, err
}

// withPlanningLease shares only optimistic concurrency and fencing. The caller
// owns its concrete CRD's planning rules and status transaction.
func withPlanningLease[T crclient.Object](
	ctx context.Context,
	store kube.WorkflowStore[T],
	locker kube.SessionLocker,
	namespace string,
	object T,
	plan func(context.Context) error,
) (resultErr error) {
	lock, err := kube.AcquireRequiredSessionLock(ctx, locker, namespace, object.GetName())
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		resultErr = errors.Join(resultErr, lock.Release(releaseCtx))
	}()

	ctx, cancel := lock.Bind(ctx)
	defer cancel()

	ctx = kube.WithLeaseFence(ctx, lock)

	latest, err := store.Load(ctx, crclient.ObjectKeyFromObject(object))
	if apierrors.IsNotFound(err) {
		return lock.Delete(ctx)
	}

	if err != nil {
		return err
	}

	if latest.GetUID() != object.GetUID() ||
		latest.GetResourceVersion() != object.GetResourceVersion() {
		return domain.NewError(
			domain.ErrorConflict,
			"plan workflow",
			"workflow changed while acquiring its lock",
		)
	}

	if latest.GetDeletionTimestamp() != nil {
		return domain.NewError(domain.ErrorConflict, "plan workflow", "workflow is being deleted")
	}

	if err := kube.RequireWorkflowHandoffComplete(latest); err != nil {
		return err
	}

	if err := errors.Join(ctx.Err(), lock.Err()); err != nil {
		return err
	}

	return errors.Join(plan(ctx), lock.Err())
}

func transferPlanningError(report *domain.TransferPlan, hasPlan bool, cause error) error {
	if cause != nil {
		return cause
	}

	if report != nil && report.Ready && hasPlan {
		return nil
	}

	messages := []string{"planning checks failed"}
	if report != nil {
		for _, check := range report.Checks {
			if !check.Passed {
				messages = append(messages, check.Message)
			}
		}
	}

	return domain.NewError(domain.ErrorPrecondition, "plan workflow", strings.Join(messages, "; "))
}

func retryCorrectedPlanning(
	phase, resumeFrom v1alpha1.WorkflowPhase,
	observedGeneration, generation int64,
) bool {
	return phase == domain.PhaseFailed && resumeFrom == domain.PhasePlanned &&
		generation > observedGeneration
}

func recordPlanningOutcome(status *v1alpha1.WorkflowStatus, generation int64, cause error) {
	now := metav1.Now()

	status.UpdatedAt = now
	if status.StartedAt.IsZero() {
		status.StartedAt = now
	}

	condition := v1alpha1.WorkflowCondition{
		Type: "Planned", Status: metav1.ConditionTrue, Reason: "DiscoverySucceeded",
		Message: "Resource identities and execution plan resolved", LastTransitionTime: now,
	}
	status.Phase = domain.PhasePlanned
	status.ObservedGeneration = generation
	status.ResumeFrom = ""

	status.ErrorCategory = ""
	if cause != nil {
		status.Phase = domain.PhaseFailed
		status.ResumeFrom = domain.PhasePlanned
		status.ErrorCategory = string(domain.CategoryOf(cause))
		condition.Status = metav1.ConditionFalse
		condition.Reason = "DiscoveryFailed"
		condition.Message = domain.BoundWorkflowMessage(cause.Error())
	}

	status.Message = condition.Message
	domain.SetWorkflowCondition(status, condition)
}

func savePlannedWorkflow[T crclient.Object](
	ctx context.Context,
	store kube.WorkflowStore[T],
	object T,
) error {
	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	// Freeze the admitted spec fingerprint once planning succeeds. Cleanup
	// policy edits are excluded by the fingerprint itself, and a failed plan
	// leaves the hash unset so corrected-spec replanning stays possible.
	if status := workflowStatusPtr(object); status != nil &&
		status.ExecutionIntentHash == "" && status.Phase != domain.PhaseFailed {
		hash, err := kube.WorkflowExecutionIntentHash(object)
		if err != nil {
			return err
		}

		status.ExecutionIntentHash = hash
	}

	if err := store.Save(ctx, object); err != nil {
		return err
	}

	return errors.Join(ctx.Err(), kube.LeaseFenceError(ctx))
}

func workflowStatusPtr(object crclient.Object) *v1alpha1.WorkflowStatus {
	switch typed := object.(type) {
	case *v1alpha1.Migration:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.PodMigration:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.Reservation:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.Copy:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.Backup:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.Restore:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.Rename:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.ClusterMigration:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.ClusterPodMigration:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.ClusterReservation:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.ClusterCopy:
		return &typed.Status.WorkflowStatus
	case *v1alpha1.Move:
		return &typed.Status.WorkflowStatus
	default:
		return nil
	}
}

// executionIntentMutationError reports a spec change after the controller
// admitted and planned the workflow. The frozen status plan keeps execution
// from being retargeted; this fence makes the tampering visible instead of
// silently finishing against a request the tenant replaced.
func executionIntentMutationError(object crclient.Object) error {
	status := workflowStatusPtr(object)
	if status == nil || status.ExecutionIntentHash == "" {
		return nil
	}

	current, err := kube.WorkflowExecutionIntentHash(object)
	if err != nil {
		return err
	}

	if status.ExecutionIntentHash == current {
		return nil
	}

	return changedWorkflowDefinitionError()
}

// verifyAdmittedSpec fences spec tampering on already-planned workflows. The
// common path is a fingerprint comparison; the lease is only taken once a
// mutation has been detected and must be checkpointed as a failure.
func verifyAdmittedSpec[T crclient.Object](
	ctx context.Context,
	store kube.WorkflowStore[T],
	locker kube.SessionLocker,
	namespace string,
	object T,
	recorder events.EventRecorder,
) error {
	if executionIntentMutationError(object) == nil {
		return nil
	}

	return checkpointSpecMutation(ctx, store, locker, namespace, object, recorder)
}

// checkpointSpecMutation persists the failure caused by a mutated workflow
// spec. It runs under the same planning lease as planning itself so a racing
// status writer cannot interleave.
func checkpointSpecMutation[T crclient.Object](
	ctx context.Context,
	store kube.WorkflowStore[T],
	locker kube.SessionLocker,
	namespace string,
	object T,
	recorder events.EventRecorder,
) error {
	return withPlanningLease(
		ctx,
		store,
		locker,
		namespace,
		object,
		func(ctx context.Context) error {
			cause := executionIntentMutationError(object)
			if cause == nil {
				return nil
			}

			previous := workflowStatusPtr(object).DeepCopy()

			recordPlanningFailureForMutation(
				workflowStatusPtr(object),
				object.GetGeneration(),
				cause,
			)

			if err := store.Save(ctx, object); err != nil {
				*workflowStatusPtr(object) = *previous
				return err
			}

			if recorder != nil {
				recorder.Eventf(
					object,
					nil,
					"Warning",
					"SpecMutated",
					"Reconcile",
					"%s",
					workflowStatusPtr(object).Message,
				)
			}

			return cause
		},
	)
}

func recordPlanningFailureForMutation(
	status *v1alpha1.WorkflowStatus,
	generation int64,
	cause error,
) {
	now := metav1.Now()

	status.UpdatedAt = now
	if status.StartedAt.IsZero() {
		status.StartedAt = now
	}

	// The failed workflow must carry a resume checkpoint: execution had
	// already reached status.Phase when the admitted fingerprint stopped
	// matching. Validation rejects a Failed phase without one, which would
	// otherwise wedge deletion convergence and recovery forever.
	if status.ResumeFrom == "" && status.Phase != "" &&
		status.Phase != domain.PhaseFailed {
		status.ResumeFrom = status.Phase
	}

	status.Phase = domain.PhaseFailed
	status.ObservedGeneration = generation
	status.ErrorCategory = string(domain.CategoryOf(cause))
	status.Message = domain.BoundWorkflowMessage(cause.Error())
	domain.SetWorkflowCondition(status, v1alpha1.WorkflowCondition{
		Type: "SpecMutated", Status: metav1.ConditionTrue, Reason: "ExecutionIntentChanged",
		Message: cause.Error(), LastTransitionTime: now,
	})
}

func recordPlanningEvent(
	recorder events.EventRecorder,
	object crclient.Object,
	cause error,
	message string,
) {
	if recorder == nil {
		return
	}

	if cause != nil {
		recorder.Eventf(object, nil, "Warning", "DiscoveryFailed", "Plan", "%s", message)
		return
	}

	recorder.Eventf(object, nil, "Normal", "DiscoverySucceeded", "Plan", "%s", message)
}
