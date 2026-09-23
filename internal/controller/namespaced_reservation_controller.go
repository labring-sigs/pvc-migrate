package controller

import (
	"context"
	"errors"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type NamespacedReservationPlanner func(context.Context, *v1alpha1.Reservation, string) (*domain.TransferPlan, error)

type ReservationReconciler struct {
	store          *kube.CRDWorkflowStore[*v1alpha1.Reservation]
	client         kubernetes.Interface
	planner        NamespacedReservationPlanner
	locker         kube.SessionLocker
	config         app.ReservationExecutorConfig
	handoff        *namespacedReservationCopyRecovery
	checkCollision func(context.Context, string, []string) error
	active         *sync.Map
	recorder       events.EventRecorder
}

func (r *ReservationReconciler) Reconcile(
	ctx context.Context,
	request reconcile.Request,
) (reconcile.Result, error) {
	object, err := r.store.Load(ctx, request.NamespacedName)
	if apierrors.IsNotFound(err) {
		return reconcile.Result{}, nil
	}

	if err != nil {
		return reconcile.Result{}, err
	}

	if object.DeletionTimestamp != nil {
		cancelActiveReconcile(r.active, object.UID)

		if r.handoff != nil {
			if handled, err := r.handoff.recoverReservation(ctx, object); handled || err != nil {
				return handoffReconcileResult(err)
			}
		}

		return workflowReconcileResult(r.executor().FinalizeDeleted(ctx, object))
	}

	return runActiveReconcile(
		ctx,
		r.active,
		object.UID,
		func(ctx context.Context) (reconcile.Result, error) {
			latest, err := r.store.Load(ctx, request.NamespacedName)
			if apierrors.IsNotFound(err) {
				return reconcile.Result{}, nil
			}

			if err != nil {
				return reconcile.Result{}, err
			}

			if latest.UID != object.UID || latest.DeletionTimestamp != nil {
				return reconcile.Result{RequeueAfter: time.Millisecond}, nil
			}

			return r.reconcile(ctx, latest)
		},
	)
}

func (r *ReservationReconciler) reconcile(
	ctx context.Context,
	object *v1alpha1.Reservation,
) (reconcile.Result, error) {
	if r.handoff != nil {
		if handled, err := r.handoff.recoverReservation(ctx, object); handled || err != nil {
			return handoffReconcileResult(err)
		}
	}

	if err := kube.RequireWorkflowHandoffComplete(object); err != nil {
		return workflowReconcileResult(err)
	}

	if err := r.checkCollision(
		ctx,
		object.Name,
		[]string{object.Namespace},
	); err != nil {
		return workflowReconcileResult(err)
	}

	if err := r.store.EnsureProtection(ctx, object); err != nil {
		return workflowReconcileResult(err)
	}

	switch object.Status.Phase {
	case domain.PhaseFailed:
		if object.Status.Plan != nil || !retryCorrectedPlanning(
			object.Status.Phase, object.Status.ResumeFrom,
			object.Status.ObservedGeneration, object.Generation,
		) {
			if result, stop := planningFailureGate(ctx, r.store, r.recorder, object); stop {
				return result, nil
			}
		}
	case domain.PhaseReserved, domain.PhaseAborted:
		return reconcile.Result{}, nil
	}

	if object.Status.Plan == nil {
		if err := r.plan(ctx, object); err != nil {
			return workflowReconcileResult(err)
		}

		if object.Status.Phase == domain.PhasePlanned {
			return reconcile.Result{RequeueAfter: time.Millisecond}, nil
		}

		if delay, retry := requeuePlanningFailureDelay(
			workflowStatusPtr(object), time.Now(),
		); retry {
			return reconcile.Result{RequeueAfter: delay}, nil
		}

		return reconcile.Result{}, nil
	}

	if err := verifyAdmittedSpec(
		ctx,
		r.store,
		r.locker,
		object.Namespace,
		object,
		r.recorder,
	); err != nil {
		return workflowReconcileResult(err)
	}

	return workflowReconcileResult(r.executor().Run(ctx, object))
}

func (r *ReservationReconciler) executor() *app.ReservationExecutor {
	return app.NewReservationExecutor(r.client, r.store, r.locker, r.config)
}

func (r *ReservationReconciler) plan(
	ctx context.Context,
	object *v1alpha1.Reservation,
) error {
	if r.planner == nil {
		return errors.New("reservation planner is not configured")
	}

	namespace := object.Namespace

	return withPlanningLease(
		ctx,
		r.store,
		r.locker,
		namespace,
		object,
		func(ctx context.Context) error {
			if err := r.checkCollision(
				ctx,
				object.Name,
				[]string{object.Namespace},
			); err != nil {
				return err
			}

			before := object.Status.DeepCopy()

			cause := kube.RequireNamespace(ctx, r.client, namespace)
			if cause == nil {
				report, err := r.planner(ctx, object, r.config.TrustedToolImage)
				cause = transferPlanningError(report, object.Status.Plan != nil, err)
			}

			if cause != nil {
				object.Status = *before.DeepCopy()
			}

			recordPlanningOutcome(&object.Status.WorkflowStatus, object.Generation, cause)

			if err := savePlannedWorkflow(ctx, r.store, object); err != nil {
				object.Status = *before
				return errors.Join(cause, err)
			}

			recordPlanningEvent(r.recorder, object, cause, object.Status.Message)

			return nil
		},
	)
}
