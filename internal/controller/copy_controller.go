package controller

import (
	"context"
	"errors"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type CopyPlanner func(context.Context, *v1alpha1.ClusterCopy, string) (*domain.TransferPlan, error)

type ClusterCopyReconciler struct {
	store          *kube.CRDWorkflowStore[*v1alpha1.ClusterCopy]
	client         kubernetes.Interface
	planner        CopyPlanner
	locker         kube.SessionLocker
	engine         copyengine.Engine
	config         app.CopyExecutorConfig
	handoff        *reservationCopyRecovery
	checkCollision func(context.Context, string, []string) error
	active         *sync.Map
	recorder       events.EventRecorder
}

func (r *ClusterCopyReconciler) Reconcile(
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
			if handled, err := r.handoff.recoverCopy(ctx, object); handled || err != nil {
				return handoffReconcileResult(err)
			}
		}

		return workflowReconcileResult(r.executor(object).FinalizeDeleted(ctx, object))
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

func (r *ClusterCopyReconciler) reconcile(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
) (reconcile.Result, error) {
	if r.handoff != nil {
		if handled, err := r.handoff.recoverCopy(ctx, object); handled || err != nil {
			return handoffReconcileResult(err)
		}
	}

	if err := kube.RequireWorkflowHandoffComplete(object); err != nil {
		return workflowReconcileResult(err)
	}

	if err := r.checkCollision(
		ctx,
		object.Name,
		copyWorkflowNamespaces(object.Spec, object.Status.Plan),
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
			return reconcile.Result{}, nil
		}
	case domain.PhaseWarmCopied, domain.PhaseAborted:
		return reconcile.Result{}, nil
	}

	if object.Status.Plan == nil {
		if err := r.plan(ctx, object); err != nil {
			return workflowReconcileResult(err)
		}

		if object.Status.Phase == domain.PhasePlanned {
			return reconcile.Result{RequeueAfter: time.Millisecond}, nil
		}

		return reconcile.Result{}, nil
	}

	if err := verifyAdmittedSpec(
		ctx,
		r.store,
		r.locker,
		copyWorkflowStorageNamespace(object.Spec, object.Status.Plan),
		object,
		r.recorder,
	); err != nil {
		return workflowReconcileResult(err)
	}

	return workflowReconcileResult(r.executor(object).Run(ctx, object))
}

func (r *ClusterCopyReconciler) executor(object *v1alpha1.ClusterCopy) *app.ClusterCopyExecutor {
	config := r.config
	applyTransferPolicy(&config.Transfer, &object.Spec.TransferOptions, func(message string) {
		if r.recorder != nil {
			r.recorder.Eventf(
				object,
				nil,
				"Warning",
				"RetryPolicyInvalid",
				"Execute",
				"%s",
				message,
			)
		}
	})

	return app.NewClusterCopyExecutor(r.client, r.store, r.locker,
		copyWorkflowStorageNamespace(object.Spec, object.Status.Plan), r.engine, config)
}

func (r *ClusterCopyReconciler) plan(ctx context.Context, object *v1alpha1.ClusterCopy) error {
	if r.planner == nil {
		return errors.New("copy planner is not configured")
	}

	namespace := copyWorkflowStorageNamespace(object.Spec, object.Status.Plan)

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
				copyWorkflowNamespaces(object.Spec, object.Status.Plan),
			); err != nil {
				return err
			}

			before := object.Status.DeepCopy()

			cause := kube.RequireNamespace(ctx, r.client, namespace)
			if cause == nil {
				report, err := r.planner(ctx, object, r.config.Transfer.TrustedToolImage)
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

func copyWorkflowStorageNamespace(
	spec v1alpha1.ClusterCopySpec,
	plan *v1alpha1.ClusterCopyPlan,
) string {
	if plan != nil {
		return string(plan.SessionNamespace)
	}

	if spec.SessionNamespace != "" {
		return string(spec.SessionNamespace)
	}

	return string(spec.SourceNamespace)
}

func copyWorkflowNamespaces(
	spec v1alpha1.ClusterCopySpec,
	plan *v1alpha1.ClusterCopyPlan,
) []string {
	if plan != nil {
		return []string{
			string(plan.SourceNamespace),
			string(plan.DestinationNamespace),
			string(plan.SessionNamespace),
		}
	}

	return []string{
		string(spec.SourceNamespace),
		string(spec.DestinationNamespace),
		copyWorkflowStorageNamespace(spec, nil),
	}
}
