package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type RenamePlanner func(context.Context, *v1alpha1.Rename, string) (*domain.PVCIdentityReport, error)

// RenameReconciler drives the same concrete operation executor as the CLI.
type RenameReconciler struct {
	store          *kube.CRDWorkflowStore[*v1alpha1.Rename]
	client         kubernetes.Interface
	planner        RenamePlanner
	locker         kube.SessionLocker
	checkCollision func(context.Context, string, string) error
	active         *sync.Map
	recorder       events.EventRecorder
}

func (r *RenameReconciler) Reconcile(
	ctx context.Context,
	request reconcile.Request,
) (result reconcile.Result, resultErr error) {
	object, err := r.store.Load(ctx, request.NamespacedName)
	if apierrors.IsNotFound(err) {
		return reconcile.Result{}, nil
	}

	if err != nil {
		return reconcile.Result{}, err
	}

	if object.DeletionTimestamp != nil {
		if cancel, ok := r.active.Load(object.UID); ok {
			if cancelFunc, ok := cancel.(context.CancelFunc); ok {
				cancelFunc()
			}
		}

		err := app.NewRenameExecutor(r.client, r.store, r.locker, object.Namespace).
			FinalizeDeleted(ctx, object)

		return renameReconcileResult(err)
	}

	ctx, cancel := context.WithCancel(ctx)
	if _, running := r.active.LoadOrStore(object.UID, cancel); running {
		cancel()
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	defer func() {
		interrupted := errors.Is(ctx.Err(), context.Canceled)

		cancel()
		r.active.Delete(object.UID)

		if interrupted {
			result, resultErr = reconcile.Result{}, nil
		}
	}()

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

	object = latest
	if err := r.checkCollision(ctx, object.Namespace, object.Name); err != nil {
		return renameReconcileResult(err)
	}

	if err := r.store.EnsureProtection(ctx, object); err != nil {
		return renameReconcileResult(err)
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
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return reconcile.Result{}, nil
	}

	if object.Status.Plan == nil {
		if err := r.plan(ctx, object); err != nil {
			return renameReconcileResult(err)
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

	err = verifyAdmittedSpec(ctx, r.store, r.locker, object.Namespace, object, r.recorder)
	if err == nil {
		err = app.NewRenameExecutor(r.client, r.store, r.locker, object.Namespace).Run(ctx, object)
	}

	return renameReconcileResult(err)
}

func renameReconcileResult(err error) (reconcile.Result, error) {
	if kube.IsSessionLockContention(err) || domain.CategoryOf(err) == domain.ErrorConflict {
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	return reconcile.Result{}, err
}

func (r *RenameReconciler) plan(ctx context.Context, object *v1alpha1.Rename) (resultErr error) {
	if r.planner == nil {
		return errors.New("rename planner is not configured")
	}

	lock, err := kube.AcquireRequiredSessionLock(ctx, r.locker, object.Namespace, object.Name)
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

	latest, err := r.store.Load(ctx, crclient.ObjectKeyFromObject(object))
	if apierrors.IsNotFound(err) {
		return lock.Delete(ctx)
	}

	if err != nil {
		return err
	}

	if latest.UID != object.UID || latest.ResourceVersion != object.ResourceVersion {
		return domain.NewError(
			domain.ErrorConflict,
			"plan rename",
			"workflow changed while acquiring its lock",
		)
	}

	if latest.DeletionTimestamp != nil || latest.Status.Plan != nil ||
		latest.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if err := r.checkCollision(ctx, object.Namespace, object.Name); err != nil {
		return err
	}

	if err := kube.RequireNamespace(ctx, r.client, object.Namespace); err != nil {
		return r.savePlanningFailure(ctx, object, lock, err)
	}

	before := object.Status.DeepCopy()

	report, err := r.planner(ctx, object, object.Namespace)
	if err == nil && (report == nil || !report.Ready || object.Status.Plan == nil) {
		messages := []string{"Rename planning checks failed"}
		if report != nil {
			for _, check := range report.Checks {
				if !check.Passed {
					messages = append(messages, check.Message)
				}
			}
		}

		err = domain.NewError(domain.ErrorPrecondition, "plan rename", strings.Join(messages, "; "))
	}

	if err != nil {
		object.Status = *before
		return r.savePlanningFailure(ctx, object, lock, err)
	}

	recordPlanningOutcome(&object.Status.WorkflowStatus, object.Generation, nil)

	if err := saveRenamePlanning(ctx, r.store, object, lock); err != nil {
		object.Status = *before
		return err
	}

	if r.recorder != nil {
		r.recorder.Eventf(
			object,
			nil,
			"Normal",
			"DiscoverySucceeded",
			"Plan",
			"%s",
			object.Status.Message,
		)
	}

	return nil
}

func (r *RenameReconciler) savePlanningFailure(
	ctx context.Context,
	object *v1alpha1.Rename,
	lock kube.SessionLock,
	cause error,
) error {
	before := object.Status.DeepCopy()
	recordPlanningOutcome(&object.Status.WorkflowStatus, object.Generation, cause)

	if err := saveRenamePlanning(ctx, r.store, object, lock); err != nil {
		object.Status = *before
		return errors.Join(cause, err)
	}

	if r.recorder != nil {
		r.recorder.Eventf(
			object,
			nil,
			"Warning",
			"DiscoveryFailed",
			"Plan",
			"%s",
			object.Status.Message,
		)
	}

	return nil
}

func saveRenamePlanning(
	ctx context.Context,
	store kube.WorkflowStore[*v1alpha1.Rename],
	object *v1alpha1.Rename,
	lock kube.SessionLock,
) error {
	if err := errors.Join(ctx.Err(), lock.Err()); err != nil {
		return err
	}

	if err := store.Save(ctx, object); err != nil {
		return err
	}

	return errors.Join(ctx.Err(), lock.Err())
}
