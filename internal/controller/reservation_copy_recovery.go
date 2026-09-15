package controller

import (
	"context"
	"slices"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// reservationCopyRecovery coordinates only the persisted handoff protocol.
// Normal operation execution stays in each operation's reconciler.
type reservationCopyRecovery struct {
	client       crclient.Client
	kubernetes   kubernetes.Interface
	reservations *kube.CRDWorkflowStore[*v1alpha1.ClusterReservation]
	copies       *kube.CRDWorkflowStore[*v1alpha1.ClusterCopy]
	locker       kube.SessionLocker
	engine       copyengine.Engine
	copyConfig   app.CopyExecutorConfig
}

func (r *reservationCopyRecovery) recoverReservation(
	ctx context.Context,
	source *v1alpha1.ClusterReservation,
) (bool, error) {
	target, err := r.copies.Load(ctx, crclient.ObjectKeyFromObject(source))
	if apierrors.IsNotFound(err) {
		if kube.RequireWorkflowHandoffComplete(source) == nil {
			return false, nil
		}
		return true, r.cancel(ctx, source)
	}

	if err != nil {
		return false, err
	}

	if kube.RequireWorkflowHandoffComplete(source) == nil &&
		kube.RequireWorkflowHandoffComplete(target) == nil {
		return false, nil
	}

	return true, r.recoverPair(ctx, source, target)
}

func (r *reservationCopyRecovery) recoverCopy(
	ctx context.Context,
	target *v1alpha1.ClusterCopy,
) (bool, error) {
	if kube.RequireWorkflowHandoffComplete(target) == nil {
		return false, nil
	}

	source, err := r.reservations.Load(ctx, crclient.ObjectKeyFromObject(target))
	if apierrors.IsNotFound(err) {
		executor := app.NewClusterCopyExecutor(r.kubernetes, r.copies, r.locker,
			copyWorkflowStorageNamespace(target.Spec, target.Status.Plan), r.engine, r.copyConfig)

		return true, executor.ActivateReservationHandoff(ctx, target,
			func(ctx context.Context, object *v1alpha1.ClusterCopy) error {
				return kube.ActivateCRDCopyHandoff(ctx, r.client, object)
			})
	}

	if err != nil {
		return true, err
	}

	return true, r.recoverPair(ctx, source, target)
}

func (r *reservationCopyRecovery) recoverPair(
	ctx context.Context,
	source *v1alpha1.ClusterReservation,
	target *v1alpha1.ClusterCopy,
) error {
	if err := kube.ValidatePendingReservationCopy(source, target); err != nil {
		return err
	}
	// Once deletion has started without our finalizer, Kubernetes cannot accept
	// restoring it. Wait for that record to disappear, then activate Copy so its
	// own deletion path can perform cleanup if necessary.
	if source.DeletionTimestamp != nil &&
		!slices.Contains(source.Finalizers, kube.SessionFinalizer) {
		return nil
	}

	if source.DeletionTimestamp != nil || target.DeletionTimestamp != nil ||
		kube.RequireWorkflowHandoffComplete(source) == nil {
		return r.cancel(ctx, source)
	}

	executor := app.NewClusterCopyExecutor(
		r.kubernetes,
		r.copies,
		r.locker,
		reservationWorkflowStorageNamespace(
			source.Spec,
			source.Status.Plan,
		),
		r.engine,
		r.copyConfig,
	)
	_, err := executor.AdoptReservation(
		ctx,
		r.reservations,
		source,
		target.Spec,
		func(ctx context.Context, reservation *v1alpha1.ClusterReservation, destination *v1alpha1.ClusterCopy) error {
			return kube.HandoffCRDReservationToCopy(ctx, r.client, reservation, destination)
		},
	)

	return err
}

func (r *reservationCopyRecovery) cancel(
	ctx context.Context,
	source *v1alpha1.ClusterReservation,
) error {
	executor := app.NewClusterReservationExecutor(
		r.kubernetes,
		r.reservations,
		r.locker,
		reservationWorkflowStorageNamespace(
			source.Spec,
			source.Status.Plan,
		),
		app.ReservationExecutorConfig{},
	)

	return executor.CancelCopyHandoff(ctx, source,
		func(ctx context.Context, object *v1alpha1.ClusterReservation) error {
			return kube.CancelCRDReservationCopyHandoff(ctx, r.client, object)
		})
}

func handoffReconcileResult(err error) (reconcile.Result, error) {
	if err != nil {
		return workflowReconcileResult(err)
	}
	// Re-read after metadata handoff, including when an unrelated finalizer is
	// delaying source deletion and no further watch event has arrived yet.
	return reconcile.Result{RequeueAfter: time.Second}, nil
}
