package controller

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// namespacedReservationCopyRecovery coordinates only the persisted handoff protocol.
// Normal operation execution stays in each operation's reconciler.
type namespacedReservationCopyRecovery struct {
	client       crclient.Client
	kubernetes   kubernetes.Interface
	reservations *kube.CRDWorkflowStore[*v1alpha1.Reservation]
	copies       *kube.CRDWorkflowStore[*v1alpha1.Copy]
	locker       kube.SessionLocker
	engine       copyengine.Engine
	copyConfig   app.CopyExecutorConfig
}

func (r *namespacedReservationCopyRecovery) recoverReservation(
	ctx context.Context,
	source *v1alpha1.Reservation,
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

func (r *namespacedReservationCopyRecovery) recoverCopy(
	ctx context.Context,
	target *v1alpha1.Copy,
) (bool, error) {
	if kube.RequireWorkflowHandoffComplete(target) == nil {
		return false, nil
	}

	source, err := r.reservations.Load(ctx, crclient.ObjectKeyFromObject(target))
	if apierrors.IsNotFound(err) {
		executor := app.NewCopyExecutor(r.kubernetes, r.copies, r.locker,
			r.engine, r.copyConfig)

		return true, executor.ActivateReservationHandoff(ctx, target,
			func(ctx context.Context, object *v1alpha1.Copy) error {
				return kube.NamespacedActivateCRDCopyHandoff(ctx, r.client, object)
			})
	}

	if err != nil {
		return true, err
	}

	return true, r.recoverPair(ctx, source, target)
}

func (r *namespacedReservationCopyRecovery) recoverPair(
	ctx context.Context,
	source *v1alpha1.Reservation,
	target *v1alpha1.Copy,
) error {
	if err := kube.NamespacedValidatePendingReservationCopy(source, target); err != nil {
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

	executor := app.NewCopyExecutor(
		r.kubernetes,
		r.copies,
		r.locker,
		r.engine,
		r.copyConfig,
	)
	_, err := executor.AdoptReservation(
		ctx,
		r.reservations,
		source,
		target.Spec,
		func(ctx context.Context, reservation *v1alpha1.Reservation, destination *v1alpha1.Copy) error {
			return kube.NamespacedHandoffCRDReservationToCopy(
				ctx,
				r.client,
				reservation,
				destination,
			)
		},
	)

	return err
}

func (r *namespacedReservationCopyRecovery) cancel(
	ctx context.Context,
	source *v1alpha1.Reservation,
) error {
	executor := app.NewReservationExecutor(
		r.kubernetes,
		r.reservations,
		r.locker,
		app.ReservationExecutorConfig{},
	)

	return executor.CancelCopyHandoff(ctx, source,
		func(ctx context.Context, object *v1alpha1.Reservation) error {
			return kube.NamespacedCancelCRDReservationCopyHandoff(ctx, r.client, object)
		})
}
