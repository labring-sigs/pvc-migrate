package kube

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// CancelCRDReservationCopyHandoff returns cleanup responsibility to the original
// Reservation. The caller holds the same Lease used by adoption and execution.
func CancelCRDReservationCopyHandoff(
	ctx context.Context,
	client crclient.Client,
	reservation *v1alpha1.ClusterReservation,
) error {
	if reservation == nil {
		return workflowStoreConflict("cancel handoff", "reservation is required")
	}

	if err := requireWorkflowStorageVersion(reservation); err != nil {
		return err
	}

	source := &v1alpha1.ClusterReservation{}

	key := crclient.ObjectKeyFromObject(reservation)
	if err := client.Get(ctx, key, source); err != nil {
		return err
	}

	if err := checkWorkflowStorageVersion(reservation, source); err != nil {
		return err
	}

	target := &v1alpha1.ClusterCopy{}

	targetErr := client.Get(ctx, key, target)
	if targetErr != nil && !apierrors.IsNotFound(targetErr) {
		return targetErr
	}

	if apierrors.IsNotFound(targetErr) &&
		source.Annotations[reservationCopyPendingAnnotation] == "" {
		return nil
	}

	if targetErr == nil {
		if err := ValidatePendingReservationCopy(source, target); err != nil {
			return err
		}
	}

	restored := source.DeepCopy()
	delete(restored.Annotations, reservationCopyPendingAnnotation)
	restored.Finalizers = ensureSessionFinalizer(restored.Finalizers)

	if err := handoffFenceError(ctx); err != nil {
		return err
	}

	if err := client.Update(ctx, restored); err != nil {
		return err
	}

	if err := handoffFenceError(ctx); err != nil {
		return err
	}

	*reservation = *restored.DeepCopy()

	if apierrors.IsNotFound(targetErr) {
		return nil
	}

	// Copy remains fenced by its pending annotation throughout cancellation.
	// Restoring Reservation protection must precede removal of Copy protection.
	target.Finalizers = removeSessionFinalizer(target.Finalizers)

	if err := handoffFenceError(ctx); err != nil {
		return err
	}

	if err := client.Update(ctx, target); err != nil {
		return err
	}

	if err := handoffFenceError(ctx); err != nil {
		return err
	}

	uid, version := target.UID, target.ResourceVersion
	err := client.Delete(ctx, target, &crclient.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version},
	})

	return errors.Join(crclient.IgnoreNotFound(err), ctx.Err(), LeaseFenceError(ctx))
}

// ValidatePendingReservationCopy verifies that both records belong to the
// same handoff and Copy has not acquired execution progress.
func ValidatePendingReservationCopy(
	source *v1alpha1.ClusterReservation,
	target *v1alpha1.ClusterCopy,
) error {
	if source == nil || target == nil || source.UID == "" || source.Name == "" ||
		source.Namespace != "" || target.Namespace != "" || source.Name != target.Name {
		return workflowStoreConflict(
			"cancel handoff",
			"reservation and copy must share a cluster-scoped identity",
		)
	}

	pending := target.Annotations[reservationCopyPendingAnnotation]
	if pending == "" || target.Annotations[reservationCopyOriginAnnotation] != string(source.UID) ||
		(source.Annotations[reservationCopyPendingAnnotation] != "" && source.Annotations[reservationCopyPendingAnnotation] != pending) {
		return workflowStoreConflict(
			"cancel handoff",
			"copy does not belong to this pending handoff",
		)
	}

	if target.Status.Phase != "" && target.Status.Phase != "Reserved" {
		return workflowStoreConflict("cancel handoff", "copy has already entered execution")
	}

	for _, checkpoint := range target.Status.Volumes {
		if checkpoint.Sync.Attempts != 0 || checkpoint.Sync.WarmCompletedAt != nil {
			return workflowStoreConflict("cancel handoff", "copy has an execution checkpoint")
		}
	}

	return nil
}
