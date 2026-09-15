package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RequestResume records an explicit retry for a controller. A failed discovery
// resumes planning; a failed execution keeps its exact resource checkpoints.
func (r *ClusterReservationExecutor) RequestResume(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
) error {
	if err := validateClusterReservationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, r.store, r.locker, r.storageNamespace, object,
		func(ctx context.Context) error {
			if err := r.Validate(ctx, object); err != nil {
				return err
			}

			if object.Status.Phase != domain.PhaseFailed {
				return nil
			}

			previous := object.Status.WorkflowStatus.DeepCopy()
			if err := domain.ReactivateWorkflow(
				&object.Status.WorkflowStatus,
				"reservation resume requested",
				r.now(),
			); err != nil {
				return err
			}

			if err := persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return r.store.Save(ctx, object) },
			); err != nil {
				object.Status.WorkflowStatus = *previous
				return err
			}

			return nil
		})
}

// FinalizeDeleted stops provisioning and releases storage ownership under the
// same lock used by normal execution. Resource cleanup precedes metadata removal.
func (r *ClusterReservationExecutor) FinalizeDeleted(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
) error {
	// Statuses written by older releases can carry Failed without a resume
	// checkpoint. Deletion is the final convergence pass and must not be
	// wedged by that era's validation gap.
	if object.DeletionTimestamp != nil &&
		object.Status.Phase == domain.PhaseFailed &&
		object.Status.ResumeFrom == "" {
		object.Status.ResumeFrom = domain.PhasePlanned
	}

	if err := validateClusterReservationObject(object); err != nil {
		return err
	}

	if object.DeletionTimestamp == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize reservation",
			"workflow deletion is required",
		)
	}

	ctx = context.WithValue(ctx, workflowDeletionContextKey{}, true)

	return withStoredWorkflowLock(ctx, r.store, r.locker, r.storageNamespace, object,
		func(ctx context.Context) error {
			before := object.Status.WorkflowStatus.DeepCopy()
			domain.SetWorkflowCondition(&object.Status.WorkflowStatus, v1alpha1.WorkflowCondition{
				Type:               "Deleting",
				Status:             metav1.ConditionTrue,
				Reason:             "CleaningUp",
				Message:            "Stopping reservation and releasing storage ownership before deletion",
				LastTransitionTime: metav1.NewTime(r.now().UTC()),
			})

			if err := persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return r.store.Save(ctx, object) },
			); err != nil {
				object.Status.WorkflowStatus = *before
				return err
			}

			if object.Status.Phase != domain.PhaseReserved &&
				object.Status.Phase != domain.PhaseAborted {
				if err := r.abort(ctx, object); err != nil {
					return err
				}
			}

			return r.cleanup(
				ctx,
				object,
				ReservationCleanupOptions{Finalize: true, DeleteSession: true},
			)
		})
}

// ValidateAbort requires no cluster reads: reservation never pauses a workload
// or changes the application's PVC identity. Resource release belongs to cleanup.
func (r *ClusterReservationExecutor) ValidateAbort(object *v1alpha1.ClusterReservation) error {
	return validateClusterReservationObject(object)
}

func (r *ClusterReservationExecutor) Abort(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
) error {
	if err := r.ValidateAbort(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, r.store, r.locker, r.storageNamespace, object,
		func(ctx context.Context) error { return r.abort(ctx, object) })
}

func (r *ClusterReservationExecutor) abort(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
) error {
	if err := r.ValidateAbort(object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if object.Status.Plan == nil {
		return r.transition(
			ctx,
			object,
			domain.PhaseAborted,
			"reservation aborted before execution planning",
		)
	}

	if err := r.transition(ctx, object, domain.PhaseAborting, "aborting reservation"); err != nil {
		return err
	}

	return r.transition(
		ctx,
		object,
		domain.PhaseAborted,
		"reservation aborted; reserved volumes are retained for cleanup",
	)
}
