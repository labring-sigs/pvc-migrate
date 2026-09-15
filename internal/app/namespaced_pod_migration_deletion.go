package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (m *PodMigrationExecutor) FinalizeDeleted(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	// Statuses written by older releases can carry Failed without a resume
	// checkpoint. Deletion is the final convergence pass and must not be
	// wedged by that era's validation gap.
	if object.DeletionTimestamp != nil &&
		object.Status.Phase == domain.PhaseFailed &&
		object.Status.ResumeFrom == "" {
		object.Status.ResumeFrom = domain.PhasePlanned
	}

	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	if object.DeletionTimestamp == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize migration",
			"workflow deletion is required",
		)
	}

	ctx = context.WithValue(ctx, workflowDeletionContextKey{}, true)

	return withStoredWorkflowLock(
		ctx,
		m.store,
		m.locker,
		object.Namespace,
		object,
		func(ctx context.Context) error {
			previous := object.Status.WorkflowStatus.DeepCopy()
			domain.SetWorkflowCondition(
				&object.Status.WorkflowStatus,
				v1alpha1.WorkflowCondition{
					Type:               "Deleting",
					Status:             metav1.ConditionTrue,
					Reason:             "CleaningUp",
					Message:            "Releasing migration storage ownership before deletion",
					LastTransitionTime: metav1.NewTime(m.now().UTC()),
				},
			)

			if err := persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return m.store.Save(ctx, object) },
			); err != nil {
				object.Status.WorkflowStatus = *previous
				return err
			}

			phase := workflowResumePhase(object.Status.WorkflowStatus)
			if phase != domain.PhaseCompleted && phase != domain.PhaseRolledBack &&
				phase != domain.PhaseAborted {
				var err error
				if deletionRequiresConvergence(phase) {
					err = m.run(ctx, object)
				} else {
					err = m.abort(ctx, object)
				}

				if err != nil {
					return err
				}
			}

			return m.cleanup(
				ctx,
				object,
				MigrationCleanupOptions{Finalize: true, DeleteSession: true},
			)
		},
	)
}
