package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *ClusterCopyExecutor) RequestResume(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
) error {
	if err := validateClusterCopyObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, c.store, c.locker, c.storageNamespace, object,
		func(ctx context.Context) error {
			if err := c.Validate(ctx, object); err != nil {
				return err
			}

			if object.Status.Phase != domain.PhaseFailed {
				return nil
			}

			previous := object.Status.WorkflowStatus.DeepCopy()
			if err := domain.ReactivateWorkflow(
				&object.Status.WorkflowStatus,
				"copy resume requested",
				c.now(),
			); err != nil {
				return err
			}

			if err := persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return c.store.Save(ctx, object) },
			); err != nil {
				object.Status.WorkflowStatus = *previous
				return err
			}

			return nil
		})
}

func (c *ClusterCopyExecutor) FinalizeDeleted(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
) error {
	if object == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize copy",
			"workflow deletion is required",
		)
	}

	// Statuses written by older releases can carry Failed without a resume
	// checkpoint. Deletion is the final convergence pass and must not be
	// wedged by that era's validation gap.
	if object.DeletionTimestamp != nil &&
		object.Status.Phase == domain.PhaseFailed &&
		object.Status.ResumeFrom == "" {
		object.Status.ResumeFrom = domain.PhasePlanned
	}

	if err := validateClusterCopyObject(object); err != nil {
		return err
	}

	if object.DeletionTimestamp == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize copy",
			"workflow deletion is required",
		)
	}

	ctx = context.WithValue(ctx, workflowDeletionContextKey{}, true)

	return withStoredWorkflowLock(ctx, c.store, c.locker, c.storageNamespace, object,
		func(ctx context.Context) error {
			previous := object.Status.WorkflowStatus.DeepCopy()
			domain.SetWorkflowCondition(&object.Status.WorkflowStatus, v1alpha1.WorkflowCondition{
				Type: "Deleting", Status: metav1.ConditionTrue, Reason: "CleaningUp",
				Message:            "Stopping copy and releasing storage ownership before deletion",
				LastTransitionTime: metav1.NewTime(c.now().UTC()),
			})

			if err := persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return c.store.Save(ctx, object) },
			); err != nil {
				object.Status.WorkflowStatus = *previous
				return err
			}

			if object.Status.Phase != domain.PhaseWarmCopied &&
				object.Status.Phase != domain.PhaseAborted {
				if err := c.abort(ctx, object); err != nil {
					return err
				}
			}

			return c.cleanup(ctx, object, CopyCleanupOptions{Finalize: true, DeleteSession: true})
		})
}

func (c *ClusterCopyExecutor) ValidateAbort(object *v1alpha1.ClusterCopy) error {
	return validateClusterCopyObject(object)
}

// RequestCopyPass explicitly starts another pass after successful completion.
// Reconciliation alone never resets completed checkpoints or repeats a pass.
func (c *ClusterCopyExecutor) RequestCopyPass(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
) error {
	if err := validateClusterCopyObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, c.store, c.locker, c.storageNamespace, object,
		func(ctx context.Context) error {
			if object.Status.Phase != domain.PhaseWarmCopied {
				return domain.NewError(
					domain.ErrorPrecondition,
					"copy",
					"another pass requires a completed copy",
				)
			}

			if err := c.Validate(ctx, object); err != nil {
				return err
			}

			previous := object.Status.DeepCopy()
			for i := range object.Status.Volumes {
				object.Status.Volumes[i].Sync.WarmCompletedAt = nil
				object.Status.Volumes[i].Sync.LastError = ""
				object.Status.Volumes[i].Sync.BytesCopied = 0
			}

			if err := c.transition(
				ctx,
				object,
				domain.PhaseWarmCopying,
				"another copy pass requested",
			); err != nil {
				object.Status = *previous
				return err
			}

			return nil
		})
}

func (c *ClusterCopyExecutor) Abort(ctx context.Context, object *v1alpha1.ClusterCopy) error {
	if err := validateClusterCopyObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, c.store, c.locker, c.storageNamespace, object,
		func(ctx context.Context) error { return c.abort(ctx, object) })
}

func (c *ClusterCopyExecutor) abort(ctx context.Context, object *v1alpha1.ClusterCopy) error {
	if object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if object.Status.Plan == nil {
		return c.transition(
			ctx,
			object,
			domain.PhaseAborted,
			"copy aborted before execution planning",
		)
	}

	if err := c.transition(ctx, object, domain.PhaseAborting, "stopping copy tools"); err != nil {
		return err
	}

	if err := c.cleanupInterrupted(ctx, object); err != nil {
		return err
	}

	return c.transition(
		ctx,
		object,
		domain.PhaseAborted,
		"copy aborted; volumes are retained for cleanup",
	)
}

func (c *ClusterCopyExecutor) cleanupInterrupted(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
) error {
	plan := object.Status.Plan

	indexes := clusterCopyVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		index, exists := indexes[volume.SourcePVC.Name]
		if !exists {
			continue
		}

		checkpoint := object.Status.Volumes[index]
		if checkpoint.Sync.Attempts == 0 || checkpoint.Sync.WarmCompletedAt != nil {
			continue
		}

		if err := c.validateVolume(
			ctx,
			kube.ReservationRequest{
				SessionID:  object.Name,
				TargetNode: plan.TargetNode,
				ToolImage:  c.transfer.toolImage(plan.ToolImage),
			},
			string(plan.SourceNamespace),
			string(plan.DestinationNamespace),
			volume,
			*checkpoint.ClusterVolumeReservationStatus.DeepCopy(),
		); err != nil {
			return err
		}

		request := copyengine.CleanupRequest{
			SessionID: object.Name,
			Source: qualifiedResourceReference(
				volume.SourcePVC,
				string(plan.SourceNamespace),
			),
			DestinationNamespace: string(plan.DestinationNamespace),
			Mode:                 copyengine.ModeWarm,
			Attempt:              checkpoint.Sync.Attempts,
			Strategies:           plan.Strategies,
			KubeconfigPath:       c.transfer.config.KubeconfigPath,
			Context:              c.transfer.config.Context,
		}
		if err := c.transfer.copier.Cleanup(ctx, request); err != nil {
			return err
		}

		if err := c.transfer.cleanupCopyToolPods(ctx, request.Source, *checkpoint.DestinationPVC,
			copyengine.OperationID(request.AttemptIdentity)); err != nil {
			return err
		}
	}

	return nil
}
