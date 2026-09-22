package app

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (c *CopyExecutor) copyVolumes(ctx context.Context, object *v1alpha1.Copy) error {
	if err := validateCopyObject(object); err != nil {
		return err
	}

	plan := object.Status.Plan

	targets, err := c.probeTargets(
		ctx,
		plan,
		object.Namespace,
		object.Namespace,
		object.Status.SourceNode,
	)
	if err != nil {
		return c.fail(ctx, object, err)
	}

	results, err := c.probe(ctx, object.Name, plan.ToolImage, targets)
	if err != nil {
		return c.fail(ctx, object, warmCopyProbeError(domain.OperationCopy, targets, err))
	}

	if err := c.transition(
		ctx,
		object,
		domain.PhaseWarmCopying,
		"copying reserved volumes",
	); err != nil {
		return err
	}

	indexes := copyVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Sync.WarmCompletedAt != nil {
			continue
		}

		source := qualifiedResourceReference(volume.SourcePVC, object.Namespace)

		request := copyengine.CopyRequest{
			AttemptIdentity: copyengine.AttemptIdentity{
				SessionID: object.Name, Source: source, Mode: copyengine.ModeWarm,
			},
			Source: copyengine.CopySource{
				Path: domain.SourceTransferPath(volume.TransferScope),
			},
			Destination: copyengine.CopyDestination{
				Reference: qualifiedResourceReference(*status.DestinationPVC, object.Namespace),
				Path:      domain.DestinationTransferPath(volume.TransferScope),
			},
			Policy: copyengine.CopyPolicy{
				IgnoreSizes: destinationCapacityIsSmaller(
					volume.SourceCapacity,
					volume.Capacity,
				),
				Strategies:            slices.Clone(plan.Strategies),
				DeleteExtraneousFiles: plan.DeleteExtraneous,
				VerifyChecksum:        plan.VerifyChecksum,
			},
			Runtime: copyengine.CopyRuntime{ToolImage: plan.ToolImage},
		}

		err := c.transfer.copyWithRetry(ctx, request, object.Status.SourceNode, plan.TargetNode, "",
			&status.Sync.Attempts, &status.Sync.LastError, results,
			func(ctx context.Context) error { return c.store.Save(ctx, object) },
			func(ctx context.Context) error {
				if err := c.validateConsumers(
					ctx,
					source,
					volume.AccessModes,
					plan.Online,
					object.Status.SourceNode,
				); err != nil {
					return err
				}

				return c.validateVolume(
					ctx,
					kube.ReservationRequest{
						SessionID:  object.Name,
						TargetNode: plan.TargetNode,
						ToolImage:  c.transfer.toolImage(plan.ToolImage),
					},
					object.Namespace,
					object.Namespace,
					volume,
					qualifiedReservationCheckpoint(
						status.VolumeReservationStatus,
						object.Namespace,
					),
				)
			},
			func(ctx context.Context) (bool, error) {
				_, shared, err := c.sharedSource(
					ctx,
					qualifiedResourceReference(volume.SourcePV, ""),
				)

				return shared, err
			})
		if err != nil {
			return c.fail(ctx, object, err)
		}

		if err := checkpointWarmCopy(
			ctx,
			&status.Sync.WarmCompletedAt,
			&status.Sync.LastError,
			c.now(),
			func(ctx context.Context) error { return c.store.Save(ctx, object) },
		); err != nil {
			return err
		}
	}

	return c.transition(ctx, object, domain.PhaseWarmCopied, "copy completed for all volumes")
}
