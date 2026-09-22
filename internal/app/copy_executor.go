package app

import (
	"context"
	"errors"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type CopyExecutorConfig struct {
	Transfer            VolumeCopyConfig
	ToolImageProber     kube.ToolImageProber
	VolumeUsageReader   kube.VolumeUsageReader
	SharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
	ProbeTimeout        time.Duration
}

// ClusterCopyExecutor owns copy orchestration and uses only the Copy CRD for durable
// input and progress. The volume runner owns chart execution and retry mechanics.
type ClusterCopyExecutor struct {
	copyResources
	store            kube.WorkflowStore[*v1alpha1.ClusterCopy]
	locker           kube.SessionLocker
	storageNamespace string
	now              func() time.Time
}

func NewClusterCopyExecutor(
	client kubernetes.Interface,
	store kube.WorkflowStore[*v1alpha1.ClusterCopy],
	locker kube.SessionLocker,
	storageNamespace string,
	engine copyengine.Engine,
	config CopyExecutorConfig,
) *ClusterCopyExecutor {
	return &ClusterCopyExecutor{
		copyResources:    newCopyResources(client, engine, config),
		store:            store,
		locker:           locker,
		storageNamespace: storageNamespace,
		now:              time.Now,
	}
}

func (c *ClusterCopyExecutor) Run(ctx context.Context, object *v1alpha1.ClusterCopy) error {
	if err := validateClusterCopyObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, c.store, c.locker, c.storageNamespace, object,
		func(ctx context.Context) error { return c.run(ctx, object) })
}

func (c *ClusterCopyExecutor) run(ctx context.Context, object *v1alpha1.ClusterCopy) error {
	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if phase == domain.PhaseAborted || phase == domain.PhaseWarmCopied {
		return nil
	}

	if phase == domain.PhaseAborting {
		return c.abort(ctx, object)
	}

	if object.Status.Plan == nil {
		return domain.NewError(domain.ErrorPrecondition, "copy", "copy requires execution planning")
	}

	if err := validateCopyRetry(object.Status.FailureReason); err != nil {
		return err
	}

	if phase == domain.PhaseWarmCopying {
		if err := c.cleanupInterrupted(ctx, object); err != nil {
			return err
		}
	}

	node, err := c.validateResources(ctx, object)
	if err != nil {
		return c.fail(ctx, object, err)
	}

	if object.Status.SourceNode != node {
		previous := object.Status.SourceNode

		object.Status.SourceNode = node
		if err := persistCheckpoint(
			ctx,
			func(ctx context.Context) error { return c.store.Save(ctx, object) },
		); err != nil {
			object.Status.SourceNode = previous
			return err
		}
	}

	if phase == domain.PhasePlanned || phase == domain.PhaseReserving {
		if err := c.reserve(ctx, object); err != nil {
			return err
		}
	}

	return c.copyVolumes(ctx, object)
}

func (c *ClusterCopyExecutor) reserve(ctx context.Context, object *v1alpha1.ClusterCopy) error {
	plan := object.Status.Plan
	if c.config.ToolImageProber != nil && plan.TargetNode != "" {
		if _, err := c.probe(
			ctx,
			object.Name,
			plan.ToolImage,
			reservationToolProbeTargets(
				[]string{string(plan.DestinationNamespace)},
				plan.TargetNode,
			),
		); err != nil {
			return c.fail(ctx, object, err)
		}
	}

	previous := object.Status.DeepCopy()
	if len(object.Status.Volumes) == 0 {
		for _, volume := range plan.Volumes {
			object.Status.Volumes = append(
				object.Status.Volumes,
				v1alpha1.ClusterCopyVolumeStatus{SourcePVCName: volume.SourcePVC.Name},
			)
		}
	}

	if err := c.transition(
		ctx,
		object,
		domain.PhaseReserving,
		"reserving copy destination storage",
	); err != nil {
		object.Status = *previous
		return err
	}

	if err := kube.RequireNamespace(ctx, c.client, string(plan.DestinationNamespace)); err != nil {
		return c.fail(ctx, object, err)
	}

	indexes := clusterCopyVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Reserved {
			continue
		}

		desired, err := reservationManifest(
			qualifiedResourceReference(volume.DestinationPVC, string(plan.DestinationNamespace)),
			volume.Capacity,
			volume.StorageClass,
			volume.VolumeMode,
			volume.AccessModes,
		)
		if err != nil {
			return c.fail(ctx, object, err)
		}

		checkpoint := status.ClusterVolumeReservationStatus.DeepCopy()
		err = c.reserver.ReserveVolume(
			ctx,
			kube.ReservationRequest{
				SessionID:  object.Name,
				TargetNode: plan.TargetNode,
				ToolImage:  c.transfer.toolImage(plan.ToolImage),
			},
			qualifiedResourceReference(volume.SourcePVC, string(plan.SourceNamespace)),
			qualifiedResourceReference(
				volume.SourcePV,
				"",
			),
			volume.SourceCapacity,
			desired,
			checkpoint,
		)

		previous := status.DeepCopy()

		status.ClusterVolumeReservationStatus = *checkpoint.DeepCopy()
		if saveErr := persistCheckpoint(
			ctx,
			func(ctx context.Context) error { return c.store.Save(ctx, object) },
		); saveErr != nil {
			*status = *previous
			return errors.Join(err, saveErr)
		}

		if err != nil {
			return c.fail(ctx, object, err)
		}

		if !status.Reserved || status.DestinationPVC == nil || status.DestinationPV == nil {
			return c.fail(
				ctx,
				object,
				domain.NewError(
					domain.ErrorInternal,
					"copy",
					"reserver did not produce a complete checkpoint",
				),
			)
		}
	}

	return c.transition(ctx, object, domain.PhaseReserved, "copy destination storage is reserved")
}

func (c *ClusterCopyExecutor) transition(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
	next v1alpha1.WorkflowPhase,
	message string,
) error {
	if err := validateCopyTransition(
		workflowResumePhase(object.Status.WorkflowStatus),
		next,
		object.Status.Plan != nil,
	); err != nil {
		return err
	}

	previous := object.Status.WorkflowStatus.DeepCopy()
	if object.Status.StartedAt.IsZero() {
		object.Status.StartedAt = metav1.NewTime(c.now().UTC())
	}

	domain.RecordWorkflowTransition(&object.Status.WorkflowStatus, next, message, c.now())

	if err := persistCheckpoint(
		ctx,
		func(ctx context.Context) error { return c.store.Save(ctx, object) },
	); err != nil {
		object.Status.WorkflowStatus = *previous
		return err
	}

	return nil
}

func (c *ClusterCopyExecutor) fail(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
	cause error,
) error {
	return recordWorkflowFailure(
		ctx,
		&object.Status.WorkflowStatus,
		cause,
		failureReason(cause),
		c.now(),
		func(ctx context.Context) error { return c.store.Save(ctx, object) },
		func(ctx context.Context, next v1alpha1.WorkflowPhase, message string) error {
			return c.transition(ctx, object, next, message)
		},
	)
}
