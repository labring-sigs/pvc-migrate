package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ReservationExecutorConfig supplies execution services, never workflow input
// or durable state. The concrete CRD owns all reservation input and progress.
type ReservationExecutorConfig struct {
	VolumeUsageReader kube.VolumeUsageReader
	ToolImageProber   kube.ToolImageProber
	TrustedToolImage  string
	ProbeTimeout      time.Duration
	Writer            io.Writer
	Logger            *slog.Logger
	StreamToolLogs    bool
	StructuredLogs    bool
}

func newReservationReserver(
	client kubernetes.Interface,
	config ReservationExecutorConfig,
) *kube.Reserver {
	reserver := kube.NewReserver(client).
		WithTrustedToolImage(config.TrustedToolImage).
		WithLogger(config.Logger)
	if config.StreamToolLogs {
		reserver.WithToolLogs(
			kube.ToolLogOptions{
				Writer:     config.Writer,
				Logger:     config.Logger,
				Structured: config.StructuredLogs,
			},
		)
	}

	return reserver
}

type ClusterReservationExecutor struct {
	client           kubernetes.Interface
	reserver         volumeReserver
	store            kube.WorkflowStore[*v1alpha1.ClusterReservation]
	locker           kube.SessionLocker
	storageNamespace string
	config           ReservationExecutorConfig
	now              func() time.Time
}

func NewClusterReservationExecutor(
	client kubernetes.Interface,
	store kube.WorkflowStore[*v1alpha1.ClusterReservation],
	locker kube.SessionLocker,
	storageNamespace string,
	config ReservationExecutorConfig,
) *ClusterReservationExecutor {
	return &ClusterReservationExecutor{
		client:           client,
		reserver:         newReservationReserver(client, config),
		store:            store,
		locker:           locker,
		storageNamespace: storageNamespace,
		config:           config,
		now:              time.Now,
	}
}

// Run provisions destination storage and checkpoints only ClusterReservation
// status. Retrying a partial reservation reuses its persisted resource identities.
func (r *ClusterReservationExecutor) Run(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
) error {
	if err := validateClusterReservationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, r.store, r.locker, r.storageNamespace, object,
		func(ctx context.Context) error { return r.run(ctx, object) })
}

func (r *ClusterReservationExecutor) run(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
) error {
	switch workflowResumePhase(object.Status.WorkflowStatus) {
	case domain.PhaseAborted:
		return nil
	case domain.PhaseAborting:
		return r.abort(ctx, object)
	}

	if object.Status.Plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reservation",
			"reservation requires execution planning",
		)
	}

	if err := r.Validate(ctx, object); err != nil {
		return r.fail(ctx, object, err)
	}

	if object.Status.Phase == domain.PhaseReserved {
		return nil
	}

	plan := object.Status.Plan

	image := plan.ToolImage
	if trusted := strings.TrimSpace(r.config.TrustedToolImage); trusted != "" {
		image = trusted
	}

	if r.config.ToolImageProber != nil {
		if _, err := r.config.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
			OperationID: object.Name,
			Image:       image,
			Targets: reservationToolProbeTargets(
				[]string{string(plan.DestinationNamespace)},
				plan.TargetNode,
			),
			Timeout: r.config.ProbeTimeout,
			Writer:  r.config.Writer,
			Logger:  r.config.Logger,
		}); err != nil {
			return r.fail(ctx, object, err)
		}
	}

	previous := object.Status.DeepCopy()
	if len(object.Status.Volumes) == 0 {
		for _, volume := range plan.Volumes {
			object.Status.Volumes = append(
				object.Status.Volumes,
				v1alpha1.ClusterReservationVolumeStatus{
					SourcePVCName: volume.SourcePVC.Name,
				},
			)
		}
	}

	if err := r.transition(
		ctx,
		object,
		domain.PhaseReserving,
		"reserving destination storage",
	); err != nil {
		object.Status = *previous
		return err
	}

	if err := kube.RequireNamespace(ctx, r.client, string(plan.DestinationNamespace)); err != nil {
		return r.fail(ctx, object, err)
	}

	indexes := clusterReservationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
			return err
		}

		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Reserved {
			continue
		}

		desired, err := reservationManifest(
			qualifiedResourceReference(volume.DestinationPVC, string(plan.DestinationNamespace)),
			volume.Capacity, volume.StorageClass, volume.VolumeMode, volume.AccessModes,
		)
		if err != nil {
			return r.fail(ctx, object, err)
		}

		checkpoint := status.ClusterVolumeReservationStatus.DeepCopy()
		err = r.reserver.ReserveVolume(
			ctx,
			kube.ReservationRequest{
				SessionID: object.Name, TargetNode: plan.TargetNode, ToolImage: image,
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
			func(ctx context.Context) error { return r.store.Save(ctx, object) },
		); saveErr != nil {
			*status = *previous
			return errors.Join(err, saveErr)
		}

		if err != nil {
			return r.fail(ctx, object, err)
		}

		if !status.Reserved || status.DestinationPVC == nil || status.DestinationPV == nil {
			return r.fail(
				ctx,
				object,
				domain.NewError(
					domain.ErrorInternal,
					"reservation",
					"reserver did not produce a complete checkpoint",
				),
			)
		}
	}

	return r.transition(
		ctx,
		object,
		domain.PhaseReserved,
		"destination storage is provisioned and retained",
	)
}

// Validate checks reservation resources without changing the CRD or the cluster.
func (r *ClusterReservationExecutor) Validate(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
) error {
	if err := validateClusterReservationObject(object); err != nil {
		return err
	}

	if phase := workflowResumePhase(
		object.Status.WorkflowStatus,
	); phase == domain.PhaseAborted ||
		phase == domain.PhaseAborting {
		return nil
	}

	plan := object.Status.Plan
	if plan == nil {
		return nil
	}

	indexes := clusterReservationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		source := qualifiedResourceReference(volume.SourcePVC, string(plan.SourceNamespace))

		pv := qualifiedResourceReference(volume.SourcePV, "")
		if err := kube.VerifyVolumeShrinkUsage(
			ctx,
			r.config.VolumeUsageReader,
			r.config.Logger,
			source,
			pv,
			volume.SourceCapacity,
			volume.Capacity,
			domain.SourceTransferPath(
				volume.TransferScope,
			),
			plan.SkipSourceUsageCheck,
		); err != nil {
			return err
		}

		desired, err := reservationManifest(
			qualifiedResourceReference(volume.DestinationPVC, string(plan.DestinationNamespace)),
			volume.Capacity, volume.StorageClass, volume.VolumeMode, volume.AccessModes,
		)
		if err != nil {
			return err
		}

		checkpoint := v1alpha1.ClusterVolumeReservationStatus{SourcePVCName: volume.SourcePVC.Name}
		if len(object.Status.Volumes) != 0 {
			checkpoint = *object.Status.Volumes[indexes[volume.SourcePVC.Name]].ClusterVolumeReservationStatus.DeepCopy()
		}

		if err := r.reserver.ValidateVolumeReservation(ctx, kube.ReservationRequest{
			SessionID: object.Name, TargetNode: plan.TargetNode, ToolImage: plan.ToolImage,
		}, source, pv, volume.SourceCapacity, desired, checkpoint); err != nil {
			return err
		}
	}

	return nil
}

func (r *ClusterReservationExecutor) transition(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
	next v1alpha1.WorkflowPhase,
	message string,
) error {
	if err := validateReservationTransition(
		object.Status.Phase,
		next,
		object.Status.Plan != nil,
	); err != nil {
		return err
	}

	previous := object.Status.WorkflowStatus.DeepCopy()
	if object.Status.StartedAt.IsZero() {
		object.Status.StartedAt = metav1.NewTime(r.now().UTC())
	}

	domain.RecordWorkflowTransition(&object.Status.WorkflowStatus, next, message, r.now())

	if err := persistCheckpoint(
		ctx,
		func(ctx context.Context) error { return r.store.Save(ctx, object) },
	); err != nil {
		object.Status.WorkflowStatus = *previous
		return err
	}

	return nil
}

func (r *ClusterReservationExecutor) fail(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
	cause error,
) error {
	return recordWorkflowFailure(ctx, &object.Status.WorkflowStatus, cause, "", r.now(),
		func(ctx context.Context) error { return r.store.Save(ctx, object) },
		func(ctx context.Context, phase v1alpha1.WorkflowPhase, message string) error {
			return r.transition(ctx, object, phase, message)
		})
}
