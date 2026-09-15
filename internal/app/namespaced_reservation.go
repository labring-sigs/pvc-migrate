package app

import (
	"context"
	"errors"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ReservationExecutor owns the namespaced API. Every PVC and checkpoint is
// resolved within metadata.namespace; it never constructs a ClusterReservation.
type ReservationExecutor struct {
	client   kubernetes.Interface
	reserver volumeReserver
	store    kube.WorkflowStore[*v1alpha1.Reservation]
	locker   kube.SessionLocker
	config   ReservationExecutorConfig
	now      func() time.Time
}

func NewReservationExecutor(
	client kubernetes.Interface,
	store kube.WorkflowStore[*v1alpha1.Reservation],
	locker kube.SessionLocker,
	config ReservationExecutorConfig,
) *ReservationExecutor {
	return &ReservationExecutor{
		client:   client,
		store:    store,
		locker:   locker,
		config:   config,
		now:      time.Now,
		reserver: newReservationReserver(client, config),
	}
}

func (r *ReservationExecutor) Run(ctx context.Context, object *v1alpha1.Reservation) error {
	if err := validateReservationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, r.store, r.locker, object.Namespace, object,
		func(ctx context.Context) error { return r.run(ctx, object) })
}

func (r *ReservationExecutor) run(ctx context.Context, object *v1alpha1.Reservation) error {
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
			OperationID: object.Name, Image: image,
			Targets: reservationToolProbeTargets([]string{object.Namespace}, plan.TargetNode),
			Timeout: r.config.ProbeTimeout, Writer: r.config.Writer, Logger: r.config.Logger,
		}); err != nil {
			return r.fail(ctx, object, err)
		}
	}

	previous := object.Status.DeepCopy()
	if len(object.Status.Volumes) == 0 {
		for _, volume := range plan.Volumes {
			object.Status.Volumes = append(
				object.Status.Volumes,
				v1alpha1.ReservationVolumeStatus{SourcePVCName: volume.SourcePVC.Name},
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

	if err := kube.RequireNamespace(ctx, r.client, object.Namespace); err != nil {
		return r.fail(ctx, object, err)
	}

	indexes := reservationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if status.Reserved {
			continue
		}

		desired, err := reservationManifest(
			qualifiedResourceReference(volume.DestinationPVC, object.Namespace),
			volume.Capacity,
			volume.StorageClass,
			volume.VolumeMode,
			volume.AccessModes,
		)
		if err != nil {
			return r.fail(ctx, object, err)
		}

		checkpoint := qualifiedReservationCheckpoint(
			status.VolumeReservationStatus,
			object.Namespace,
		)
		err = r.reserver.ReserveVolume(
			ctx,
			kube.ReservationRequest{
				SessionID:  object.Name,
				TargetNode: plan.TargetNode,
				ToolImage:  image,
			},
			qualifiedResourceReference(
				volume.SourcePVC,
				object.Namespace,
			),
			qualifiedResourceReference(volume.SourcePV, ""),
			volume.SourceCapacity,
			desired,
			&checkpoint,
		)

		local, scopeErr := localReservationCheckpoint(checkpoint, object.Namespace)
		if scopeErr != nil {
			return r.fail(ctx, object, errors.Join(err, scopeErr))
		}

		previous := status.DeepCopy()

		status.VolumeReservationStatus = local
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

func (r *ReservationExecutor) Validate(ctx context.Context, object *v1alpha1.Reservation) error {
	if err := validateReservationObject(object); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if phase == domain.PhaseAborted || phase == domain.PhaseAborting || object.Status.Plan == nil {
		return nil
	}

	plan := object.Status.Plan

	indexes := reservationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		source, pv := qualifiedResourceReference(
			volume.SourcePVC,
			object.Namespace,
		), qualifiedResourceReference(
			volume.SourcePV,
			"",
		)
		if err := kube.VerifyVolumeShrinkUsage(
			ctx,
			r.config.VolumeUsageReader,
			r.config.Logger,
			source,
			pv,
			volume.SourceCapacity,
			volume.Capacity,
			domain.SourceTransferPath(volume.TransferScope),
			plan.SkipSourceUsageCheck,
		); err != nil {
			return err
		}

		desired, err := reservationManifest(
			qualifiedResourceReference(volume.DestinationPVC, object.Namespace),
			volume.Capacity,
			volume.StorageClass,
			volume.VolumeMode,
			volume.AccessModes,
		)
		if err != nil {
			return err
		}

		checkpoint := v1alpha1.ClusterVolumeReservationStatus{SourcePVCName: volume.SourcePVC.Name}
		if len(object.Status.Volumes) != 0 {
			checkpoint = qualifiedReservationCheckpoint(
				object.Status.Volumes[indexes[volume.SourcePVC.Name]].VolumeReservationStatus,
				object.Namespace,
			)
		}

		if err := r.reserver.ValidateVolumeReservation(
			ctx,
			kube.ReservationRequest{
				SessionID:  object.Name,
				TargetNode: plan.TargetNode,
				ToolImage:  plan.ToolImage,
			},
			source,
			pv,
			volume.SourceCapacity,
			desired,
			checkpoint,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *ReservationExecutor) transition(
	ctx context.Context,
	object *v1alpha1.Reservation,
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

func (r *ReservationExecutor) fail(
	ctx context.Context,
	object *v1alpha1.Reservation,
	cause error,
) error {
	return recordWorkflowFailure(ctx, &object.Status.WorkflowStatus, cause, "", r.now(),
		func(ctx context.Context) error { return r.store.Save(ctx, object) },
		func(ctx context.Context, phase v1alpha1.WorkflowPhase, message string) error {
			return r.transition(ctx, object, phase, message)
		})
}
