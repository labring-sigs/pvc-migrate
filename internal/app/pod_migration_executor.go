package app

import (
	"context"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ClusterPodMigrationExecutor owns workload migration state in its concrete CRD.
// Resource services do not carry operation inputs or durable state.
type ClusterPodMigrationExecutor struct {
	podMigrationResources
	store            kube.WorkflowStore[*v1alpha1.ClusterPodMigration]
	locker           kube.SessionLocker
	storageNamespace string
	now              func() time.Time
}

func NewClusterPodMigrationExecutor(
	client kubernetes.Interface,
	store kube.WorkflowStore[*v1alpha1.ClusterPodMigration],
	locker kube.SessionLocker,
	storageNamespace string,
	engine copyengine.Engine,
	config PodMigrationExecutorConfig,
) *ClusterPodMigrationExecutor {
	return &ClusterPodMigrationExecutor{
		podMigrationResources: newPodMigrationResources(client, engine, config),
		store:                 store,
		locker:                locker,
		storageNamespace:      storageNamespace,
		now:                   time.Now,
	}
}

func (m *ClusterPodMigrationExecutor) Reserve(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object,
		func(ctx context.Context) error { return m.reserve(ctx, object) })
}

func (m *ClusterPodMigrationExecutor) reserve(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := m.ValidateReservation(ctx, object); err != nil {
		return err
	}

	if err := m.restoreSharedMounts(
		ctx,
		object.Name,
		[]string{
			string(object.Status.Plan.SourceNamespace),
			string(object.Status.Plan.TemporaryNamespace),
		},
		&object.Status.OpenEBSLVMSharedMounts,
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
	); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseReserved {
		return nil
	}

	if object.Status.Phase == domain.PhaseFailed &&
		object.Status.ResumeFrom == domain.PhaseReserved {
		return m.transition(
			ctx,
			object,
			domain.PhaseReserved,
			"pod migration reservation revalidated",
		)
	}

	plan := object.Status.Plan
	if m.config.ToolImageProber != nil {
		if _, err := m.config.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
			OperationID: object.Name,
			Image:       m.transfer.toolImage(plan.ToolImage),
			Targets: reservationToolProbeTargets(
				[]string{string(plan.TemporaryNamespace)},
				plan.TargetNode,
			),
			Timeout: m.config.ProbeTimeout,
			Writer:  m.config.Transfer.Writer,
			Logger:  m.config.Transfer.Logger,
		}); err != nil {
			return err
		}
	}

	previous := object.Status.DeepCopy()
	if len(object.Status.Volumes) == 0 {
		for _, volume := range plan.Volumes {
			object.Status.Volumes = append(
				object.Status.Volumes,
				v1alpha1.ClusterPodMigrationVolumeStatus{
					ClusterVolumeReservationStatus: v1alpha1.ClusterVolumeReservationStatus{
						SourcePVCName: volume.SourcePVC.Name,
					},
				},
			)
		}
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseReserving,
		"reserving migration destination storage",
	); err != nil {
		object.Status = *previous
		return err
	}

	if err := kube.RequireNamespace(ctx, m.client, string(plan.TemporaryNamespace)); err != nil {
		return m.fail(ctx, object, err)
	}

	indexes := clusterPodMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]].ClusterVolumeReservationStatus
		if status.Reserved {
			continue
		}

		desired, err := reservationManifest(
			qualifiedResourceReference(volume.DestinationPVC, string(plan.TemporaryNamespace)),
			volume.Capacity, volume.StorageClass, volume.VolumeMode, volume.AccessModes,
		)
		if err != nil {
			return m.fail(ctx, object, err)
		}

		if err := reserveVolumeCheckpoint(ctx, m.reserver, kube.ReservationRequest{
			SessionID:  object.Name,
			TargetNode: plan.TargetNode,
			ToolImage:  m.transfer.toolImage(plan.ToolImage),
		}, qualifiedResourceReference(volume.SourcePVC, string(plan.SourceNamespace)),
			qualifiedResourceReference(volume.SourcePV, ""), volume.SourceCapacity, desired, status,
			func(ctx context.Context) error { return m.store.Save(ctx, object) }); err != nil {
			return m.fail(ctx, object, err)
		}
	}

	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if err := m.ensureDestinationMount(
			ctx,
			volume.AccessModes,
			volume.ConcurrentConsumers,
			*status.DestinationPVC,
			*status.DestinationPV,
			plan.OpenEBSLVMEnableShared,
		); err != nil {
			return m.fail(ctx, object, err)
		}
	}

	return m.transition(
		ctx,
		object,
		domain.PhaseReserved,
		"pod migration destination storage is provisioned and retained",
	)
}

func (m *ClusterPodMigrationExecutor) ValidateReservation(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if phase != domain.PhasePlanned && phase != domain.PhaseReserving &&
		phase != domain.PhaseReserved {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve pod migration",
			"pod migration phase cannot reserve storage",
		)
	}

	plan := object.Status.Plan
	if plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve pod migration",
			"pod migration requires execution planning",
		)
	}

	indexes := clusterPodMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		checkpoint := v1alpha1.ClusterVolumeReservationStatus{SourcePVCName: volume.SourcePVC.Name}
		if index, exists := indexes[volume.SourcePVC.Name]; exists {
			checkpoint = *object.Status.Volumes[index].ClusterVolumeReservationStatus.DeepCopy()
		}

		if err := m.validateReservationVolume(ctx, kube.ReservationRequest{
			SessionID:  object.Name,
			TargetNode: plan.TargetNode,
			ToolImage:  m.transfer.toolImage(plan.ToolImage),
		}, string(plan.SourceNamespace), string(plan.TemporaryNamespace), volume, checkpoint, plan.SkipSourceUsageCheck); err != nil {
			return err
		}
	}

	return nil
}

func (m *ClusterPodMigrationExecutor) transition(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
	next v1alpha1.WorkflowPhase,
	message string,
) error {
	if err := validatePodMigrationTransition(
		workflowResumePhase(object.Status.WorkflowStatus),
		next,
	); err != nil {
		return err
	}

	previous := object.Status.WorkflowStatus.DeepCopy()
	if object.Status.StartedAt.IsZero() {
		object.Status.StartedAt = metav1.NewTime(m.now().UTC())
	}

	domain.RecordWorkflowTransition(&object.Status.WorkflowStatus, next, message, m.now())

	if err := persistCheckpoint(
		ctx,
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
	); err != nil {
		object.Status.WorkflowStatus = *previous
		return err
	}

	return nil
}

func (m *ClusterPodMigrationExecutor) fail(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
	cause error,
) error {
	reason := ""
	if workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseFinalSyncing ||
		workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseWarmCopying {
		reason = failureReason(cause)
	}

	return recordWorkflowFailure(ctx, &object.Status.WorkflowStatus, cause, reason, m.now(),
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
		func(ctx context.Context, phase v1alpha1.WorkflowPhase, message string) error {
			return m.transition(ctx, object, phase, message)
		})
}
