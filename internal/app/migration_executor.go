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

// MigrationExecutorConfig supplies process services. Migration input and all
// durable execution state belong to the concrete CRD.
type MigrationExecutorConfig struct {
	Transfer          VolumeCopyConfig
	VolumeUsageReader kube.VolumeUsageReader
	ToolImageProber   kube.ToolImageProber
	ProbeTimeout      time.Duration
}

type ClusterMigrationExecutor struct {
	migrationResources
	store            kube.WorkflowStore[*v1alpha1.ClusterMigration]
	locker           kube.SessionLocker
	storageNamespace string
	now              func() time.Time
}

func NewClusterMigrationExecutor(
	client kubernetes.Interface,
	store kube.WorkflowStore[*v1alpha1.ClusterMigration],
	locker kube.SessionLocker,
	storageNamespace string,
	engine copyengine.Engine,
	config MigrationExecutorConfig,
) *ClusterMigrationExecutor {
	return &ClusterMigrationExecutor{
		migrationResources: newMigrationResources(client, engine, config),
		store:              store,
		locker:             locker,
		storageNamespace:   storageNamespace,
		now:                time.Now,
	}
}

// migrationResources supplies resource capabilities for the two API scopes.
// Each executor owns its concrete CRD, lifecycle and persistence.
type migrationResources struct {
	client   kubernetes.Interface
	reserver volumeReserver
	switcher volumeSwitcher
	transfer *volumeCopyRunner
	config   MigrationExecutorConfig
}

func newMigrationResources(
	client kubernetes.Interface,
	engine copyengine.Engine,
	config MigrationExecutorConfig,
) migrationResources {
	transfer := newVolumeCopyRunner(client, engine, config.Transfer)

	config.Transfer = transfer.config
	if config.ProbeTimeout <= 0 {
		config.ProbeTimeout = transfer.config.HelmTimeout
	}

	return migrationResources{
		client:   client,
		config:   config,
		transfer: transfer,
		switcher: kube.NewSwitcher(client).WithLogger(config.Transfer.Logger),
		reserver: newReservationReserver(client, ReservationExecutorConfig{
			TrustedToolImage: config.Transfer.TrustedToolImage, Logger: config.Transfer.Logger,
			Writer: config.Transfer.Writer, StreamToolLogs: config.Transfer.StreamToolLogs,
			StructuredLogs: config.Transfer.StructuredLogs,
		}),
	}
}

func (m *ClusterMigrationExecutor) Reserve(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
) error {
	if err := validateClusterMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object,
		func(ctx context.Context) error { return m.reserve(ctx, object) })
}

func (m *ClusterMigrationExecutor) reserve(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
) error {
	if err := m.ValidateReservation(ctx, object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseReserved {
		return nil
	}

	if object.Status.Phase == domain.PhaseFailed &&
		object.Status.ResumeFrom == domain.PhaseReserved {
		return m.transition(ctx, object, domain.PhaseReserved, "migration reservation revalidated")
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
				v1alpha1.ClusterMigrationVolumeStatus{
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

	indexes := clusterMigrationVolumeIndexes(object.Status.Volumes)
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

	return m.transition(
		ctx,
		object,
		domain.PhaseReserved,
		"migration destination storage is provisioned and retained",
	)
}

func (m *ClusterMigrationExecutor) ValidateReservation(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
) error {
	if err := validateClusterMigrationObject(object); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if phase != domain.PhasePlanned && phase != domain.PhaseReserving &&
		phase != domain.PhaseReserved {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve migration",
			"migration phase cannot reserve storage",
		)
	}

	plan := object.Status.Plan
	if plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve migration",
			"migration requires execution planning",
		)
	}

	indexes := clusterMigrationVolumeIndexes(object.Status.Volumes)
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

func (m *ClusterMigrationExecutor) transition(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
	next v1alpha1.WorkflowPhase,
	message string,
) error {
	if err := validateMigrationTransition(
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

func (m *ClusterMigrationExecutor) fail(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
	cause error,
) error {
	reason := ""
	if workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseFinalSyncing {
		reason = failureReason(cause)
	}

	return recordWorkflowFailure(ctx, &object.Status.WorkflowStatus, cause, reason, m.now(),
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
		func(ctx context.Context, phase v1alpha1.WorkflowPhase, message string) error {
			return m.transition(ctx, object, phase, message)
		})
}
