package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *ClusterPodMigrationExecutor) Activate(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object,
		func(ctx context.Context) error { return m.activate(ctx, object) })
}

func (m *ClusterPodMigrationExecutor) ValidateActivation(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if err := validateClusterPodMigrationObject(object); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if phase == domain.PhaseActivated || phase == domain.PhaseCompleted ||
		phase == domain.PhaseResuming {
		return m.verifyActiveVolumes(ctx, object)
	}

	if phase != domain.PhaseFinalSynced && phase != domain.PhaseActivating {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pod migration activation",
			"migration must complete final sync before activation",
		)
	}

	plan := object.Status.Plan

	if m.workloads == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pod migration activation",
			"workload controller is required",
		)
	}

	if err := m.workloads.VerifyPaused(
		ctx,
		object.Name,
		string(plan.SourceNamespace),
		clusterPodWorkload(plan.Workload, object.Status.Workload),
		object.Status.Phase,
		object.Status.ResumeFrom,
	); err != nil {
		return err
	}

	indexes := clusterPodMigrationVolumeIndexes(object.Status.Volumes)
	groups := make(map[string][]kube.PVCAdmissionChange)

	var offline []kube.PVCTransferBindings
	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]

		binding := plannedMigrationBindings(
			string(plan.SourceNamespace),
			volume,
			status.ClusterVolumeReservationStatus,
		)
		if status.Activation.ActivatedAt != nil || status.Activation.ActivePVC != nil {
			if err := verifyActiveStorageVolume(
				ctx,
				m.client,
				object.Name,
				binding.SourcePVC,
				binding.DestinationPV,
				status.Activation.ActivePVC,
			); err != nil {
				return err
			}

			continue
		}

		active, found, err := unrecordedActivePVC(
			ctx,
			m.client,
			binding.SourcePVC,
			binding.SourcePV,
		)
		if err != nil {
			return err
		}

		if active != nil {
			if err := verifyActiveStorageVolume(
				ctx,
				m.client,
				object.Name,
				binding.SourcePVC,
				binding.DestinationPV,
				active,
			); err != nil {
				return err
			}

			if err := m.switcher.VerifyVolumesOfflineForSession(
				ctx,
				object.Name,
				[]kube.PVCTransferBindings{
					{
						SourcePVC:      *active,
						SourcePV:       binding.DestinationPV,
						DestinationPVC: *active,
						DestinationPV:  binding.DestinationPV,
					},
				},
			); err != nil {
				return err
			}

			continue
		}

		change, err := migrationActivationAdmission(
			string(plan.SourceNamespace),
			volume,
			found && !status.Activation.SourcePVCDeleted,
		)
		if err != nil {
			return err
		}

		groups[string(plan.SourceNamespace)] = append(groups[string(plan.SourceNamespace)], change)
		offline = append(offline, binding)
	}

	if err := validateMigrationPVCAdmission(ctx, m.client, groups); err != nil {
		return err
	}

	if phase == domain.PhaseActivating || object.Status.Phase == domain.PhaseFailed {
		return m.switcher.VerifyActivationRecovery(ctx, object.Name, offline)
	}

	return m.switcher.VerifyVolumesOfflineForSession(ctx, object.Name, offline)
}

func (m *ClusterPodMigrationExecutor) activate(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	if object.Status.Plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pod migration activation",
			"pod migration requires execution planning",
		)
	}

	plan := object.Status.Plan
	if err := m.restoreSharedMounts(
		ctx,
		object.Name,
		[]string{string(plan.SourceNamespace), string(plan.TemporaryNamespace)},
		&object.Status.OpenEBSLVMSharedMounts,
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
	); err != nil {
		return err
	}

	if err := m.ValidateActivation(ctx, object); err != nil {
		return err
	}

	if workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseResuming ||
		object.Status.Phase == domain.PhaseActivated ||
		object.Status.Phase == domain.PhaseCompleted {
		return nil
	}

	if workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseCompleted {
		return m.transition(ctx, object, domain.PhaseCompleted, "migration completion revalidated")
	}

	if workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseActivated {
		return m.transition(ctx, object, domain.PhaseActivated, "migration activation revalidated")
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseActivating,
		"activating pod migration volumes",
	); err != nil {
		return err
	}

	indexes := clusterPodMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		binding := plannedMigrationBindings(
			string(plan.SourceNamespace),
			volume,
			status.ClusterVolumeReservationStatus,
		)

		desired, err := migrationPVCManifest(
			object.Name,
			binding.SourcePVC,
			binding.SourcePV,
			binding.DestinationPV,
			volume.SourcePVCSpec,
			volume.SourcePVCMetadata,
			volume.StorageClass,
			volume.Capacity,
		)
		if err != nil {
			return m.fail(ctx, object, err)
		}

		if err := activateMigrationVolume(
			ctx,
			m.client,
			m.switcher,
			object.Name,
			binding,
			desired,
			&status.Activation,
			func(ctx context.Context) error { return m.store.Save(ctx, object) },
		); err != nil {
			return m.fail(ctx, object, err)
		}
	}

	return m.transition(ctx, object, domain.PhaseActivated, "pod migration volumes are active")
}

func (m *ClusterPodMigrationExecutor) verifyActiveVolumes(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	plan := object.Status.Plan

	indexes := clusterPodMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]

		binding := plannedMigrationBindings(
			string(plan.SourceNamespace),
			volume,
			status.ClusterVolumeReservationStatus,
		)
		if err := verifyActiveStorageVolume(
			ctx,
			m.client,
			object.Name,
			binding.SourcePVC,
			binding.DestinationPV,
			status.Activation.ActivePVC,
		); err != nil {
			return err
		}
	}

	return nil
}
