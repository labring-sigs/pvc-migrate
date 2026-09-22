package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *MigrationExecutor) Activate(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error { return m.activate(ctx, object) })
}

func (m *MigrationExecutor) ValidateActivation(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
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
			"migration activation",
			"migration must complete final sync before activation",
		)
	}

	plan := object.Status.Plan
	indexes := migrationVolumeIndexes(object.Status.Volumes)
	groups := make(map[string][]kube.PVCAdmissionChange)

	var offline []kube.PVCTransferBindings
	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]

		binding := namespacedMigrationBindings(
			object.Namespace,
			volume,
			status.VolumeReservationStatus,
		)
		if status.Activation.ActivatedAt != nil || status.Activation.ActivePVC != nil {
			if err := verifyActiveStorageVolume(
				ctx,
				m.client,
				object.Name,
				binding.SourcePVC,
				binding.DestinationPV,
				qualifiedOptionalReference(status.Activation.ActivePVC, object.Namespace),
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
			object.Namespace,
			volume,
			found && !status.Activation.SourcePVCDeleted,
		)
		if err != nil {
			return err
		}

		groups[object.Namespace] = append(groups[object.Namespace], change)
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

func (m *MigrationExecutor) activate(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := m.ValidateActivation(ctx, object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseActivated ||
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
		"activating offline migration volumes",
	); err != nil {
		return err
	}

	plan := object.Status.Plan

	indexes := migrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		binding := namespacedMigrationBindings(
			object.Namespace,
			volume,
			status.VolumeReservationStatus,
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

		checkpoint := qualifiedActivationCheckpoint(status.Activation, object.Namespace)
		if err := activateMigrationVolume(
			ctx,
			m.client,
			m.switcher,
			object.Name,
			binding,
			desired,
			&checkpoint,
			func(ctx context.Context) error {
				local, err := localActivationCheckpoint(checkpoint, object.Namespace)
				if err != nil {
					return err
				}

				previous := status.Activation.DeepCopy()

				status.Activation = local
				if err := persistCheckpoint(ctx, func(ctx context.Context) error {
					return m.store.Save(ctx, object)
				}); err != nil {
					status.Activation = *previous
					return err
				}

				return nil
			},
		); err != nil {
			return m.fail(ctx, object, err)
		}
	}

	return m.transition(ctx, object, domain.PhaseActivated, "offline migration volumes are active")
}

func (m *MigrationExecutor) verifyActiveVolumes(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	plan := object.Status.Plan

	indexes := migrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]

		binding := namespacedMigrationBindings(
			object.Namespace,
			volume,
			status.VolumeReservationStatus,
		)
		if err := verifyActiveStorageVolume(
			ctx,
			m.client,
			object.Name,
			binding.SourcePVC,
			binding.DestinationPV,
			qualifiedOptionalReference(status.Activation.ActivePVC, object.Namespace),
		); err != nil {
			return err
		}
	}

	return nil
}
