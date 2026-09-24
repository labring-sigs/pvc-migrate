package app

import (
	"context"
	"errors"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *PodMigrationExecutor) Rollback(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error { return m.rollback(ctx, object) })
}

func (m *PodMigrationExecutor) ValidateRollback(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if err := validatePodMigrationObject(object); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if phase == domain.PhaseRolledBack {
		return m.verifyRestoredVolumes(ctx, object)
	}

	switch phase {
	case domain.PhaseFinalSynced,
		domain.PhaseActivating,
		domain.PhaseActivated,
		domain.PhaseResuming,
		domain.PhaseCompleted,
		domain.PhaseRollingBack:
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback migration",
			"migration phase cannot roll back",
		)
	}

	if m.workloads == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"rollback pod migration",
			"workload controller is required",
		)
	}

	if m.rollbackVolumesRestored(object) {
		return m.verifyRestoredVolumes(ctx, object)
	}

	plan := object.Status.Plan
	if _, err := m.rollbackWorkloadCheckpoint(
		ctx,
		object.Name,
		object.Namespace,
		podWorkload(plan.Workload, object.Status.Workload),
		plan.Volumes,
	); err != nil {
		return err
	}

	if !podRollbackNeedsPause(
		object.Status.Phase,
		object.Status.ResumeFrom,
		object.Status.History,
	) {
		if err := m.workloads.VerifyPaused(
			ctx,
			object.Name,
			object.Namespace,
			podWorkload(plan.Workload, object.Status.Workload),
			object.Status.Phase,
			object.Status.ResumeFrom,
		); err != nil {
			return err
		}
	}

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)

	var offline []kube.PVCTransferBindings
	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]

		binding := namespacedMigrationBindings(
			object.Namespace,
			volume,
			status.VolumeReservationStatus,
		)
		if status.Activation.RolledBackAt != nil {
			if err := verifyRollbackStorageVolume(
				ctx,
				m.client,
				object.Name,
				binding.SourcePVC,
				binding.SourcePV,
				qualifiedOptionalReference(status.Activation.ActivePVC, object.Namespace),
			); err != nil {
				return err
			}

			continue
		}

		if phase == domain.PhaseRollingBack {
			recovered, err := validateUnrecordedRollbackStorage(
				ctx,
				m.client,
				m.switcher,
				object.Name,
				binding.SourcePVC,
				binding.SourcePV,
				binding.DestinationPV,
				qualifiedOptionalReference(status.Activation.ActivePVC, object.Namespace),
			)
			if err != nil {
				return err
			}

			if recovered {
				continue
			}
		}

		if status.Activation.ActivatedAt != nil || status.Activation.ActivePVC != nil {
			if err := verifyActiveStorageVolume(
				ctx,
				m.client,
				object.Name,
				binding.SourcePVC,
				object.Namespace,
				binding.DestinationPV,
				qualifiedOptionalReference(status.Activation.ActivePVC, object.Namespace),
			); err != nil {
				return err
			}

			continue
		}

		if phase == domain.PhaseActivating {
			active, _, err := unrecordedActivePVC(
				ctx,
				m.client,
				binding.SourcePVC,
				binding.SourcePV,
				object.Namespace,
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
					object.Namespace,
					binding.DestinationPV,
					active,
				); err != nil {
					return err
				}

				continue
			}
		}

		offline = append(offline, binding)
	}

	if phase == domain.PhaseActivating || phase == domain.PhaseRollingBack {
		return m.switcher.VerifyActivationRecovery(ctx, object.Name, offline)
	}

	return m.switcher.VerifyVolumesOfflineForSession(ctx, object.Name, offline)
}

func (m *PodMigrationExecutor) rollback(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	if err := m.ValidateRollback(ctx, object); err != nil {
		return err
	}

	plan := object.Status.Plan

	save := func(ctx context.Context) error { return m.store.Save(ctx, object) }
	if err := m.restoreSharedMounts(
		ctx,
		object.Name,
		[]string{object.Namespace},
		&object.Status.OpenEBSLVMSharedMounts,
		save,
	); err != nil {
		return err
	}

	if workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseRolledBack {
		if object.Status.Phase == domain.PhaseRolledBack {
			return nil
		}

		return m.transition(
			ctx,
			object,
			domain.PhaseRolledBack,
			"pod migration rollback revalidated",
		)
	}

	restored := m.rollbackVolumesRestored(object)

	pause := !restored &&
		podRollbackNeedsPause(object.Status.Phase, object.Status.ResumeFrom, object.Status.History)
	if pause {
		checkpoint, err := m.rollbackWorkloadCheckpoint(
			ctx,
			object.Name,
			object.Namespace,
			podWorkload(plan.Workload, object.Status.Workload),
			plan.Volumes,
		)
		if err != nil {
			return err
		}

		if err := m.saveWorkloadCheckpoint(ctx, object, checkpoint); err != nil {
			return err
		}
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseRollingBack,
		"rolling back to source volumes",
	); err != nil {
		return err
	}

	if pause {
		if err := kube.LeaseFenceError(ctx); err != nil {
			return err
		}

		checkpoint, pauseErr := m.workloads.Pause(
			ctx,
			object.Name,
			object.Namespace,
			podWorkload(plan.Workload, object.Status.Workload),
			object.Status.Phase,
			object.Status.ResumeFrom,
		)
		if err := m.saveWorkloadCheckpoint(ctx, object, checkpoint); err != nil {
			return errors.Join(pauseErr, err)
		}

		if pauseErr != nil {
			return m.fail(ctx, object, pauseErr)
		}
	}

	if !restored {
		if err := m.workloads.VerifyPaused(
			ctx,
			object.Name,
			object.Namespace,
			podWorkload(plan.Workload, object.Status.Workload),
			object.Status.Phase,
			object.Status.ResumeFrom,
		); err != nil {
			return m.fail(ctx, object, err)
		}
	}

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range slices.Backward(plan.Volumes) {
		status := &object.Status.Volumes[indexes[volume.SourcePVC.Name]]

		binding := namespacedMigrationBindings(
			object.Namespace,
			volume,
			status.VolumeReservationStatus,
		)
		if status.Activation.RolledBackAt != nil {
			continue
		}

		desired := kube.BoundPVCManifest(
			object.Name,
			binding.SourcePVC,
			binding.SourcePV.Name,
			volume.SourcePVCSpec,
			volume.SourcePVCMetadata,
		)

		desired.Annotations[kube.RollbackPVAnnotation] = binding.DestinationPV.Name

		checkpoint := qualifiedActivationCheckpoint(status.Activation, object.Namespace)
		if err := rollbackMigrationVolume(
			ctx,
			m.switcher,
			object.Name,
			object.Namespace,
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

	if err := m.verifyRestoredVolumes(ctx, object); err != nil {
		return m.fail(ctx, object, err)
	}

	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	checkpoint, resumeErr := m.workloads.Resume(
		ctx,
		object.Name,
		object.Namespace,
		podWorkload(plan.Workload, object.Status.Workload),
		plan.SourceNode,
		object.Status.Phase,
		object.Status.ResumeFrom,
		object.Status.WorkflowStatus.DeepCopy().History,
	)
	if err := m.saveWorkloadCheckpoint(ctx, object, checkpoint); err != nil {
		return errors.Join(resumeErr, err)
	}

	if resumeErr != nil {
		return m.fail(ctx, object, resumeErr)
	}

	if plan.Workload.Adapter == v1alpha1.WorkloadStandalone {
		workload := podWorkload(plan.Workload, object.Status.Workload)
		if err := verifyResumedStandalonePod(
			ctx,
			m.client,
			object.Name,
			qualifiedResourceReference(*workload.Pod, object.Namespace),
			plan.SourceNode,
		); err != nil {
			return m.fail(ctx, object, err)
		}
	}

	return m.transition(
		ctx,
		object,
		domain.PhaseRolledBack,
		"source volumes restored and workload resumed",
	)
}

func (m *PodMigrationExecutor) verifyRestoredVolumes(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	plan := object.Status.Plan

	indexes := podMigrationVolumeIndexes(object.Status.Volumes)
	for _, volume := range plan.Volumes {
		status := object.Status.Volumes[indexes[volume.SourcePVC.Name]]
		if err := verifyRollbackStorageVolume(
			ctx,
			m.client,
			object.Name,
			qualifiedResourceReference(
				volume.SourcePVC,
				object.Namespace,
			),
			qualifiedResourceReference(volume.SourcePV, ""),
			qualifiedOptionalReference(status.Activation.ActivePVC, object.Namespace),
		); err != nil {
			return err
		}
	}

	return nil
}

func (m *PodMigrationExecutor) rollbackVolumesRestored(object *v1alpha1.PodMigration) bool {
	if len(object.Status.Volumes) == 0 {
		return false
	}

	for _, volume := range object.Status.Volumes {
		if volume.Activation.RolledBackAt == nil {
			return false
		}
	}

	return true
}
