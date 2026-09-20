package app

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (m *MigrationExecutor) FinalizeDeleted(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	// Statuses written by older releases can carry Failed without a resume
	// checkpoint. Deletion is the final convergence pass and must not be
	// wedged by that era's validation gap.
	if object.DeletionTimestamp != nil &&
		object.Status.Phase == domain.PhaseFailed &&
		object.Status.ResumeFrom == "" {
		object.Status.ResumeFrom = domain.PhasePlanned
	}

	if err := validateMigrationObject(object); err != nil {
		return err
	}

	if object.DeletionTimestamp == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize migration",
			"workflow deletion is required",
		)
	}

	ctx = context.WithValue(ctx, workflowDeletionContextKey{}, true)

	return withStoredWorkflowLock(
		ctx,
		m.store,
		m.locker,
		object.Namespace,
		object,
		func(ctx context.Context) error {
			previous := object.Status.WorkflowStatus.DeepCopy()
			domain.SetWorkflowCondition(
				&object.Status.WorkflowStatus,
				v1alpha1.WorkflowCondition{
					Type:               "Deleting",
					Status:             metav1.ConditionTrue,
					Reason:             "CleaningUp",
					Message:            "Releasing migration storage ownership before deletion",
					LastTransitionTime: metav1.NewTime(m.now().UTC()),
				},
			)

			if err := persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return m.store.Save(ctx, object) },
			); err != nil {
				object.Status.WorkflowStatus = *previous
				return err
			}

			phase := workflowResumePhase(object.Status.WorkflowStatus)
			if phase != domain.PhaseCompleted && phase != domain.PhaseRolledBack &&
				phase != domain.PhaseAborted {
				var err error
				if deletionRequiresConvergence(phase) {
					err = m.run(ctx, object)
				} else {
					err = m.abort(ctx, object)
				}

				if err != nil {
					return err
				}
			}

			return m.cleanup(
				ctx,
				object,
				MigrationCleanupOptions{Finalize: true, DeleteSession: true},
			)
		},
	)
}

// Run continues the concrete migration from its last durable stage. Workload
// discovery, precopy and pause/resume belong exclusively to PodMigration.
func (m *MigrationExecutor) Run(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error { return m.run(ctx, object) })
}

// Validate performs the owning stage's preflight without advancing state.
func (m *MigrationExecutor) Validate(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return nil
	}

	switch workflowResumePhase(object.Status.WorkflowStatus) {
	case domain.PhasePlanned, domain.PhaseReserving:
		return m.ValidateReservation(ctx, object)
	case domain.PhaseReserved, domain.PhaseFinalSyncing:
		return m.ValidateFinalSync(ctx, object)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		return m.ValidateActivation(ctx, object)
	case domain.PhaseActivated, domain.PhaseResuming, domain.PhaseCompleted:
		return m.verifyActiveVolumes(ctx, object)
	case domain.PhaseRollingBack, domain.PhaseRolledBack:
		return m.ValidateRollback(ctx, object)
	case domain.PhaseAborting:
		return m.ValidateAbort(ctx, object)
	case domain.PhaseAborted:
		return nil
	default:
		return domain.NewError(
			domain.ErrorValidation,
			"migration",
			"migration phase has no execution preflight",
		)
	}
}

func (m *MigrationExecutor) RequestResume(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error {
			if err := m.Validate(ctx, object); err != nil {
				return err
			}

			if object.Status.Phase != domain.PhaseFailed {
				return nil
			}

			previous := object.Status.WorkflowStatus.DeepCopy()
			if err := domain.ReactivateWorkflow(
				&object.Status.WorkflowStatus,
				"migration resume requested",
				m.now(),
			); err != nil {
				return err
			}

			if err := persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return m.store.Save(ctx, object) },
			); err != nil {
				object.Status.WorkflowStatus = *previous
				return err
			}

			return nil
		})
}

func (m *MigrationExecutor) run(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	switch workflowResumePhase(object.Status.WorkflowStatus) {
	case domain.PhasePlanned, domain.PhaseReserving:
		if err := m.reserve(ctx, object); err != nil {
			return err
		}
		fallthrough
	case domain.PhaseReserved, domain.PhaseFinalSyncing:
		if err := m.finalSync(ctx, object); err != nil {
			return err
		}
		fallthrough
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		if err := m.activate(ctx, object); err != nil {
			return err
		}
		fallthrough
	case domain.PhaseActivated, domain.PhaseResuming:
		if err := m.verifyActiveVolumes(ctx, object); err != nil {
			return m.fail(ctx, object, err)
		}
		return m.transition(ctx, object, domain.PhaseCompleted, "offline migration completed")
	case domain.PhaseCompleted:
		if err := m.verifyActiveVolumes(ctx, object); err != nil {
			return err
		}

		if object.Status.Phase != domain.PhaseCompleted {
			return m.transition(
				ctx,
				object,
				domain.PhaseCompleted,
				"offline migration completion revalidated",
			)
		}

		return nil
	case domain.PhaseRollingBack:
		return m.rollback(ctx, object)
	case domain.PhaseRolledBack:
		if err := m.verifyRestoredVolumes(ctx, object); err != nil {
			return err
		}

		if object.Status.Phase != domain.PhaseRolledBack {
			return m.transition(
				ctx,
				object,
				domain.PhaseRolledBack,
				"offline migration rollback revalidated",
			)
		}

		return nil
	case domain.PhaseAborting:
		return m.abort(ctx, object)
	case domain.PhaseAborted:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"migration",
			"migration requires execution planning",
		)
	}
}

func (m *MigrationExecutor) Abort(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error { return m.abort(ctx, object) })
}

func (m *MigrationExecutor) ValidateAbort(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	switch workflowResumePhase(object.Status.WorkflowStatus) {
	case "",
		domain.PhasePlanned,
		domain.PhaseReserving,
		domain.PhaseReserved,
		domain.PhaseFinalSyncing,
		domain.PhaseFinalSynced,
		domain.PhaseAborting:
	case domain.PhaseAborted:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort migration",
			"migration cutover requires rollback",
		)
	}

	if object.Status.Plan == nil {
		return nil
	}

	plan := object.Status.Plan
	if workflowDeletionInProgress(ctx) {
		deleted, err := deletedPlannedSourcePVC(
			ctx,
			m.client,
			object.Namespace,
			plan.Volumes,
		)
		if err != nil {
			return err
		}

		// Deleted source storage cannot be re-verified; deletion converges
		// through cleanup, which releases the retained volumes instead.
		if deleted {
			return nil
		}
	}

	for _, volume := range plan.Volumes {
		if err := verifySourceStorage(
			ctx,
			m.client,
			qualifiedResourceReference(
				volume.SourcePVC,
				object.Namespace,
			),
			qualifiedResourceReference(volume.SourcePV, ""),
		); err != nil {
			return err
		}
	}

	return nil
}

func (m *MigrationExecutor) abort(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := m.ValidateAbort(ctx, object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if object.Status.Plan == nil {
		return m.transition(
			ctx,
			object,
			domain.PhaseAborted,
			"migration aborted before execution planning",
		)
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseAborting,
		"stopping offline migration tools",
	); err != nil {
		return err
	}

	if err := m.cleanupInterruptedFinalSync(ctx, object); err != nil {
		return m.fail(ctx, object, err)
	}

	return m.transition(
		ctx,
		object,
		domain.PhaseAborted,
		"migration aborted; reserved volumes are retained for cleanup",
	)
}

func (m *MigrationExecutor) Rollback(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, object.Namespace, object,
		func(ctx context.Context) error { return m.rollback(ctx, object) })
}

func (m *MigrationExecutor) ValidateRollback(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := validateMigrationObject(object); err != nil {
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

	plan := object.Status.Plan
	indexes := migrationVolumeIndexes(object.Status.Volumes)

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

func (m *MigrationExecutor) rollback(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	if err := m.ValidateRollback(ctx, object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseRolledBack {
		return nil
	}

	if object.Status.Phase == domain.PhaseCompleted ||
		object.Status.Phase == domain.PhaseActivated {
		if plan := object.Status.Plan; plan != nil {
			for _, volume := range plan.Volumes {
				if err := rollbackDestinationConsumed(
					ctx, m.client,
					object.Namespace,
					volume.SourcePVC.Name,
					object.Name,
				); err != nil {
					return err
				}
			}
		}
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseRollingBack,
		"rolling back offline migration volumes",
	); err != nil {
		return err
	}

	plan := object.Status.Plan

	indexes := migrationVolumeIndexes(object.Status.Volumes)
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
				if err := m.store.Save(ctx, object); err != nil {
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

	return m.transition(
		ctx,
		object,
		domain.PhaseRolledBack,
		"source volumes restored; workload remains caller-managed",
	)
}

func (m *MigrationExecutor) verifyRestoredVolumes(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	plan := object.Status.Plan

	indexes := migrationVolumeIndexes(object.Status.Volumes)
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
