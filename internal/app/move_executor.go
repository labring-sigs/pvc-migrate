package app

import (
	"context"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// MoveExecutor owns cross-namespace identity changes using the Move CRD alone.
type MoveExecutor struct {
	client           kubernetes.Interface
	switcher         *kube.Switcher
	store            kube.WorkflowStore[*v1alpha1.Move]
	locker           kube.SessionLocker
	storageNamespace string
	now              func() time.Time
}

func NewMoveExecutor(
	client kubernetes.Interface,
	store kube.WorkflowStore[*v1alpha1.Move],
	locker kube.SessionLocker,
	storageNamespace string,
) *MoveExecutor {
	return &MoveExecutor{
		client:           client,
		switcher:         kube.NewSwitcher(client),
		store:            store,
		locker:           locker,
		storageNamespace: storageNamespace,
		now:              time.Now,
	}
}

func (m *MoveExecutor) Run(ctx context.Context, object *v1alpha1.Move) error {
	return m.locked(ctx, object, func(ctx context.Context) error {
		err := validateMoveObject(object, m.storageNamespace)
		if err == nil {
			err = m.run(ctx, object)
		}

		if err != nil && object.Status.Phase != domain.PhaseFailed {
			return m.fail(ctx, object, err)
		}

		return err
	})
}

func (m *MoveExecutor) run(ctx context.Context, object *v1alpha1.Move) error {
	switch phase := workflowResumePhase(object.Status.WorkflowStatus); phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	case domain.PhaseRollingBack:
		return m.rollback(ctx, object)
	case domain.PhaseAborting:
		return m.abort(ctx, object)
	case domain.PhasePlanned, domain.PhaseMoving:
	default:
		return invalidWorkflowResumePhase(phase, domain.OperationMove)
	}

	if err := m.ValidateResume(ctx, object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"move",
			"workflow requires execution planning",
		)
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseMoving,
		"moving PVC while retaining its PV",
	); err != nil {
		return err
	}

	plan := object.Status.Plan

	pvc, err := m.rebind(ctx, object.Name,
		qualifiedResourceReference(plan.Identity.SourcePVC, string(plan.SourceNamespace)),
		qualifiedResourceReference(plan.Identity.DestinationPVC, string(plan.DestinationNamespace)),
		qualifiedResourceReference(plan.Identity.SourcePV, ""), plan.Identity.SourceTemplate,
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
	)
	if err != nil {
		return m.fail(ctx, object, err)
	}

	before := object.Status.DeepCopy()
	active := kube.PVCReference(pvc)
	object.Status.Activation.ActivePVC = &active
	now := metav1.NewTime(m.now().UTC())

	object.Status.Activation.ActivatedAt = &now
	if err := m.transition(ctx, object, domain.PhaseCompleted, "PVC move completed"); err != nil {
		object.Status = *before
		return err
	}

	return nil
}

func (m *MoveExecutor) ValidateResume(ctx context.Context, object *v1alpha1.Move) error {
	if err := validateMoveObject(object, m.storageNamespace); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if object.Status.Plan == nil {
		if phase != "" && phase != domain.PhasePlanned {
			return invalidWorkflowResumePhase(phase, domain.OperationMove)
		}
		return nil
	}

	plan := object.Status.Plan
	switch phase {
	case domain.PhasePlanned, domain.PhaseMoving:
		if phase == domain.PhasePlanned {
			if err := m.switcher.VerifyPVCSourceSnapshot(
				ctx,
				qualifiedResourceReference(plan.Identity.SourcePVC, string(plan.SourceNamespace)),
				qualifiedResourceReference(
					plan.Identity.SourcePV,
					"",
				),
				plan.Identity.SourceTemplate,
			); err != nil {
				return err
			}
		}

		return m.switcher.VerifyPVCRebind(
			ctx,
			object.Name,
			qualifiedResourceReference(plan.Identity.SourcePVC, string(plan.SourceNamespace)),
			qualifiedResourceReference(
				plan.Identity.DestinationPVC,
				string(plan.DestinationNamespace),
			),
			qualifiedResourceReference(plan.Identity.SourcePV, ""),
			phase == domain.PhaseMoving,
		)
	case domain.PhaseRollingBack:
		return m.ValidateRollback(ctx, object)
	case domain.PhaseAborting:
		return m.ValidateAbort(object)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	default:
		return invalidWorkflowResumePhase(phase, domain.OperationMove)
	}
}

func (m *MoveExecutor) RequestResume(ctx context.Context, object *v1alpha1.Move) error {
	return m.locked(ctx, object, func(ctx context.Context) error {
		if err := m.ValidateResume(ctx, object); err != nil {
			return err
		}

		if object.Status.Phase != domain.PhaseFailed {
			return nil
		}

		before := object.Status.WorkflowStatus.DeepCopy()
		if err := domain.ReactivateWorkflow(
			&object.Status.WorkflowStatus,
			"move resume requested",
			m.now(),
		); err != nil {
			return err
		}

		if err := persistCheckpoint(
			ctx,
			func(ctx context.Context) error { return m.store.Save(ctx, object) },
		); err != nil {
			object.Status.WorkflowStatus = *before
			return err
		}

		return nil
	})
}

func (m *MoveExecutor) ValidateAbort(object *v1alpha1.Move) error {
	if err := validateMoveObject(object, m.storageNamespace); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return nil
	}

	return validateIdentityAbort(object.Status.WorkflowStatus)
}

func (m *MoveExecutor) Abort(ctx context.Context, object *v1alpha1.Move) error {
	return m.locked(ctx, object, func(ctx context.Context) error { return m.abort(ctx, object) })
}

func (m *MoveExecutor) abort(ctx context.Context, object *v1alpha1.Move) error {
	if err := m.ValidateAbort(object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if object.Status.Plan != nil {
		if err := m.transition(ctx, object, domain.PhaseAborting, "aborting move"); err != nil {
			return err
		}
	}

	return m.transition(
		ctx,
		object,
		domain.PhaseAborted,
		"move aborted; storage resources retained",
	)
}

func (m *MoveExecutor) ValidateRollback(ctx context.Context, object *v1alpha1.Move) error {
	if err := validateMoveObject(object, m.storageNamespace); err != nil {
		return err
	}

	if err := validateIdentityRollback(object.Status.WorkflowStatus); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseRolledBack {
		return nil
	}

	plan := object.Status.Plan
	if plan == nil || object.Status.Activation.ActivePVC == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback move",
			"completed PVC identity is required",
		)
	}

	to := qualifiedResourceReference(plan.Identity.SourcePVC, string(plan.SourceNamespace))
	to.UID, to.ResourceVersion = "", ""

	return m.switcher.VerifyPVCRebind(
		ctx,
		object.Name,
		*object.Status.Activation.ActivePVC,
		to,
		qualifiedResourceReference(plan.Identity.SourcePV, ""),
		workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseRollingBack,
	)
}

func (m *MoveExecutor) Rollback(ctx context.Context, object *v1alpha1.Move) error {
	return m.locked(ctx, object, func(ctx context.Context) error { return m.rollback(ctx, object) })
}

func (m *MoveExecutor) rollback(ctx context.Context, object *v1alpha1.Move) error {
	if err := m.ValidateRollback(ctx, object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseRolledBack {
		return nil
	}

	if err := m.transition(
		ctx,
		object,
		domain.PhaseRollingBack,
		"restoring PVC identity",
	); err != nil {
		return err
	}

	plan := object.Status.Plan
	to := qualifiedResourceReference(plan.Identity.SourcePVC, string(plan.SourceNamespace))
	to.UID, to.ResourceVersion = "", ""

	pvc, err := m.rebind(ctx, object.Name, *object.Status.Activation.ActivePVC,
		to, qualifiedResourceReference(plan.Identity.SourcePV, ""), plan.Identity.SourceTemplate,
		func(ctx context.Context) error { return m.store.Save(ctx, object) })
	if err != nil {
		return m.fail(ctx, object, err)
	}

	before := object.Status.DeepCopy()
	active := kube.PVCReference(pvc)
	object.Status.Activation.ActivePVC = &active
	now := metav1.NewTime(m.now().UTC())

	object.Status.Activation.RolledBackAt = &now
	if err := m.transition(
		ctx,
		object,
		domain.PhaseRolledBack,
		"PVC namespace and name restored",
	); err != nil {
		object.Status = *before
		return err
	}

	return nil
}

func (m *MoveExecutor) ValidateCleanup(
	ctx context.Context,
	object *v1alpha1.Move,
	options IdentityCleanupOptions,
) error {
	if err := validateMoveObject(object, m.storageNamespace); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return nil
	}

	if err := validateIdentityCleanup(object.Status.Phase, options); err != nil {
		return err
	}

	if !options.Finalize {
		return nil
	}

	plan := object.Status.Plan

	return validateRetainedIdentity(
		ctx,
		m.client,
		object.Name,
		moveActivePVC(plan, object.Status.Activation.ActivePVC),
		qualifiedResourceReference(plan.Identity.SourcePV, ""),
		plan.Identity.SourceTemplate.ReclaimPolicy,
	)
}

func (m *MoveExecutor) Cleanup(
	ctx context.Context,
	object *v1alpha1.Move,
	options IdentityCleanupOptions,
) error {
	return m.locked(
		ctx,
		object,
		func(ctx context.Context) error { return m.cleanup(ctx, object, options) },
	)
}

func (m *MoveExecutor) cleanup(
	ctx context.Context,
	object *v1alpha1.Move,
	options IdentityCleanupOptions,
) error {
	if err := m.ValidateCleanup(ctx, object, options); err != nil {
		return err
	}

	if plan := object.Status.Plan; plan != nil && options.Finalize {
		if err := kube.FinalizePVC(
			ctx,
			m.client,
			moveActivePVC(plan, object.Status.Activation.ActivePVC),
			object.Name,
			plan.Identity.SourceTemplate.Metadata,
		); err != nil {
			return err
		}

		if err := finalizeActivePV(
			ctx,
			m.client,
			object.Name,
			qualifiedResourceReference(plan.Identity.SourcePV, ""),
			plan.Identity.SourceTemplate.ReclaimPolicy,
		); err != nil &&
			!apierrors.IsNotFound(err) {
			return err
		}
	}

	if options.DeleteSession {
		if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
			if err := held.lock.Delete(ctx); err != nil {
				return err
			}
		}

		return m.store.Delete(ctx, object)
	}

	return nil
}

func (m *MoveExecutor) FinalizeDeleted(ctx context.Context, object *v1alpha1.Move) error {
	// Statuses written by older releases can carry Failed without a resume
	// checkpoint. Deletion is the final convergence pass and must not be
	// wedged by that era's validation gap.
	if object.DeletionTimestamp != nil &&
		object.Status.Phase == domain.PhaseFailed &&
		object.Status.ResumeFrom == "" {
		object.Status.ResumeFrom = domain.PhasePlanned
	}

	if object.DeletionTimestamp != nil &&
		object.Status.Phase == domain.PhaseFailed &&
		object.Status.ResumeFrom == "" {
		// Statuses written by older releases can carry Failed without a
		// resume checkpoint. Deletion is the final convergence pass and must
		// not be wedged by that era's validation gap.
		object.Status.ResumeFrom = domain.PhasePlanned
	}

	if object == nil || object.DeletionTimestamp == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize move",
			"workflow deletion is required",
		)
	}

	ctx = context.WithValue(ctx, workflowDeletionContextKey{}, true)

	return m.locked(ctx, object, func(ctx context.Context) error {
		if err := validateMoveStatus(&object.Status, m.storageNamespace); err != nil {
			return err
		}

		before := object.Status.WorkflowStatus.DeepCopy()
		domain.SetWorkflowCondition(&object.Status.WorkflowStatus, v1alpha1.WorkflowCondition{
			Type:               "Deleting",
			Status:             metav1.ConditionTrue,
			Reason:             "ConvergingStorage",
			Message:            "Recovering PVC identity and releasing workflow resources before deletion",
			LastTransitionTime: metav1.NewTime(m.now().UTC()),
		})

		if err := persistCheckpoint(
			ctx,
			func(ctx context.Context) error { return m.store.Save(ctx, object) },
		); err != nil {
			object.Status.WorkflowStatus = *before
			return err
		}

		switch workflowResumePhase(object.Status.WorkflowStatus) {
		case domain.PhaseMoving, domain.PhaseRollingBack:
			if err := m.run(ctx, object); err != nil {
				return err
			}
		case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		default:
			if err := m.abort(ctx, object); err != nil {
				return err
			}
		}

		return m.cleanup(ctx, object, IdentityCleanupOptions{Finalize: true, DeleteSession: true})
	})
}

func (m *MoveExecutor) locked(
	ctx context.Context,
	object *v1alpha1.Move,
	run func(context.Context) error,
) error {
	if object == nil || object.Name == "" || object.Namespace != "" || m.storageNamespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"move",
			"cluster-scoped workflow name and storage namespace are required",
		)
	}

	return withStoredWorkflowLock(ctx, m.store, m.locker, m.storageNamespace, object, run)
}

func (m *MoveExecutor) transition(
	ctx context.Context,
	object *v1alpha1.Move,
	phase v1alpha1.WorkflowPhase,
	message string,
) error {
	before := object.Status.WorkflowStatus.DeepCopy()
	if object.Status.StartedAt.IsZero() {
		object.Status.StartedAt = metav1.NewTime(m.now().UTC())
	}

	if err := domain.TransitionIdentity(&object.Status.WorkflowStatus, object.Status.Plan != nil,
		domain.PhaseMoving, phase, message, m.now()); err != nil {
		object.Status.WorkflowStatus = *before
		return err
	}

	if err := persistCheckpoint(
		ctx,
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
	); err != nil {
		object.Status.WorkflowStatus = *before
		return err
	}

	return nil
}

func (m *MoveExecutor) fail(ctx context.Context, object *v1alpha1.Move, cause error) error {
	return recordWorkflowFailure(ctx, &object.Status.WorkflowStatus, cause, "", m.now(),
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
		func(ctx context.Context, phase v1alpha1.WorkflowPhase, message string) error {
			return m.transition(ctx, object, phase, message)
		})
}

func (m *MoveExecutor) rebind(ctx context.Context, id string, from, to, pv v1alpha1.ObjectReference,
	template v1alpha1.PVCSourceTemplate, save func(context.Context) error,
) (*corev1.PersistentVolumeClaim, error) {
	return m.switcher.RenamePVC(ctx, id, from, pv,
		kube.BoundPVCManifest(id, to, pv.Name, template.Spec, template.Metadata),
		func() error { return persistCheckpoint(ctx, save) })
}

func moveActivePVC(
	plan *v1alpha1.MovePlan,
	active *v1alpha1.ObjectReference,
) v1alpha1.ObjectReference {
	if active != nil {
		return *active
	}
	return qualifiedResourceReference(plan.Identity.SourcePVC, string(plan.SourceNamespace))
}
