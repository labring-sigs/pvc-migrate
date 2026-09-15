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

// RenameExecutor owns Rename orchestration. Its only durable state is the
// Rename CRD; the persistence backend is supplied by the entry point.
type RenameExecutor struct {
	client           kubernetes.Interface
	switcher         *kube.Switcher
	store            kube.WorkflowStore[*v1alpha1.Rename]
	locker           kube.SessionLocker
	storageNamespace string
	now              func() time.Time
}

func NewRenameExecutor(
	client kubernetes.Interface,
	store kube.WorkflowStore[*v1alpha1.Rename],
	locker kube.SessionLocker,
	storageNamespace string,
) *RenameExecutor {
	return &RenameExecutor{
		client:           client,
		switcher:         kube.NewSwitcher(client),
		store:            store,
		locker:           locker,
		storageNamespace: storageNamespace,
		now:              time.Now,
	}
}

func (r *RenameExecutor) Run(ctx context.Context, object *v1alpha1.Rename) error {
	return r.locked(ctx, object, func(ctx context.Context) error {
		err := validateRenameObject(object)
		if err == nil {
			err = r.run(ctx, object)
		}

		if err != nil && object.Status.Phase != domain.PhaseFailed {
			return r.fail(ctx, object, err)
		}

		return err
	})
}

// RequestResume persists an explicit retry for a controller to pick up. An
// unplanned failure resumes planning; a planned failure keeps its exact plan.
func (r *RenameExecutor) RequestResume(ctx context.Context, object *v1alpha1.Rename) error {
	return r.locked(ctx, object, func(ctx context.Context) error {
		if err := r.ValidateResume(ctx, object); err != nil {
			return err
		}

		if object.Status.Phase != domain.PhaseFailed {
			return nil
		}

		previous := object.Status.WorkflowStatus.DeepCopy()
		if err := domain.ReactivateWorkflow(
			&object.Status.WorkflowStatus,
			"rename resume requested",
			r.now(),
		); err != nil {
			return err
		}

		if err := persistCheckpoint(
			ctx,
			func(ctx context.Context) error { return r.store.Save(ctx, object) },
		); err != nil {
			object.Status.WorkflowStatus = *previous
			return err
		}

		return nil
	})
}

func (r *RenameExecutor) run(ctx context.Context, object *v1alpha1.Rename) error {
	phase := workflowResumePhase(object.Status.WorkflowStatus)
	switch phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	case domain.PhaseRollingBack:
		return r.rollback(ctx, object)
	case domain.PhaseAborting:
		return r.abort(ctx, object)
	case domain.PhasePlanned, domain.PhaseRenaming:
	default:
		return invalidWorkflowResumePhase(phase, domain.OperationRename)
	}

	if err := r.ValidateResume(ctx, object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rename",
			"workflow requires execution planning",
		)
	}

	if err := r.transition(
		ctx,
		object,
		domain.PhaseRenaming,
		"renaming PVC while retaining its PV",
	); err != nil {
		return err
	}

	plan := object.Status.Plan
	from, pv := qualifiedResourceReference(
		plan.SourcePVC,
		object.Namespace,
	), qualifiedResourceReference(
		plan.SourcePV,
		"",
	)
	to := qualifiedResourceReference(plan.DestinationPVC, object.Namespace)

	pvc, err := r.switcher.RenamePVC(
		ctx,
		object.Name,
		from,
		pv,
		kube.BoundPVCManifest(
			object.Name,
			to,
			pv.Name,
			plan.SourceTemplate.Spec,
			plan.SourceTemplate.Metadata,
		),
		func() error {
			return persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return r.store.Save(ctx, object) },
			)
		},
	)
	if err != nil {
		return r.fail(ctx, object, err)
	}

	previous := object.Status.DeepCopy()
	activation := &object.Status.Activation
	activation.ActivePVC = localResourceReference(kube.PVCReference(pvc))
	now := metav1.NewTime(r.now().UTC())
	activation.ActivatedAt = &now

	if err := r.transition(ctx, object, domain.PhaseCompleted, "PVC rename completed"); err != nil {
		object.Status = *previous
		return err
	}

	return nil
}

func (r *RenameExecutor) ValidateResume(ctx context.Context, object *v1alpha1.Rename) error {
	if err := validateRenameObject(object); err != nil {
		return err
	}

	phase := workflowResumePhase(object.Status.WorkflowStatus)
	if object.Status.Plan == nil {
		if phase != "" && phase != domain.PhasePlanned {
			return invalidWorkflowResumePhase(phase, domain.OperationRename)
		}
		return nil
	}

	switch phase {
	case domain.PhasePlanned, domain.PhaseRenaming:
		plan := object.Status.Plan
		if phase == domain.PhasePlanned {
			if err := r.switcher.VerifyPVCSourceSnapshot(ctx,
				qualifiedResourceReference(plan.SourcePVC, object.Namespace),
				qualifiedResourceReference(plan.SourcePV, ""), plan.SourceTemplate,
			); err != nil {
				return err
			}
		}

		return r.switcher.VerifyPVCRebind(
			ctx,
			object.Name,
			qualifiedResourceReference(plan.SourcePVC, object.Namespace),
			qualifiedResourceReference(plan.DestinationPVC, object.Namespace),
			qualifiedResourceReference(plan.SourcePV, ""),
			phase == domain.PhaseRenaming,
		)
	case domain.PhaseRollingBack:
		return r.ValidateRollback(ctx, object)
	case domain.PhaseAborting:
		return r.ValidateAbort(object)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	default:
		return invalidWorkflowResumePhase(phase, domain.OperationRename)
	}
}

func (r *RenameExecutor) ValidateAbort(object *v1alpha1.Rename) error {
	if err := validateRenameObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return nil
	}

	return validateIdentityAbort(object.Status.WorkflowStatus)
}

func (r *RenameExecutor) Abort(ctx context.Context, object *v1alpha1.Rename) error {
	return r.locked(ctx, object, func(ctx context.Context) error { return r.abort(ctx, object) })
}

func (r *RenameExecutor) abort(ctx context.Context, object *v1alpha1.Rename) error {
	if err := r.ValidateAbort(object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if object.Status.Plan == nil {
		return r.transition(
			ctx,
			object,
			domain.PhaseAborted,
			"workflow aborted before execution planning",
		)
	}

	if err := r.transition(ctx, object, domain.PhaseAborting, "aborting rename"); err != nil {
		return err
	}

	return r.transition(
		ctx,
		object,
		domain.PhaseAborted,
		"rename aborted; storage resources are retained for cleanup",
	)
}

func (r *RenameExecutor) ValidateRollback(ctx context.Context, object *v1alpha1.Rename) error {
	if err := validateRenameObject(object); err != nil {
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
			"rollback rename",
			"completed PVC identity is required",
		)
	}

	from := qualifiedResourceReference(*object.Status.Activation.ActivePVC, object.Namespace)
	to := qualifiedResourceReference(plan.SourcePVC, object.Namespace)
	to.UID, to.ResourceVersion = "", ""

	return r.switcher.VerifyPVCRebind(
		ctx,
		object.Name,
		from,
		to,
		qualifiedResourceReference(plan.SourcePV, ""),
		workflowResumePhase(object.Status.WorkflowStatus) == domain.PhaseRollingBack,
	)
}

func (r *RenameExecutor) Rollback(ctx context.Context, object *v1alpha1.Rename) error {
	return r.locked(ctx, object, func(ctx context.Context) error { return r.rollback(ctx, object) })
}

func (r *RenameExecutor) rollback(ctx context.Context, object *v1alpha1.Rename) error {
	if err := r.ValidateRollback(ctx, object); err != nil {
		return err
	}

	if object.Status.Phase == domain.PhaseRolledBack {
		return nil
	}

	if err := r.transition(
		ctx,
		object,
		domain.PhaseRollingBack,
		"restoring PVC identity",
	); err != nil {
		return err
	}

	plan := object.Status.Plan
	from := qualifiedResourceReference(*object.Status.Activation.ActivePVC, object.Namespace)
	to := qualifiedResourceReference(plan.SourcePVC, object.Namespace)
	to.UID, to.ResourceVersion = "", ""
	pv := qualifiedResourceReference(plan.SourcePV, "")

	pvc, err := r.switcher.RenamePVC(
		ctx,
		object.Name,
		from,
		pv,
		kube.BoundPVCManifest(
			object.Name,
			to,
			pv.Name,
			plan.SourceTemplate.Spec,
			plan.SourceTemplate.Metadata,
		),
		func() error {
			return persistCheckpoint(
				ctx,
				func(ctx context.Context) error { return r.store.Save(ctx, object) },
			)
		},
	)
	if err != nil {
		return r.fail(ctx, object, err)
	}

	previous := object.Status.DeepCopy()
	activation := &object.Status.Activation
	activation.ActivePVC = localResourceReference(kube.PVCReference(pvc))
	now := metav1.NewTime(r.now().UTC())
	activation.RolledBackAt = &now

	if err := r.transition(ctx, object, domain.PhaseRolledBack, "PVC name restored"); err != nil {
		object.Status = *previous
		return err
	}

	return nil
}

func (r *RenameExecutor) ValidateCleanup(
	ctx context.Context,
	object *v1alpha1.Rename,
	options IdentityCleanupOptions,
) error {
	if err := validateRenameObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return nil
	}

	if err := validateIdentityCleanup(object.Status.Phase, options); err != nil {
		return err
	}

	if object.Status.Plan == nil || !options.Finalize {
		return nil
	}

	plan := object.Status.Plan

	return validateRetainedIdentity(
		ctx,
		r.client,
		object.Name,
		renameActivePVC(
			object.Namespace,
			plan.SourcePVC,
			object.Status.Activation.ActivePVC,
		),
		qualifiedResourceReference(plan.SourcePV, ""),
		plan.SourceTemplate.ReclaimPolicy,
	)
}

func (r *RenameExecutor) Cleanup(
	ctx context.Context,
	object *v1alpha1.Rename,
	options IdentityCleanupOptions,
) error {
	return r.locked(ctx, object, func(ctx context.Context) error {
		return r.cleanup(ctx, object, options)
	})
}

func (r *RenameExecutor) cleanup(
	ctx context.Context,
	object *v1alpha1.Rename,
	options IdentityCleanupOptions,
) error {
	if err := r.ValidateCleanup(ctx, object, options); err != nil {
		return err
	}

	if object.Status.Plan != nil && options.Finalize {
		plan := object.Status.Plan
		if err := kube.FinalizePVC(
			ctx,
			r.client,
			renameActivePVC(
				object.Namespace,
				plan.SourcePVC,
				object.Status.Activation.ActivePVC,
			),
			object.Name,
			plan.SourceTemplate.Metadata,
		); err != nil {
			return err
		}

		if err := finalizeActivePV(
			ctx,
			r.client,
			object.Name,
			qualifiedResourceReference(plan.SourcePV, ""),
			plan.SourceTemplate.ReclaimPolicy,
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

		return r.store.Delete(ctx, object)
	}

	return nil
}

// FinalizeDeleted converges the persisted PVC plan under one Lease before
// releasing protection. A later spec edit cannot redirect deletion recovery.
func (r *RenameExecutor) FinalizeDeleted(ctx context.Context, object *v1alpha1.Rename) error {
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
			"finalize rename",
			"workflow deletion is required",
		)
	}

	ctx = context.WithValue(ctx, workflowDeletionContextKey{}, true)

	return r.locked(ctx, object, func(ctx context.Context) error {
		if err := validateRenameStatus(&object.Status); err != nil {
			return err
		}

		before := object.Status.WorkflowStatus.DeepCopy()
		domain.SetWorkflowCondition(&object.Status.WorkflowStatus, v1alpha1.WorkflowCondition{
			Type:               "Deleting",
			Status:             metav1.ConditionTrue,
			Reason:             "ConvergingStorage",
			Message:            "Recovering PVC identity and releasing workflow resources before deletion",
			LastTransitionTime: metav1.NewTime(r.now().UTC()),
		})

		if err := persistCheckpoint(
			ctx,
			func(ctx context.Context) error { return r.store.Save(ctx, object) },
		); err != nil {
			object.Status.WorkflowStatus = *before
			return err
		}

		switch workflowResumePhase(object.Status.WorkflowStatus) {
		case domain.PhaseRenaming, domain.PhaseRollingBack:
			if err := r.run(ctx, object); err != nil {
				return err
			}
		case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		default:
			if err := r.abort(ctx, object); err != nil {
				return err
			}
		}

		return r.cleanup(ctx, object, IdentityCleanupOptions{Finalize: true, DeleteSession: true})
	})
}

func renameActivePVC(
	namespace string,
	source v1alpha1.LocalResourceReference,
	active *v1alpha1.LocalResourceReference,
) v1alpha1.ObjectReference {
	if active != nil {
		return qualifiedResourceReference(*active, namespace)
	}

	return qualifiedResourceReference(source, namespace)
}

func validateRetainedIdentity(
	ctx context.Context,
	client kubernetes.Interface,
	id string,
	claim, volume v1alpha1.ObjectReference,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	pvc, err := client.CoreV1().
		PersistentVolumeClaims(claim.Namespace).
		Get(ctx, claim.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if err == nil && (pvc.UID != claim.UID ||
		(pvc.Labels[kube.SessionKey] != "" && pvc.Labels[kube.SessionKey] != id) ||
		(pvc.Annotations[kube.SessionKey] != "" && pvc.Annotations[kube.SessionKey] != id)) {
		return domain.NewError(
			domain.ErrorConflict,
			"finalize PVC",
			"PVC identity or ownership changed",
		)
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, volume.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if err := validateFinalizablePV(pv, volume, id, policy); err != nil {
		return err
	}

	if pv.Spec.ClaimRef != nil && pv.Status.Phase == corev1.VolumeBound &&
		(pv.Spec.ClaimRef.UID != claim.UID || pv.Spec.ClaimRef.Name != claim.Name || pv.Spec.ClaimRef.Namespace != claim.Namespace) {
		return domain.NewError(domain.ErrorConflict, "finalize PV", "PV binding changed")
	}

	return nil
}

func (r *RenameExecutor) transition(
	ctx context.Context,
	object *v1alpha1.Rename,
	phase v1alpha1.WorkflowPhase,
	message string,
) error {
	previous := object.Status.WorkflowStatus.DeepCopy()
	if object.Status.StartedAt.IsZero() {
		object.Status.StartedAt = metav1.NewTime(r.now().UTC())
	}

	if err := domain.TransitionIdentity(
		&object.Status.WorkflowStatus,
		object.Status.Plan != nil,
		domain.PhaseRenaming,
		phase,
		message,
		r.now(),
	); err != nil {
		object.Status.WorkflowStatus = *previous
		return err
	}

	if err := persistCheckpoint(
		ctx,
		func(ctx context.Context) error { return r.store.Save(ctx, object) },
	); err != nil {
		object.Status.WorkflowStatus = *previous
		return err
	}

	return nil
}

func (r *RenameExecutor) fail(ctx context.Context, object *v1alpha1.Rename, cause error) error {
	return recordWorkflowFailure(ctx, &object.Status.WorkflowStatus, cause, "", r.now(),
		func(ctx context.Context) error { return r.store.Save(ctx, object) },
		func(ctx context.Context, phase v1alpha1.WorkflowPhase, message string) error {
			return r.transition(ctx, object, phase, message)
		},
	)
}

func (r *RenameExecutor) locked(
	ctx context.Context,
	object *v1alpha1.Rename,
	run func(context.Context) error,
) error {
	if err := validateRenameIdentity(object); err != nil {
		return err
	}

	return withStoredWorkflowLock(ctx, r.store, r.locker, r.storageNamespace, object, run)
}

func validateRenameIdentity(object *v1alpha1.Rename) error {
	if object == nil || object.Name == "" || object.Namespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"workflow name and namespace are required",
		)
	}

	return nil
}

func validateRenameObject(object *v1alpha1.Rename) error {
	if err := validateRenameIdentity(object); err != nil {
		return err
	}

	if object.DeletionTimestamp != nil {
		return validateRenameStatus(&object.Status)
	}

	if object.Status.ObservedGeneration != 0 &&
		object.Status.ObservedGeneration != object.Generation {
		return domain.NewError(
			domain.ErrorConflict,
			"rename",
			"workflow spec changed after planning",
		)
	}

	if err := validateRenameSpec(object.Spec, object.Status.Plan); err != nil {
		return err
	}

	return validateRenameStatus(&object.Status)
}

func validateRenameSpec(spec v1alpha1.RenameSpec, plan *v1alpha1.RenamePlan) error {
	if spec.SourcePVC.Name == "" || spec.DestinationPVC.Name == "" ||
		spec.SourcePVC.Name == spec.DestinationPVC.Name {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"distinct source and destination PVC names are required",
		)
	}

	if plan == nil {
		return nil
	}

	if plan.SourcePVC.Name != spec.SourcePVC.Name ||
		plan.DestinationPVC.Name != spec.DestinationPVC.Name {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"plan endpoints differ from the requested PVC identity",
		)
	}

	if spec.SourcePVC.UID != "" && spec.SourcePVC.UID != plan.SourcePVC.UID {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"planned source PVC differs from the requested identity",
		)
	}

	if expected := spec.SourcePV; expected != nil &&
		(expected.Name != plan.SourcePV.Name || (expected.UID != "" && expected.UID != plan.SourcePV.UID)) {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"planned source PV differs from the requested identity",
		)
	}

	return nil
}

func validateRenameStatus(status *v1alpha1.RenameStatus) error {
	if status.Phase == "" && status.Plan == nil &&
		status.Activation == (v1alpha1.RenameActivationStatus{}) {
		return nil
	}

	if err := domain.ValidateIdentityLifecycle(
		status.WorkflowStatus,
		domain.PhaseRenaming,
	); err != nil {
		return err
	}

	phase := workflowResumePhase(status.WorkflowStatus)

	plan := status.Plan
	if plan == nil {
		if status.Activation != (v1alpha1.RenameActivationStatus{}) ||
			(phase != domain.PhasePlanned && phase != domain.PhaseAborted) {
			return domain.NewError(
				domain.ErrorValidation,
				"rename",
				"execution requires a persisted plan",
			)
		}

		return nil
	}

	if plan.SourcePVC.Name == "" || plan.DestinationPVC.Name == "" ||
		plan.SourcePVC.Name == plan.DestinationPVC.Name {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"source and destination PVC names must be distinct and nonempty",
		)
	}

	if plan.SourcePVC.UID == "" ||
		plan.SourcePV.Name == "" ||
		plan.SourcePV.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"plan and checkpoint do not match the requested PVC identity",
		)
	}

	if plan.SourceTemplate.ReclaimPolicy != corev1.PersistentVolumeReclaimRetain &&
		plan.SourceTemplate.ReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"source reclaim policy must be recorded",
		)
	}

	return validateRenameActivation(phase, plan, status.Activation.ActivePVC)
}

func validateRenameActivation(
	phase v1alpha1.WorkflowPhase,
	plan *v1alpha1.RenamePlan,
	active *v1alpha1.LocalResourceReference,
) error {
	if active != nil && (active.Name == "" || active.UID == "") {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"active PVC identity is incomplete",
		)
	}

	if active != nil && active.Name != plan.SourcePVC.Name &&
		active.Name != plan.DestinationPVC.Name {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"active PVC is outside the planned rename endpoints",
		)
	}

	if (phase == domain.PhaseCompleted || phase == domain.PhaseRollingBack) &&
		(active == nil || active.Name != plan.DestinationPVC.Name) {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"completed rename requires the destination PVC checkpoint",
		)
	}

	if phase == domain.PhaseRolledBack && (active == nil || active.Name != plan.SourcePVC.Name) {
		return domain.NewError(
			domain.ErrorValidation,
			"rename",
			"completed rollback requires the restored source PVC checkpoint",
		)
	}

	return nil
}
