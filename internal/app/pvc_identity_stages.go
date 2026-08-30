package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) Rename(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.rename(lockedCtx, session) },
	)
}

// Move executes the cross-namespace PVC identity workflow. The storage
// switcher is shared with rename, while the public service entry point keeps
// the operation boundary explicit.
func (s *Service) Move(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.rename(lockedCtx, session) },
	)
}

func (s *Service) rename(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Operation().RebindsPVC() || len(session.Spec.Volumes) != 1 {
		return domain.NewError(
			domain.ErrorValidation,
			"rebind PVC",
			"session is not a single-volume PVC identity operation",
		)
	}

	if session.Status.Phase == domain.PhaseCompleted {
		return nil
	}

	phase := domain.PhaseRenaming

	message := "renaming PVC while retaining its PV"
	if session.Spec.Operation() == domain.OperationMove {
		phase = domain.PhaseMoving
		message = "moving PVC while retaining its PV"
	}

	valid := session.Status.Phase == domain.PhasePlanned || session.Status.Phase == phase ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == phase)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC",
			fmt.Sprintf("session phase %s cannot rebind PVC", session.Status.Phase),
		)
	}

	if err := s.validateRebindOfflineVolumes(ctx, session); err != nil {
		return err
	}

	if err := s.begin(ctx, session, phase, message); err != nil {
		return err
	}

	s.logInfo(
		"PVC identity change started",
		"session",
		session.ID,
		"operation",
		session.Spec.Operation(),
		"source",
		session.Spec.Volumes[0].SourcePVC.Namespace+"/"+session.Spec.Volumes[0].SourcePVC.Name,
		"destination",
		session.Spec.Volumes[0].DestinationPVC.Namespace+"/"+session.Spec.Volumes[0].DestinationPVC.Name,
	)
	volume := &session.Spec.Volumes[0]
	status := &session.Status.Volumes[0]

	pvc, err := s.switcher.RenamePVC(
		ctx,
		session,
		volume,
		func() error { return s.store.Update(ctx, session) },
	)
	if err != nil {
		return s.failContext(ctx, session, err)
	}

	now := metav1.NewTime(s.now().UTC())
	status.Activation.ActivePVC = domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
	status.Activation.ActivatedAt = &now

	if session.Spec.Operation() == domain.OperationMove {
		return s.finish(ctx, session, domain.PhaseCompleted, "PVC move completed")
	}

	return s.finish(ctx, session, domain.PhaseCompleted, "PVC rename completed")
}

func (s *Service) resumePVCIdentity(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
	operation domain.Operation,
) error {
	expected := domain.PhaseRenaming
	if operation == domain.OperationMove {
		expected = domain.PhaseMoving
	}

	switch phase {
	case domain.PhasePlanned, expected:
		return s.rename(ctx, session)
	case domain.PhaseRollingBack:
		return s.rollback(ctx, session)
	case domain.PhaseAborting:
		return s.abort(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	default:
		return invalidWorkflowResumePhase(phase, operation)
	}
}
