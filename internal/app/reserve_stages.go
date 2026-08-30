package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (s *Service) Reserve(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.reserve(lockedCtx, session) },
	)
}

func (s *Service) reserve(ctx context.Context, session *domain.Session) error {
	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if session.Status.Phase == domain.PhaseReserved ||
		phaseAfter(session.Status.Phase, domain.PhaseReserved) {
		return nil
	}

	if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	if _, err := s.probeToolImage(ctx, session, reservationToolProbeTargets(session)); err != nil {
		return err
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseReserving,
		"reserving destination storage",
	); err != nil {
		return err
	}

	if err := kube.EnsureNamespace(
		ctx,
		s.client,
		session.Spec.TemporaryNamespace,
		session.ID,
		false,
	); err != nil {
		return s.failContext(ctx, session, err)
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Reserved && volume.DestinationPV.UID != "" {
			// A retry still revalidates backend settings applied after provisioning.
		} else {
			s.logInfo(
				"destination storage reservation started",
				"session",
				session.ID,
				"pvc",
				volume.SourcePVC.Name,
				"destination",
				volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
				"node",
				session.Spec.WorkflowOptions().TargetNode,
			)

			if err := s.reserver.ReserveVolume(ctx, session, volume, status, false); err != nil {
				return s.failContext(ctx, session, err)
			}

			if err := s.store.Update(ctx, session); err != nil {
				return s.failContext(ctx, session, err)
			}
		}

		if err := s.ensureConcurrentDestinationMount(ctx, session, index); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseReserved,
		"destination storage is provisioned and retained",
	)
}

func (s *Service) resumeReserve(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving:
		return s.reserve(ctx, session)
	case domain.PhaseReserved:
		return nil
	case domain.PhaseRollingBack:
		return s.rollback(ctx, session)
	case domain.PhaseAborting:
		return s.abort(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	default:
		return invalidWorkflowResumePhase(phase, domain.OperationReserve)
	}
}
