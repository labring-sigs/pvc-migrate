package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) ValidateReservation(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "reservation dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"reservation preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
	)

	for index := range session.Spec.Volumes {
		volume := session.Spec.Volumes[index]

		status := session.Status.Volumes[index]
		if err := s.validateReservedVolume(ctx, session, &volume, &status); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateReservedVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
) error {
	checkVolume := *volume
	checkStatus := *status
	return s.reserver.ReserveVolume(ctx, session, &checkVolume, &checkStatus, true)
}

func (s *Service) validateReserveResume(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	switch phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	case domain.PhaseRollingBack:
		return s.validateRollback(ctx, session)
	case domain.PhaseAborting:
		return s.validateAbort(ctx, session)
	}

	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving:
		return s.ValidateReservation(ctx, session)
	case domain.PhaseReserved:
		return nil
	default:
		return invalidWorkflowResumePhase(phase, domain.OperationReserve)
	}
}
