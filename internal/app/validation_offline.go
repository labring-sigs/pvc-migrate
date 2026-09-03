package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) validateOfflineMigrationResume(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving:
		return s.ValidateReservation(ctx, session)
	case domain.PhaseReserved, domain.PhaseFinalSyncing:
		return s.ValidateOfflineFinalSync(ctx, session)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		return s.ValidateOfflineActivation(ctx, session)
	case domain.PhaseActivated, domain.PhaseResuming:
		return s.verifyActiveStorage(ctx, session)
	case domain.PhaseRollingBack:
		return s.validateRollback(ctx, session)
	case domain.PhaseAborting:
		return s.validateAbort(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume offline migration dry-run",
			fmt.Sprintf("phase %s cannot be resumed for offline migration", phase),
		)
	}
}

func (s *Service) validateOfflineVolumes(ctx context.Context, session *domain.Session) error {
	volumes := make([]*domain.VolumeSpec, len(session.Spec.Volumes))
	for index := range session.Spec.Volumes {
		volumes[index] = &session.Spec.Volumes[index]
	}

	return s.verifyVolumesOffline(ctx, session, volumes)
}

func (s *Service) verifyVolumesOffline(
	ctx context.Context,
	session *domain.Session,
	volumes []*domain.VolumeSpec,
) error {
	return s.switcher.VerifyVolumesOfflineForSession(ctx, session.ID, volumes)
}

// ValidateOfflineFinalSync checks the offline terminal copy without invoking
// workload-controller APIs. Consumers must already be stopped by the caller.
func (s *Service) ValidateOfflineFinalSync(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"offline final sync dry-run",
			"session is nil",
		)
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if session.Spec.Operation() != domain.OperationMigrate {
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline final sync dry-run",
			"offline final sync requires an offline migrate session",
		)
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhaseReserved, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline final sync dry-run",
			fmt.Sprintf("session phase %s cannot final-sync offline", session.Status.Phase),
		)
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return err
	}

	return s.validateOfflineVolumes(ctx, session)
}

// ValidateOfflineActivation checks staged volume identities and policies
// without requiring a paused or discovered workload.
func (s *Service) ValidateOfflineActivation(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"offline activation dry-run",
			"session is nil",
		)
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if session.Spec.Operation() != domain.OperationMigrate {
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline activation dry-run",
			"offline activation requires an offline migrate session",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	if phase == domain.PhaseActivated || phase == domain.PhaseCompleted ||
		phase == domain.PhaseResuming {
		return nil
	}

	if phase != domain.PhaseFinalSynced && phase != domain.PhaseActivating {
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline activation dry-run",
			fmt.Sprintf("session phase %s cannot activate offline", session.Status.Phase),
		)
	}

	for index := range session.Spec.Volumes {
		if session.Status.Volumes[index].Sync.FinalCompletedAt == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"offline activation dry-run",
				fmt.Sprintf(
					"PVC %s has no completed final sync",
					session.Spec.Volumes[index].SourcePVC.Name,
				),
			)
		}
	}

	if err := s.validateActivationPVCPolicies(ctx, session); err != nil {
		return err
	}

	if phase == domain.PhaseActivating || session.Status.Phase == domain.PhaseFailed {
		return s.validateActivationStorage(ctx, session)
	}

	return s.validateOfflineVolumes(ctx, session)
}
