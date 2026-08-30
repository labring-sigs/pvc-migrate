package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) validatePodMigrationResume(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved:
		if session.Status.WarmPassesCompleted < session.Spec.PrecopyPasses() {
			return s.ValidateWarmCopy(ctx, session)
		}
		return s.ValidateReservation(ctx, session)
	case domain.PhaseWarmCopying:
		return s.ValidateWarmCopy(ctx, session)
	case domain.PhaseWarmCopied:
		if session.Status.WarmPassesCompleted < session.Spec.PrecopyPasses() {
			return s.ValidateWarmCopy(ctx, session)
		}
		return s.ValidateReservation(ctx, session)
	case domain.PhasePausing:
		return s.ValidateReservation(ctx, session)
	case domain.PhasePaused, domain.PhaseFinalSyncing:
		return s.ValidatePodFinalSync(ctx, session)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		return s.ValidatePodActivation(ctx, session)
	case domain.PhaseActivated:
		if err := s.controllers.VerifyPaused(ctx, session); err != nil {
			return err
		}
		return s.validateWorkloadResume(ctx, session)
	case domain.PhaseResuming:
		return s.validateWorkloadResume(ctx, session)
	case domain.PhaseRollingBack:
		return s.validateRollback(ctx, session)
	case domain.PhaseAborting:
		return s.validateAbort(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume migrate-pod dry-run",
			fmt.Sprintf("phase %s cannot be resumed for migrate-pod", phase),
		)
	}
}

// ValidatePodFinalSync performs the read-only checks required immediately
// before a final synchronization and leaves the workload unchanged.
func (s *Service) ValidatePodFinalSync(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "final sync dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	if !isPodMigrationSession(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync dry-run",
			"final sync requires an orchestrated migration session",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhaseReserved, domain.PhaseWarmCopied, domain.PhasePausing:
		return s.ValidateReservation(ctx, session)
	case domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync dry-run",
			fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase),
		)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return err
	}

	return s.validateOfflineVolumes(ctx, session)
}

// ValidatePodActivation performs activation preconditions through read-only
// API calls. The mutating PV/PVC switch remains behind the execution path.
func (s *Service) ValidatePodActivation(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "activation dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if !isPodMigrationSession(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activation dry-run",
			"activation requires an orchestrated migration session",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseActivated || phase == domain.PhaseResuming ||
		phase == domain.PhaseCompleted ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming) {
		return nil
	}

	valid := phase == domain.PhaseFinalSynced || phase == domain.PhaseActivating ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activation dry-run",
			fmt.Sprintf("session phase %s cannot activate", session.Status.Phase),
		)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Sync.FinalCompletedAt == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"activation dry-run",
				fmt.Sprintf("PVC %s has no completed final sync", volume.SourcePVC.Name),
			)
		}
	}

	if err := s.validateActivationPVCPolicies(ctx, session); err != nil {
		return err
	}

	if phase == domain.PhaseActivating || phase == domain.PhaseFailed {
		return s.validateActivationStorage(ctx, session)
	}

	return s.validateOfflineVolumes(ctx, session)
}
