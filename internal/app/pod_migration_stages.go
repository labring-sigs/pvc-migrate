package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) pause(ctx context.Context, session *domain.Session) error {
	if !isPodMigrationSession(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause workload",
			"workload pause requires an orchestrated migration session",
		)
	}

	if session.Status.Phase == domain.PhasePaused ||
		session.Status.Phase == domain.PhaseFinalSyncing ||
		session.Status.Phase == domain.PhaseFinalSynced {
		if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
			return err
		}
		return s.controllers.VerifyPaused(ctx, session)
	}

	valid := session.Status.Phase == domain.PhaseReserved ||
		session.Status.Phase == domain.PhaseWarmCopied ||
		session.Status.Phase == domain.PhasePausing ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhasePausing)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause workload",
			fmt.Sprintf("session phase %s cannot pause", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	if err := s.begin(ctx, session, domain.PhasePausing, "pausing workload"); err != nil {
		return err
	}

	if err := s.controllers.Pause(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	// Controller adapters may record resource UIDs and original pause state
	// while applying the pause. Persist that recovery data before verification.
	if err := s.store.Update(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	return s.finish(ctx, session, domain.PhasePaused, "workload is safely paused")
}

func (s *Service) podFinalSync(ctx context.Context, session *domain.Session) error {
	if !isPodMigrationSession(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			"final sync requires an orchestrated migration session",
		)
	}

	valid := session.Status.Phase == domain.PhasePaused ||
		session.Status.Phase == domain.PhaseFinalSyncing ||
		session.Status.Phase == domain.PhaseFinalSynced ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseFinalSyncing)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"final sync preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
	)

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.validateOfflineVolumes(ctx, session); err != nil {
		return err
	}

	targets, err := s.resolveCopyToolProbeTargets(ctx, session, false)
	if err != nil {
		return err
	}

	probeResults, err := s.probeToolImage(ctx, session, targets)
	if err != nil {
		return err
	}

	return s.finalSyncWithProbeResults(ctx, session, probeResults)
}

func (s *Service) podPauseAndFinalSync(ctx context.Context, session *domain.Session) error {
	if !isPodMigrationSession(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			"final sync requires an orchestrated migration session",
		)
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhaseReserved, domain.PhaseWarmCopied, domain.PhasePausing,
		domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"pause and final sync preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
		"alreadyPaused",
		phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
			phase == domain.PhaseFinalSynced,
	)

	alreadyPaused := phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced
	if alreadyPaused {
		if err := s.controllers.VerifyPaused(ctx, session); err != nil {
			return err
		}

		if err := s.verifyShrinkUsage(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}

		if err := s.validateOfflineVolumes(ctx, session); err != nil {
			return err
		}
	} else if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	targets, err := s.resolveCopyToolProbeTargets(ctx, session, false)
	if err != nil {
		return err
	}

	probeResults, err := s.probeToolImage(ctx, session, targets)
	if err != nil {
		return err
	}

	if !alreadyPaused {
		if err := s.pause(ctx, session); err != nil {
			return err
		}

		if err := s.verifyShrinkUsage(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finalSyncWithProbeResults(ctx, session, probeResults)
}

func (s *Service) podActivate(ctx context.Context, session *domain.Session) error {
	if !isPodMigrationSession(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate",
			"activation requires an orchestrated migration session",
		)
	}

	if session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseResuming ||
		session.Status.Phase == domain.PhaseCompleted ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming) {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	valid := session.Status.Phase == domain.PhaseFinalSynced ||
		session.Status.Phase == domain.PhaseActivating ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate",
			fmt.Sprintf("session phase %s cannot activate", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.ValidatePodActivation(ctx, session); err != nil {
		return err
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseActivating,
		"activating destination volumes",
	); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Activation.ActivatedAt != nil {
			if err := s.verifyActiveStorageVolume(ctx, session, index); err != nil {
				return s.failContext(ctx, session, err)
			}
			continue
		}

		s.logInfo(
			"volume activation started",
			"session",
			session.ID,
			"index",
			index,
			"source",
			volume.SourcePVC.Namespace+"/"+volume.SourcePVC.Name,
			"destination",
			volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
			"pv",
			volume.DestinationPV.Name,
		)

		if err := s.switcher.ActivateVolume(ctx, session, volume, status, func() error {
			return s.store.Update(ctx, session)
		}); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finish(ctx, session, domain.PhaseActivated, "all destination volumes are active")
}

func (s *Service) resumeWorkload(ctx context.Context, session *domain.Session) error {
	if !isPodMigrationSession(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume workload",
			"workload resume requires a real-time Pod migration session",
		)
	}

	valid := session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseResuming ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume workload",
			fmt.Sprintf("session phase %s cannot resume", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.validateWorkloadResume(ctx, session); err != nil {
		return err
	}

	if err := s.begin(ctx, session, domain.PhaseResuming, "resuming workload"); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		if err := s.ensureConcurrentDestinationMount(
			ctx,
			session,
			index,
		); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	if err := s.controllers.Resume(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.verifyActiveVolumes(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseCompleted,
		"migration completed and workload is ready",
	)
}

func (s *Service) migratePod(ctx context.Context, session *domain.Session) error {
	if session == nil || session.Spec.Operation() != domain.OperationMigratePod {
		return domain.NewError(
			domain.ErrorPrecondition,
			"migrate-pod",
			"migrate-pod requires a MigratePod session",
		)
	}

	warmPasses := session.Spec.PrecopyPasses()
	if warmPasses < 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"migrate-pod",
			"warm passes must be non-negative",
		)
	}

	if session.Spec.WorkflowOptionsPtr() == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"migrate-pod",
			"session workflow options are missing",
		)
	}

	if session.Status.WarmPassesCompleted < warmPasses {
		if err := s.ValidateWarmCopy(ctx, session); err != nil {
			return err
		}
	}

	if err := s.Reserve(ctx, session); err != nil {
		return err
	}

	if err := s.runRemainingWarmCopies(ctx, session, warmPasses); err != nil {
		return err
	}

	return s.migrateAfterWarmCopy(ctx, session)
}

func (s *Service) migrateAfterWarmCopy(ctx context.Context, session *domain.Session) error {
	if err := s.PodPauseAndFinalSync(ctx, session); err != nil {
		return err
	}

	if err := s.PodActivate(ctx, session); err != nil {
		return err
	}

	return s.ResumePodWorkload(ctx, session)
}

func (s *Service) runRemainingWarmCopies(
	ctx context.Context,
	session *domain.Session,
	warmPasses int,
) error {
	for session.Status.WarmPassesCompleted < warmPasses {
		if err := s.WarmCopy(ctx, session); err != nil {
			return err
		}
	}

	return nil
}

func isPodMigrationSession(session *domain.Session) bool {
	return session != nil && session.Spec.Operation() == domain.OperationMigratePod
}

func (s *Service) resumePodMigration(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	if session.Spec.Operation() != domain.OperationMigratePod {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume migrate-pod",
			"Pod migration resume requires a MigratePod session",
		)
	}

	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving:
		if err := s.Reserve(ctx, session); err != nil {
			return err
		}
		return s.MigratePod(ctx, session)
	case domain.PhaseReserved:
		return s.MigratePod(ctx, session)
	case domain.PhaseWarmCopying:
		if err := s.WarmCopy(ctx, session); err != nil {
			return err
		}

		if err := s.runRemainingWarmCopies(ctx, session, session.Spec.PrecopyPasses()); err != nil {
			return err
		}

		return s.migrateAfterWarmCopy(ctx, session)
	case domain.PhaseWarmCopied, domain.PhasePausing:
		if phase == domain.PhaseWarmCopied {
			if err := s.runRemainingWarmCopies(
				ctx,
				session,
				session.Spec.PrecopyPasses(),
			); err != nil {
				return err
			}
		}

		return s.migrateAfterWarmCopy(ctx, session)
	case domain.PhasePaused, domain.PhaseFinalSyncing:
		if err := s.PodFinalSync(ctx, session); err != nil {
			return err
		}

		if err := s.PodActivate(ctx, session); err != nil {
			return err
		}

		return s.ResumePodWorkload(ctx, session)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		if err := s.PodActivate(ctx, session); err != nil {
			return err
		}
		return s.ResumePodWorkload(ctx, session)
	case domain.PhaseActivated, domain.PhaseResuming:
		return s.ResumePodWorkload(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	case domain.PhaseRollingBack:
		return s.rollback(ctx, session)
	case domain.PhaseAborting:
		return s.abort(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume migrate-pod",
			fmt.Sprintf("phase %s cannot be resumed for migrate-pod", phase),
		)
	}
}
