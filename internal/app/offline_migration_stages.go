package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) offlineFinalSync(ctx context.Context, session *domain.Session) error {
	if session == nil || session.Spec.Operation() != domain.OperationMigrate {
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline final sync",
			"offline final sync requires an offline migrate session",
		)
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
			"offline final sync",
			fmt.Sprintf("session phase %s cannot final-sync offline", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"offline final sync preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
	)

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

func (s *Service) offlineActivate(ctx context.Context, session *domain.Session) error {
	if session == nil || session.Spec.Operation() != domain.OperationMigrate {
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline activate",
			"offline activation requires an offline migrate session",
		)
	}

	if session.Status.Phase == domain.PhaseCompleted {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	if session.Status.Phase == domain.PhaseActivated {
		return nil
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	if phase != domain.PhaseFinalSynced && phase != domain.PhaseActivating {
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline activate",
			fmt.Sprintf("session phase %s cannot activate offline", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.ValidateOfflineActivation(ctx, session); err != nil {
		return err
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseActivating,
		"activating offline destination volumes",
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
			"offline volume activation started",
			"session", session.ID,
			"index", index,
			"source", volume.SourcePVC.Namespace+"/"+volume.SourcePVC.Name,
			"destination", volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
			"pv", volume.DestinationPV.Name,
		)

		if err := s.switcher.ActivateVolume(ctx, session, volume, status, func() error {
			return s.store.Update(ctx, session)
		}); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseActivated,
		"all offline destination volumes are active",
	)
}

func (s *Service) completeOfflineMigration(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseCompleted {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	if phase != domain.PhaseActivated && phase != domain.PhaseResuming {
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline migration",
			fmt.Sprintf("session phase %s cannot complete offline migration", session.Status.Phase),
		)
	}

	if err := s.verifyActiveStorage(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if session.Status.Phase == domain.PhaseFailed {
		if err := s.begin(
			ctx,
			session,
			domain.PhaseResuming,
			"verifying offline migration completion",
		); err != nil {
			return err
		}
	}

	return s.finish(ctx, session, domain.PhaseCompleted, "offline migration completed")
}

func (s *Service) offlineMigrate(ctx context.Context, session *domain.Session) error {
	if session == nil || session.Spec.Operation() != domain.OperationMigrate {
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline migrate",
			"offline migrate requires a Migrate session",
		)
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving:
		if err := s.Reserve(ctx, session); err != nil {
			return err
		}
		fallthrough
	case domain.PhaseReserved, domain.PhaseFinalSyncing:
		if err := s.OfflineFinalSync(ctx, session); err != nil {
			return err
		}
		fallthrough
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		if err := s.OfflineActivate(ctx, session); err != nil {
			return err
		}
		fallthrough
	case domain.PhaseActivated, domain.PhaseResuming:
		return s.completeOfflineMigration(ctx, session)
	case domain.PhaseCompleted:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	case domain.PhaseAborted, domain.PhaseRolledBack:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"offline migrate",
			fmt.Sprintf("session phase %s cannot run offline migration", session.Status.Phase),
		)
	}
}

func (s *Service) resumeOfflineMigration(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	if session.Spec.Operation() != domain.OperationMigrate {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume offline migration",
			"offline migration resume requires a Migrate session",
		)
	}
	// offlineMigrate derives the resumable stage from the persisted phase. The
	// phase argument remains explicit here so invalid dispatches get a useful
	// operation-specific error before any mutation.
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved,
		domain.PhaseFinalSyncing, domain.PhaseFinalSynced, domain.PhaseActivating,
		domain.PhaseActivated, domain.PhaseResuming,
		domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return s.offlineMigrate(ctx, session)
	case domain.PhaseRollingBack:
		return s.rollback(ctx, session)
	case domain.PhaseAborting:
		return s.abort(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume offline migration",
			fmt.Sprintf("phase %s cannot be resumed for offline migration", phase),
		)
	}
}
