package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func (s *Service) cleanupInterruptedCopy(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	var mode copyengine.Mode
	switch phase {
	case domain.PhaseWarmCopying:
		mode = copyengine.ModeWarm
	case domain.PhaseFinalSyncing:
		mode = copyengine.ModeFinal
	default:
		return nil
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status, err := session.VolumeStatus(volume.SourcePVC.Name)
		if err != nil {
			return err
		}

		if status.Sync.Attempts == 0 {
			continue
		}

		if mode == copyengine.ModeFinal && status.Sync.FinalCompletedAt != nil {
			continue
		}

		if err := s.validateReservedVolume(ctx, session, volume, status); err != nil {
			return err
		}

		request := copyengine.Request{
			SessionID:      session.ID,
			Source:         volume.SourcePVC,
			Destination:    volume.DestinationPVC,
			Mode:           mode,
			Attempt:        status.Sync.Attempts,
			Strategies:     session.Spec.WorkflowOptions().Strategies,
			KubeconfigPath: s.config.KubeconfigPath,
			Context:        s.config.Context,
		}
		s.logInfo(
			"recovering interrupted copy tools",
			"session",
			session.ID,
			"pvc",
			volume.SourcePVC.Name,
			"operation",
			copyengine.OperationID(request),
		)

		if err := s.copier.Cleanup(ctx, request); err != nil {
			return err
		}

		if err := s.deleteCopyToolPods(ctx, volume, copyengine.OperationID(request)); err != nil {
			return err
		}

		if err := s.waitForCopyToolRelease(ctx, volume); err != nil {
			return err
		}
	}

	return nil
}
