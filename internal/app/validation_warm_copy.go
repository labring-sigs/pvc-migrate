package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// ValidateWarmCopy performs every read-only check needed before reservation
// or warm-copy mutation. It deliberately leaves source-node inference and
// temporary OpenEBS shared-mount preparation unpersisted.
func (s *Service) ValidateWarmCopy(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "warm copy dry-run", "session is nil")
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}
	if operation := session.Spec.Operation(); operation != domain.OperationCopy &&
		operation != domain.OperationMigratePod {
		return domain.NewError(
			domain.ErrorPrecondition,
			"warm copy dry-run",
			"warm copy requires a copy or real-time Pod migration session",
		)
	}

	valid := session.Status.Phase == domain.PhasePlanned ||
		session.Status.Phase == domain.PhaseReserving ||
		session.Status.Phase == domain.PhaseReserved ||
		session.Status.Phase == domain.PhaseWarmCopied ||
		session.Status.Phase == domain.PhaseWarmCopying ||
		(session.Status.Phase == domain.PhaseFailed && (session.Status.ResumeFrom == domain.PhaseReserving || session.Status.ResumeFrom == domain.PhaseWarmCopying))
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"warm copy dry-run",
			fmt.Sprintf("session phase %s cannot warm-copy", session.Status.Phase),
		)
	}

	if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	if err := s.validateCopyConsumersBatch(ctx, session, false); err != nil {
		return err
	}

	if err := s.validateWarmCopyOpenEBSLVM(ctx, session); err != nil {
		return err
	}

	_, err := s.resolveSourceToolProbeTargets(ctx, session, true)

	return err
}

func (s *Service) validateWarmCopyOpenEBSLVM(ctx context.Context, session *domain.Session) error {
	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return nil
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		isLVM, err := s.openEBSLVMSource(ctx, volume)
		if err != nil {
			return err
		}

		if !isLVM {
			continue
		}

		active, err := s.sourcePVCIsActive(ctx, volume)
		if err != nil {
			return err
		}

		if !active {
			continue
		}

		if state, pending := pendingOpenEBSLVMSharedMount(session, volume); pending {
			previousShared := strings.TrimSpace(state.PreviousShared)
			if state.PreviousSharedSet && previousShared != "" &&
				!strings.EqualFold(previousShared, "no") &&
				!strings.EqualFold(previousShared, "yes") {
				return domain.NewError(
					domain.ErrorPrecondition,
					"OpenEBS LVM shared mount",
					fmt.Sprintf(
						"LVMVolume %s/%s has unsupported recorded spec.shared value %q",
						state.LVMVolume.Namespace,
						state.LVMVolume.Name,
						state.PreviousShared,
					),
				)
			}

			needsChange := !state.PreviousSharedSet || previousShared == "" ||
				strings.EqualFold(previousShared, "no")
			if needsChange && !session.Spec.OpenEBSLVMSharedMountEnabled() {
				return activeUnsharedOpenEBSLVMError(session, volume)
			}

			continue
		}

		prepared, err := manager.PrepareShared(ctx, volume.SourcePV)
		if err != nil {
			return err
		}

		if prepared.NeedsChange && !session.Spec.OpenEBSLVMSharedMountEnabled() {
			return activeUnsharedOpenEBSLVMError(session, volume)
		}
	}

	return nil
}

func (s *Service) validateCopyResume(
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
	case domain.PhasePlanned, domain.PhaseReserving,
		domain.PhaseReserved, domain.PhaseWarmCopying:
		return s.ValidateWarmCopy(ctx, session)
	case domain.PhaseWarmCopied:
		return nil
	default:
		return invalidWorkflowResumePhase(phase, domain.OperationCopy)
	}
}
