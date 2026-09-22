package app

import (
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func podAbortNeedsResume(
	phase, resumeFrom v1alpha1.WorkflowPhase,
	history []v1alpha1.WorkflowHistoryEntry,
) bool {
	previous := phase
	if previous == domain.PhaseFailed || previous == domain.PhaseAborting {
		previous = resumeFrom
	}

	if previous == domain.PhaseAborting {
		previous = phaseBefore(history, domain.PhaseAborting)
	}

	switch previous {
	case domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
		return true
	}

	return (phase == domain.PhaseAborting || resumeFrom == domain.PhaseAborting) &&
		abortOriginWasPaused(history)
}

func abortOriginWasPaused(history []v1alpha1.WorkflowHistoryEntry) bool {
	for _, v := range slices.Backward(history) {
		switch v.Phase {
		case domain.PhaseFailed, domain.PhaseAborting:
			continue
		case domain.PhasePausing,
			domain.PhasePaused,
			domain.PhaseFinalSyncing,
			domain.PhaseFinalSynced:
			return true
		default:
			return false
		}
	}

	return false
}

func validatePodAbortPhase(phase v1alpha1.WorkflowPhase) error {
	switch phase {
	case "", domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved,
		domain.PhaseWarmCopying, domain.PhaseWarmCopied, domain.PhasePausing, domain.PhasePaused,
		domain.PhaseFinalSyncing, domain.PhaseFinalSynced, domain.PhaseAborting, domain.PhaseAborted:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort pod migration",
			"migration cutover requires rollback",
		)
	}
}
