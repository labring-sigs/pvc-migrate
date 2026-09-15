package app

import (
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func phaseBefore(
	history []v1alpha1.WorkflowHistoryEntry,
	target v1alpha1.WorkflowPhase,
) v1alpha1.WorkflowPhase {
	for index, entry := range slices.Backward(history) {
		if entry.Phase != target {
			continue
		}

		for previous := index - 1; previous >= 0; previous-- {
			phase := history[previous].Phase
			if phase == domain.PhaseFailed || phase == target {
				continue
			}

			return phase
		}

		return ""
	}

	return ""
}
