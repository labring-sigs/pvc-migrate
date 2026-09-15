package domain

import (
	"fmt"
	"slices"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func ValidateIdentityLifecycle(status v1alpha1.WorkflowStatus, rebindPhase Phase) error {
	allowed := []Phase{
		PhasePlanned,
		rebindPhase,
		PhaseCompleted,
		PhaseRollingBack,
		PhaseRolledBack,
		PhaseAborting,
		PhaseAborted,
		PhaseFailed,
	}
	if !slices.Contains(allowed, status.Phase) ||
		(status.ResumeFrom != "" && !slices.Contains(allowed, status.ResumeFrom)) {
		return NewError(
			ErrorValidation,
			"session",
			"lifecycle phase does not belong to this PVC identity operation",
		)
	}

	return nil
}

// TransitionIdentity updates only the shared lifecycle of a PVC rebind.
func TransitionIdentity(
	status *v1alpha1.WorkflowStatus,
	planned bool,
	rebindPhase, next v1alpha1.WorkflowPhase,
	message string,
	now time.Time,
) error {
	current := status.Phase
	if current == next {
		return nil
	}

	if !planned && next == PhaseAborted &&
		(current == "" || current == PhasePlanned || current == PhaseFailed) {
		RecordWorkflowTransition(status, next, message, now)
		return nil
	}

	policy := map[Phase][]Phase{
		PhasePlanned:     {rebindPhase, PhaseAborting, PhaseFailed},
		rebindPhase:      {PhaseCompleted, PhaseFailed},
		PhaseCompleted:   {PhaseRollingBack},
		PhaseRollingBack: {PhaseRolledBack, PhaseFailed},
		PhaseAborting:    {PhaseAborted, PhaseFailed},
		PhaseFailed:      {rebindPhase, PhaseRollingBack, PhaseAborting},
	}
	if !slices.Contains(policy[current], next) {
		return NewError(
			ErrorConflict,
			"transition",
			fmt.Sprintf("phase %s cannot transition to %s", current, next),
		)
	}

	RecordWorkflowTransition(status, next, message, now)

	return nil
}
