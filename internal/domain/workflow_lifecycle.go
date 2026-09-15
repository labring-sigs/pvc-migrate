package domain

import (
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RecordWorkflowTransition records an operation-approved phase change.
// Each operation validates its own transition before calling this mechanism.
func RecordWorkflowTransition(
	lifecycle *v1alpha1.WorkflowStatus,
	next v1alpha1.WorkflowPhase,
	message string,
	now time.Time,
) {
	if lifecycle.Phase == next {
		return
	}

	t := metav1.NewTime(now.UTC())

	if next == PhaseFailed ||
		((next == PhaseAborting || next == PhaseRollingBack) && lifecycle.Phase != PhaseFailed) {
		lifecycle.ResumeFrom = lifecycle.Phase
	}

	lifecycle.Phase = next
	if next != PhaseFailed {
		lifecycle.FailureReason = ""
		lifecycle.ErrorCategory = ""
	}

	message = BoundWorkflowMessage(message)
	lifecycle.Message = message
	lifecycle.UpdatedAt = t

	lifecycle.History = append(
		lifecycle.History,
		v1alpha1.WorkflowHistoryEntry{Phase: next, Time: t, Message: message},
	)
	trimWorkflowHistory(lifecycle)

	if next == PhaseCompleted || next == PhaseAborted || next == PhaseRolledBack {
		lifecycle.CompletedAt = &t
	} else {
		lifecycle.CompletedAt = nil
	}
}
