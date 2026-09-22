package app

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func workflowResumePhase(status v1alpha1.WorkflowStatus) v1alpha1.WorkflowPhase {
	if status.Phase == domain.PhaseFailed {
		return status.ResumeFrom
	}
	return status.Phase
}

func validateIdentityAbort(status v1alpha1.WorkflowStatus) error {
	switch workflowResumePhase(status) {
	case domain.PhasePlanned, domain.PhaseAborting, domain.PhaseAborted:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort",
			"PVC identity change must finish through resume before rollback or cleanup",
		)
	}
}

func validateIdentityRollback(status v1alpha1.WorkflowStatus) error {
	switch workflowResumePhase(status) {
	case domain.PhaseCompleted, domain.PhaseRollingBack, domain.PhaseRolledBack:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback",
			"PVC identity must finish rebinding before rollback",
		)
	}
}
