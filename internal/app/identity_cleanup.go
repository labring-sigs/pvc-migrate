package app

import (
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

type IdentityCleanupOptions struct {
	Finalize      bool
	DeleteSession bool
}

func validateIdentityCleanup(phase v1alpha1.WorkflowPhase, options IdentityCleanupOptions) error {
	if phase != domain.PhaseCompleted && phase != domain.PhaseAborted &&
		phase != domain.PhaseRolledBack {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			fmt.Sprintf("session phase %s is still active", phase),
		)
	}

	if options.DeleteSession && !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			"deleting the session requires --finalize",
		)
	}

	return nil
}
