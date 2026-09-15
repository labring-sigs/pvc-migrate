package domain_test

import (
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestIdentityLifecyclesOwnTheirRebindPhase(t *testing.T) {
	for _, phase := range []v1alpha1.WorkflowPhase{domain.PhaseRenaming, domain.PhaseMoving} {
		t.Run(string(phase), func(t *testing.T) {
			status := v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned}
			if err := domain.TransitionIdentity(
				&status,
				true,
				phase,
				domain.PhaseWarmCopying,
				"unrelated",
				time.Now(),
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("accepted copy phase: %v", err)
			}

			for _, next := range []v1alpha1.WorkflowPhase{phase, domain.PhaseCompleted} {
				if err := domain.TransitionIdentity(
					&status,
					true,
					phase,
					next,
					"advance",
					time.Now(),
				); err != nil {
					t.Fatal(err)
				}
			}

			status.Phase, status.ResumeFrom = domain.PhaseFailed, domain.PhasePausing
			if err := domain.ValidateIdentityLifecycle(
				status,
				phase,
			); domain.CategoryOf(
				err,
			) != domain.ErrorValidation {
				t.Fatalf("accepted unrelated resume phase: %v", err)
			}
		})
	}
}
