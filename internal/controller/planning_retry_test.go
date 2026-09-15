package controller

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestCorrectedPlanningOnlyRetriesFailedDiscovery(t *testing.T) {
	for _, test := range []struct {
		name              string
		phase, resume     v1alpha1.WorkflowPhase
		observed, current int64
		want              bool
	}{
		{"corrected discovery", domain.PhaseFailed, domain.PhasePlanned, 1, 2, true},
		{"unchanged discovery", domain.PhaseFailed, domain.PhasePlanned, 2, 2, false},
		{"stale generation", domain.PhaseFailed, domain.PhasePlanned, 2, 1, false},
		{"execution failure", domain.PhaseFailed, domain.PhaseReserving, 1, 2, false},
		{"unknown failure", domain.PhaseFailed, "", 1, 2, false},
		{"aborted", domain.PhaseAborted, domain.PhasePlanned, 1, 2, false},
		{"completed", domain.PhaseCompleted, domain.PhasePlanned, 1, 2, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := retryCorrectedPlanning(
				test.phase,
				test.resume,
				test.observed,
				test.current,
			); got != test.want {
				t.Fatalf("retry = %v, want %v", got, test.want)
			}
		})
	}
}
