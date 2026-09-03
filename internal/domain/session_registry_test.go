//nolint:testpackage // This test must mutate the private policy result.
package domain

import "testing"

func TestAllowedTransitionsAreIsolatedPerCall(t *testing.T) {
	first := allowedTransitions()
	first[PhasePlanned][0] = PhaseCompleted
	first[PhaseFailed] = nil

	second := allowedTransitions()
	if second[PhasePlanned][0] != PhaseReserving {
		t.Fatalf("transition policy was mutated through a prior result: %#v", second[PhasePlanned])
	}

	if len(second[PhaseFailed]) == 0 {
		t.Fatal("transition policy lost the failed-session recovery rules")
	}
}
