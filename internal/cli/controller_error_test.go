package cli

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestControllerFailurePreservesExitCategory(t *testing.T) {
	for category, code := range map[domain.ErrorCategory]int{
		"": 1, domain.ErrorInternal: 1, domain.ErrorValidation: 2, domain.ErrorPrecondition: 3,
		domain.ErrorConflict: 4, domain.ErrorKubernetes: 5, domain.ErrorCopy: 6, domain.ErrorTimeout: 7,
	} {
		session := &domain.Session{
			Status: domain.SessionStatus{
				Phase:         domain.PhaseFailed,
				ErrorCategory: category,
				Message:       "operation failed",
			},
		}
		if err := controllerWaitResultError(session); domain.ExitCode(err) != code {
			t.Fatalf("category=%s exit=%d expected=%d", category, domain.ExitCode(err), code)
		}

		session.Status.Phase = domain.PhaseCompleted
		if err := controllerWaitResultError(session); err != nil {
			t.Fatal(err)
		}
	}
}
