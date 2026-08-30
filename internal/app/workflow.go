package app

import (
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func requireWorkflowSession(
	session *domain.Session,
	expected domain.SessionType,
	action string,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, action, "session is nil")
	}
	if session.Spec.Type != expected {
		return domain.NewError(
			domain.ErrorPrecondition,
			action,
			fmt.Sprintf("requires a %s session, got %s", expected, session.Spec.Type),
		)
	}
	return nil
}

func persistedResumePhase(session *domain.Session) (domain.Phase, error) {
	if err := validateRetryableSessionFailure(session); err != nil {
		return "", err
	}
	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}
	return phase, nil
}
