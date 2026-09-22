package kube

import (
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	reservationCopyPendingAnnotation = "migrate.sealos.io/reservation-copy-pending"
	reservationCopyOriginAnnotation  = "migrate.sealos.io/reservation-copy-origin"
)

// RequireWorkflowHandoffComplete prevents ordinary execution and cleanup from
// racing a persisted operation handoff after the original process exits.
func RequireWorkflowHandoffComplete(object metav1.Object) error {
	if object.GetAnnotations()[reservationCopyPendingAnnotation] != "" {
		return domain.NewError(
			domain.ErrorConflict,
			"workflow handoff",
			"reservation-to-copy handoff must finish before execution or cleanup",
		)
	}

	return nil
}

// WorkflowHandoffChanged admits metadata-only handoff checkpoints to the
// recovery queue without admitting unrelated annotation updates.
func WorkflowHandoffChanged(previous, current metav1.Object) bool {
	for _, key := range []string{reservationCopyPendingAnnotation, reservationCopyOriginAnnotation} {
		if previous.GetAnnotations()[key] != current.GetAnnotations()[key] {
			return true
		}
	}

	return false
}
