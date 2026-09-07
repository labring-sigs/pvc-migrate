package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Backup attempts use independent transfer IDs. An orphan cannot be attributed
// to this CR from the PVC alone, so retain protection instead of deleting a
// potentially unrelated transfer or discarding its recovery record.
func (s *Service) verifyBackupToolsStopped(ctx context.Context, session *domain.Session) error {
	var claim domain.ObjectReference
	switch {
	case session.Spec.Backup != nil:
		claim = session.Spec.Backup.SourcePVC
	case session.Spec.Restore != nil:
		claim = session.Spec.Restore.DestinationPVC
	default:
		return nil
	}

	claims := map[string]struct{}{claim.Name: {}}

	pods, err := s.client.CoreV1().Pods(claim.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for i := range pods.Items {
		if isPVMigrateToolForClaims(&pods.Items[i], claims) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"finalize workflow",
				fmt.Sprintf(
					"transfer Pod %s/%s still exists; wait for transfer cleanup or inspect its orphan Helm release before removing it",
					claim.Namespace,
					pods.Items[i].Name,
				),
			)
		}
	}

	jobs, err := s.client.BatchV1().Jobs(claim.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for i := range jobs.Items {
		template := jobs.Items[i].Spec.Template
		if isPVMigrateToolForClaims(
			&corev1.Pod{ObjectMeta: template.ObjectMeta, Spec: template.Spec},
			claims,
		) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"finalize workflow",
				fmt.Sprintf(
					"transfer Job %s/%s still exists; wait for transfer cleanup or inspect its orphan Helm release before removing it",
					claim.Namespace,
					jobs.Items[i].Name,
				),
			)
		}
	}

	return nil
}
