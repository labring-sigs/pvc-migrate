package planner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// checkSessionOwnership stops a new session before approval or resource
// creation when the source identity is already associated with a persisted or
// orphaned session. PVC annotations and labels are checked together with the
// source PV label because a cutover can leave ownership on either side while
// Kubernetes controllers converge.
func (p *Planner) checkSessionOwnership(
	ctx context.Context,
	plan *domain.MigrationPlan,
	sessionNamespace string,
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
) {
	owners := map[string][]string{}
	if owner := pvc.Annotations[kube.SessionKey]; owner != "" {
		owners[owner] = append(owners[owner], "PVC annotation")
	}

	if owner := pvc.Labels[kube.SessionKey]; owner != "" {
		owners[owner] = append(owners[owner], "PVC label")
	}

	if owner := pv.Labels[kube.SessionKey]; owner != "" {
		owners[owner] = append(owners[owner], "PV label")
	}

	if len(owners) == 0 {
		return
	}

	ids := make([]string, 0, len(owners))
	for id := range owners {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	if len(ids) > 1 {
		parts := make([]string, 0, len(ids))
		for _, id := range ids {
			parts = append(parts, fmt.Sprintf("%s (%s)", id, strings.Join(owners[id], ", ")))
		}

		plan.AddCheck(
			failed(
				"session-ownership",
				fmt.Sprintf(
					"PVC %s/%s and PV %s have conflicting pvc-migrate session owners: %s; inspect each owner before retrying",
					pvc.Namespace,
					pvc.Name,
					pv.Name,
					strings.Join(parts, "; "),
				),
			),
		)

		return
	}

	owner := ids[0]
	store := kube.NewConfigMapSessionStore(p.client)

	ownerSession, err := store.Get(ctx, sessionNamespace, owner)
	if err == nil {
		plan.AddCheck(
			failed(
				"session-ownership",
				fmt.Sprintf(
					"PVC %s/%s or PV %s belongs to session %s (phase %s); %s",
					pvc.Namespace,
					pvc.Name,
					pv.Name,
					owner,
					ownerSession.Status.Phase,
					persistedOwnerGuidance(ownerSession),
				),
			),
		)

		return
	}

	if apierrors.IsNotFound(err) {
		base := sessionCLIBase(sessionNamespace, false)
		executeBase := sessionCLIBase(sessionNamespace, true)
		args := fmt.Sprintf(
			"recovery cleanup-orphan %s --source-namespace %s --source-pvc %s",
			owner,
			pvc.Namespace,
			pvc.Name,
		)
		plan.AddCheck(
			failed(
				"session-ownership",
				fmt.Sprintf(
					"PVC %s/%s or PV %s has orphan ownership from session %s; validate with `%s %s`, then execute `%s %s --dry-run=false`",
					pvc.Namespace,
					pvc.Name,
					pv.Name,
					owner,
					base,
					args,
					executeBase,
					args,
				),
			),
		)

		return
	}

	plan.AddCheck(
		failed(
			"session-ownership",
			fmt.Sprintf(
				"PVC %s/%s or PV %s refers to session %s, but its session ConfigMap cannot be read: %v",
				pvc.Namespace,
				pvc.Name,
				pv.Name,
				owner,
				err,
			),
		),
	)
}

func persistedOwnerGuidance(session *domain.Session) string {
	base := sessionCLIBase(session.Spec.SessionNamespace, false)
	executeBase := sessionCLIBase(session.Spec.SessionNamespace, true)
	workflow := workflowCommand(session)

	status := fmt.Sprintf("inspect with `%s %s status %s`", base, workflow, session.ID)
	switch session.Status.Phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		args := fmt.Sprintf(
			"%s cleanup %s --delete-temporary --delete-rollback-pv --finalize --delete-session",
			workflow,
			session.ID,
		)

		return fmt.Sprintf(
			"%s; validate cleanup with `%s %s`, then execute `%s %s --dry-run=false`",
			status,
			base,
			args,
			executeBase,
			args,
		)
	case domain.PhaseWarmCopied:
		if session.Spec.Operation() == domain.OperationCopy {
			args := fmt.Sprintf("%s cleanup %s --finalize --delete-session", workflow, session.ID)

			return fmt.Sprintf(
				"%s; preserve the copied PVC and validate cleanup with `%s %s`, then execute `%s %s --dry-run=false`",
				status,
				base,
				args,
				executeBase,
				args,
			)
		}
	case domain.PhaseReserved:
		if session.Spec.Operation() == domain.OperationReserve {
			args := fmt.Sprintf(
				"%s cleanup %s --delete-temporary --delete-rollback-pv --finalize --delete-session",
				workflow,
				session.ID,
			)

			return fmt.Sprintf(
				"%s; validate copy with `%s copy --session %s`, then execute `%s copy --session %s --dry-run=false`; close the reservation by validating `%s %s`, then executing `%s %s --dry-run=false`",
				status,
				base,
				session.ID,
				base,
				session.ID,
				base,
				args,
				executeBase,
				args,
			)
		}
	case domain.PhaseFailed:
		if failedSessionCanAbort(session) {
			return fmt.Sprintf(
				"%s; validate abort with `%s %s abort %s`, then execute `%s %s abort %s --dry-run=false` and follow the cleanup guidance",
				status,
				base,
				workflow,
				session.ID,
				executeBase,
				workflow,
				session.ID,
			)
		}

		return fmt.Sprintf(
			"%s; validate rollback with `%s %s rollback %s`, then execute `%s %s rollback %s --dry-run=false`",
			status,
			base,
			workflow,
			session.ID,
			executeBase,
			workflow,
			session.ID,
		)
	}

	return fmt.Sprintf(
		"%s; validate recovery with `%s %s resume %s`, then execute `%s %s resume %s --dry-run=false`",
		status,
		base,
		workflow,
		session.ID,
		executeBase,
		workflow,
		session.ID,
	)
}

func workflowCommand(session *domain.Session) string {
	if session == nil {
		return "migrate"
	}

	switch session.Spec.Type {
	case domain.SessionTypeMigrate:
		return "migrate"
	case domain.SessionTypeMigratePod:
		return "migrate-pod"
	case domain.SessionTypeReserve:
		return "reserve"
	case domain.SessionTypeCopy:
		return "copy"
	case domain.SessionTypeBackup:
		return "backup"
	case domain.SessionTypeRename:
		return "rename"
	case domain.SessionTypeMove:
		return "move"
	default:
		return "migrate"
	}
}

func failedSessionCanAbort(session *domain.Session) bool {
	switch session.Status.ResumeFrom {
	case domain.PhaseActivating,
		domain.PhaseActivated,
		domain.PhaseResuming,
		domain.PhaseCompleted,
		domain.PhaseRollingBack:
		return false
	default:
		return true
	}
}

func sessionCLIBase(namespace string, approve bool) string {
	base := "pvc-migrate"
	if namespace != "" && namespace != "pvc-migrate-system" {
		base += " --session-namespace " + namespace
	}

	if approve {
		base += " --yes"
	}

	return base
}
