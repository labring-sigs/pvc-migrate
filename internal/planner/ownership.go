package planner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

// presentation selects how guidance renders: CLI planning quotes
// copy-paste commands and flags; controller-planned messages are recorded
// verbatim on workflow CRs and events, where CLI text does not belong.
func (p *Planner) presentation() domain.Presentation {
	if p.controllerSubmission {
		return domain.PresentationController
	}

	return domain.PresentationCLI
}

// checkSessionOwnership stops a new session before approval or resource
// creation when the source identity is already associated with a persisted or
// orphaned session. PVC annotations and labels are checked together with the
// source PV label because a cutover can leave ownership on either side while
// Kubernetes controllers converge.
func (p *Planner) checkSessionOwnership(
	ctx context.Context,
	plan checkRecorder,
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
				domain.CheckNameSessionOwnership,
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

	if p.workflowOwners == nil {
		plan.AddCheck(failed(
			domain.CheckNameSessionOwnership,
			"workflow ownership cannot be verified because the owner lookup capability is unavailable",
		))

		return
	}

	ownerSession, err := p.workflowOwners.Find(
		ctx,
		owner,
		sessionNamespace,
		pvc.Namespace,
		p.sessionRecordNamespace,
	)
	if ownerSession != nil && ownerSession.Backend == kube.SessionBackendConfigMap &&
		ownerSession.Resource.Resource == "" {
		// The record's kind is not served by this build (a removed API): no
		// lifecycle command can drive it, so it is as orphaned as a lost record.
		ownerSession = nil
	}

	if ownerSession != nil {
		plan.AddCheck(
			failed(
				domain.CheckNameSessionOwnership,
				fmt.Sprintf(
					"PVC %s/%s or PV %s belongs to session %s (phase %s); %s",
					pvc.Namespace,
					pvc.Name,
					pv.Name,
					owner,
					ownerSession.Phase,
					persistedOwnerGuidance(ownerSession, p.presentation()),
				),
			),
		)

		return
	}

	if err == nil {
		if p.presentation() == domain.PresentationController {
			// Check messages from controller planning land verbatim in
			// workflow CR status, which must not carry CLI command text.
			plan.AddCheck(
				failed(
					domain.CheckNameSessionOwnership,
					fmt.Sprintf(
						"PVC %s/%s or PV %s has orphan ownership from absent session %s; clear the ownership labels to retry",
						pvc.Namespace,
						pvc.Name,
						pv.Name,
						owner,
					),
				),
			)

			return
		}

		recordNamespace := p.sessionRecordNamespace
		if recordNamespace == "" {
			recordNamespace = sessionNamespace
		}

		base := sessionCLIBase(recordNamespace, false)
		executeBase := sessionCLIBase(recordNamespace, true)
		args := fmt.Sprintf(
			"recovery cleanup-orphan %s -n %s --source-pvc %s",
			owner,
			pvc.Namespace,
			pvc.Name,
		)
		plan.AddCheck(
			failed(
				domain.CheckNameSessionOwnership,
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
			domain.CheckNameSessionOwnership,
			fmt.Sprintf(
				"PVC %s/%s or PV %s refers to session %s, but its workflow records cannot be read: %v",
				pvc.Namespace,
				pvc.Name,
				pv.Name,
				owner,
				err,
			),
		),
	)
}

func retainedCleanupArgs(session *kube.WorkflowOwner, workflow string) string {
	args := fmt.Sprintf("%s cleanup %s", workflow, session.ID)
	// Keep is spelled out so the retained copies survive the cleanup even if
	// the recorded policy asked for deletion. Only the transfer workflows
	// (copy, reserve, migrate, migrate-pod) accept a cleanup policy; rename,
	// move, backup, and restore finalize without one and reject the flag.
	if workflowAcceptsCleanupPolicy(session) {
		args += " --unused-storage-policy Keep"
	}

	return args + " --finalize --delete-session"
}

func workflowAcceptsCleanupPolicy(session *kube.WorkflowOwner) bool {
	if session == nil {
		return false
	}

	switch session.Resource.Type {
	case domain.SessionTypeMigrate,
		domain.SessionTypeMigratePod,
		domain.SessionTypeReserve,
		domain.SessionTypeCopy:
		return true
	default:
		return false
	}
}

// persistedOwnerGuidance explains how to release the storage. The CLI
// presentation gets copy-paste commands; the controller presentation gets
// a description of the releasing action, because its check messages land
// verbatim in workflow CR status.
func persistedOwnerGuidance(session *kube.WorkflowOwner, presentation domain.Presentation) string {
	// Controller workflows are owned by the elected controller: the CLI
	// lifecycle commands only manage ConfigMap-backed sessions. Deleting the
	// CR converges storage through the controller's finalizer; kubectl is the
	// universal operator surface, so this form serves both presentations.
	if session.Backend == kube.SessionBackendCRD {
		resource := session.Resource

		scope := ""
		if !resource.Cluster {
			scope = " -n " + session.SessionNamespace
		}

		return fmt.Sprintf(
			"inspect with `kubectl get %s %s -o yaml`; the controller owns this workflow — delete it with `kubectl%s delete %s %s` and the finalizer converges storage per its spec reclaim policies",
			resource.Resource,
			session.ID,
			scope,
			resource.Resource,
			session.ID,
		)
	}

	if presentation == domain.PresentationController {
		return controllerOwnerGuidance(session)
	}

	base := sessionCLIBase(session.SessionNamespace, false)

	executeBase := sessionCLIBase(session.SessionNamespace, true)

	workflow := workflowCommand(session)

	status := fmt.Sprintf("inspect with `%s %s status %s`", base, workflow, session.ID)
	switch session.Phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		args := retainedCleanupArgs(session, workflow)

		return fmt.Sprintf(
			"%s; validate cleanup with `%s %s`, then execute `%s %s --dry-run=false`",
			status,
			base,
			args,
			executeBase,
			args,
		)
	case domain.PhaseWarmCopied:
		if session.Resource.Type == domain.SessionTypeCopy {
			args := retainedCleanupArgs(session, workflow)

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
		if session.Resource.Type == domain.SessionTypeReserve {
			args := retainedCleanupArgs(session, workflow)
			// A cluster-scoped reservation graduates through the
			// cluster-copy family; the namespaced copy command cannot see
			// its record.
			copyFamily := "copy"
			if session.Resource.Cluster {
				copyFamily = "cluster-copy"
			}

			return fmt.Sprintf(
				"%s; validate copy with `%s %s --session %s`, then execute `%s %s --session %s --dry-run=false`; close the reservation by validating `%s %s`, then executing `%s %s --dry-run=false`",
				status,
				base,
				copyFamily,
				session.ID,
				base,
				copyFamily,
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

// controllerOwnerGuidance describes the storage-releasing action without
// command text, for check messages recorded on workflow CRs.
func controllerOwnerGuidance(session *kube.WorkflowOwner) string {
	switch session.Phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return "finalize the finished session and delete its record to release this storage"
	case domain.PhaseWarmCopied:
		if session.Resource.Type == domain.SessionTypeCopy {
			return "keep or discard the copied PVC by finalizing the session to release this storage"
		}
	case domain.PhaseReserved:
		if session.Resource.Type == domain.SessionTypeReserve {
			return "continue the reservation as a copy or close it to release this storage"
		}
	case domain.PhaseFailed:
		if failedSessionCanAbort(session) {
			return "abort the failed session and finalize its cleanup to release this storage"
		}

		return "roll back the failed session and finalize its cleanup to release this storage"
	}

	return "resume or abort the owning session to release this storage"
}

func workflowCommand(session *kube.WorkflowOwner) string {
	if session == nil {
		return "migrate"
	}

	switch session.Resource.Type {
	case domain.SessionTypeMigrate:
		if session.Resource.Cluster {
			return "cluster-migrate"
		}

		return "migrate"
	case domain.SessionTypeMigratePod:
		return "migrate-pod"
	case domain.SessionTypeReserve:
		if session.Resource.Cluster {
			return "cluster-reserve"
		}

		return "reserve"
	case domain.SessionTypeCopy:
		if session.Resource.Cluster {
			return "cluster-copy"
		}

		return "copy"
	case domain.SessionTypeBackup:
		return "backup"
	case domain.SessionTypeRestore:
		return "restore"
	case domain.SessionTypeRename:
		return "rename"
	case domain.SessionTypeMove:
		return "move"
	default:
		return "migrate"
	}
}

func failedSessionCanAbort(session *kube.WorkflowOwner) bool {
	switch session.ResumeFrom {
	case domain.PhaseActivating,
		domain.PhaseRenaming,
		domain.PhaseMoving,
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
