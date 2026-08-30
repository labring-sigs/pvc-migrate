package app

import (
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

// Cleanup policy is kept separate from resource deletion so each workflow
// can review ownership and terminal-phase rules without scanning API calls.
func cleanupPVRefs(
	session *domain.Session,
	volume *domain.VolumeSpec,
) (active, rollback domain.ObjectReference, policy corev1.PersistentVolumeReclaimPolicy) {
	if cleanupKeepsSource(session) {
		return volume.SourcePV, volume.DestinationPV, volume.SourceReclaimPolicy
	}

	if session.Spec.Operation().RebindsPVC() {
		return volume.SourcePV, domain.ObjectReference{}, volume.SourceReclaimPolicy
	}

	if session.Status.Phase == domain.PhaseRolledBack ||
		session.Status.Phase == domain.PhaseAborted {
		return volume.SourcePV, volume.DestinationPV, volume.SourceReclaimPolicy
	}

	return volume.DestinationPV, volume.SourcePV, volume.DestinationPolicy
}

func cleanupRollbackReclaimPolicy(
	session *domain.Session,
	volume *domain.VolumeSpec,
) corev1.PersistentVolumeReclaimPolicy {
	if cleanupKeepsSource(session) || session.Status.Phase == domain.PhaseRolledBack ||
		session.Status.Phase == domain.PhaseAborted {
		return volume.DestinationPolicy
	}

	if session.Spec.Operation().RebindsPVC() {
		return ""
	}

	return volume.SourceReclaimPolicy
}

func cleanupKeepsSource(session *domain.Session) bool {
	if session == nil {
		return false
	}

	switch session.Spec.Operation() {
	case domain.OperationReserve, domain.OperationCopy:
		return true
	default:
		return false
	}
}

func preservesCopyOutput(session *domain.Session, options CleanupOptions) bool {
	return session != nil && session.Spec.Operation() == domain.OperationCopy &&
		session.Status.Phase == domain.PhaseWarmCopied &&
		!options.DeleteTemporary &&
		!options.DeleteRollback
}

func cleanupPhaseAllowed(session *domain.Session) bool {
	if session == nil {
		return false
	}

	phase := session.Status.Phase
	if phase == domain.PhaseAborted {
		return true
	}

	switch session.Spec.Operation() {
	case domain.OperationReserve:
		return phase == domain.PhaseReserved
	case domain.OperationCopy:
		return phase == domain.PhaseWarmCopied
	default:
		return phase == domain.PhaseCompleted || phase == domain.PhaseRolledBack
	}
}

func cleanupRollbackRole(session *domain.Session) string {
	if cleanupKeepsSource(session) || session.Status.Phase == domain.PhaseAborted {
		return kube.ResourceRoleDestination
	}
	return kube.ResourceRoleRollback
}
