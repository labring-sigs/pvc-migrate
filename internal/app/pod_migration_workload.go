package app

import (
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func podPausePhase(phase v1alpha1.WorkflowPhase) bool {
	switch phase {
	case domain.PhaseReserved, domain.PhaseWarmCopied, domain.PhasePausing,
		domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
		return true
	default:
		return false
	}
}

// Runtime identities overlay a copy of the immutable planned workload.
func podWorkload(
	plan v1alpha1.WorkloadSpec,
	checkpoint *v1alpha1.PodMigrationWorkloadStatus,
) v1alpha1.WorkloadSpec {
	workload := plan.DeepCopy()
	if checkpoint != nil {
		// The pause-probe outcomes (e.g. whether the VMCluster CRD kept the
		// per-component paused field) live in the durable checkpoint, not in
		// the plan; the checkpoint always wins.
		if checkpoint.VMCluster != nil {
			workload.VMCluster = checkpoint.VMCluster.DeepCopy()
		}

		if checkpoint.Pod != nil {
			workload.Pod = checkpoint.Pod.DeepCopy()
		}

		workload.AffectedPods = slices.Clone(checkpoint.AffectedPods)
	}

	return *workload
}
