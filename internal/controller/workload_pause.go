package controller

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// Pause returns recovered Pod identities even when a later resource operation
// fails. The caller persists that checkpoint before retrying the transition.
func (m *Manager) Pause(
	ctx context.Context,
	owner, namespace string,
	workload v1alpha1.WorkloadSpec,
	phase, resumeFrom v1alpha1.WorkflowPhase,
) (*v1alpha1.PodMigrationWorkloadStatus, error) {
	if err := validateWorkloadScope(namespace, workload.Adapter); err != nil {
		return nil, err
	}

	pod := qualifiedWorkloadReference(workload.Pod, namespace)
	controller := qualifiedWorkloadReference(workload.Controller, namespace)
	affected := qualifiedWorkloadReferences(workload.AffectedPods, namespace)

	switch workload.Adapter {
	case v1alpha1.WorkloadNone:
		return nil, nil
	case v1alpha1.WorkloadStandalone:
		return nil, m.pauseStandalone(ctx, pod)
	case v1alpha1.WorkloadDeployment:
		observed, err := m.pauseDeployment(ctx, controller, workload.OriginalReplicas,
			affected, phase == domain.PhaseRollingBack)
		return observedWorkloadPods(observed), err
	case v1alpha1.WorkloadStatefulSet:
		return nil, m.pauseStatefulSet(ctx, controller, workload.OriginalReplicas,
			workload.Ordinal, affected)
	case v1alpha1.WorkloadVictoriaLogs:
		if err := m.pauseVictoriaLogs(ctx, owner, controller,
			workload.OriginalReplicas, affected); err != nil {
			return nil, err
		}

		return nil, m.VerifyPaused(ctx, owner, namespace, workload, phase, resumeFrom)
	case v1alpha1.WorkloadKubeBlocks:
		updated, err := m.pauseKubeBlocks(ctx, owner, pod, controller,
			workload.KubeBlocks, phase, resumeFrom)

		checkpoint := recoveredWorkloadPod(workload, updated)
		if err != nil {
			return checkpoint, err
		}

		if checkpoint != nil {
			workload.Pod, workload.AffectedPods = checkpoint.Pod, checkpoint.AffectedPods
		}

		return checkpoint, m.VerifyPaused(ctx, owner, namespace, workload, phase, resumeFrom)
	case v1alpha1.WorkloadVMCluster:
		return nil, m.pauseVMCluster(ctx, owner, namespace, controller,
			workload.OriginalReplicas, workload.Ordinal, affected, workload.VMCluster)
	case v1alpha1.WorkloadGrafana:
		return nil, m.pauseGrafana(ctx, owner, namespace, controller,
			workload.OriginalReplicas, affected, workload.Grafana)
	default:
		return nil, domain.NewError(domain.ErrorPrecondition, "pause workload",
			fmt.Sprintf("adapter %q is unsupported", workload.Adapter))
	}
}

func recoveredWorkloadPod(
	workload v1alpha1.WorkloadSpec,
	updated v1alpha1.ObjectReference,
) *v1alpha1.PodMigrationWorkloadStatus {
	if updated.UID == "" {
		return nil
	}

	checkpoint := (&v1alpha1.PodMigrationWorkloadStatus{
		Pod: workload.Pod, AffectedPods: workload.AffectedPods,
	}).DeepCopy()

	ref := localWorkloadReferences([]v1alpha1.ObjectReference{updated})[0]
	if checkpoint.Pod != nil && checkpoint.Pod.Name == updated.Name {
		checkpoint.Pod = &ref
	}

	for i := range checkpoint.AffectedPods {
		if checkpoint.AffectedPods[i].Name == updated.Name {
			checkpoint.AffectedPods[i] = ref
		}
	}

	return checkpoint
}
