package controller

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// Resume returns the identities that must be checkpointed, including those
// recovered before a later controller convergence failure.
func (m *Manager) Resume(
	ctx context.Context,
	owner, namespace string,
	workload v1alpha1.WorkloadSpec,
	resumeNode string,
	phase, resumeFrom v1alpha1.WorkflowPhase,
	history []v1alpha1.WorkflowHistoryEntry,
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
		previous := pod

		err := m.resumeStandalone(ctx, owner, &pod, workload.OriginalObject, resumeNode)
		if pod == previous {
			return nil, err
		}

		return recoveredWorkloadPod(workload, pod), err
	case v1alpha1.WorkloadDeployment:
		observed, err := m.resumeDeployment(ctx, controller, workload.OriginalReplicas)
		return observedWorkloadPods(observed), err
	case v1alpha1.WorkloadStatefulSet:
		observed, err := m.resumeStatefulSet(ctx, controller,
			workload.OriginalReplicas, workload.Ordinal, affected)
		return recoveredWorkloadPods(workload, observed), err
	case v1alpha1.WorkloadVictoriaLogs:
		observed, err := m.resumeVictoriaLogs(
			ctx,
			owner,
			controller,
			workload.OriginalReplicas,
			affected,
		)

		return recoveredWorkloadPods(workload, observed), err
	case v1alpha1.WorkloadKubeBlocks:
		updated, err := m.resumeKubeBlocks(ctx, owner, pod, controller, workload.KubeBlocks,
			phase, resumeFrom, kubeBlocksAbortStartedFromPausing(phase, resumeFrom, history))
		return recoveredWorkloadPod(workload, updated), err
	case v1alpha1.WorkloadVMCluster:
		observed, err := m.resumeVMCluster(ctx, owner, namespace, controller,
			workload.OriginalReplicas, workload.Ordinal, affected, workload.VMCluster)
		return recoveredWorkloadPods(workload, observed), err
	case v1alpha1.WorkloadGrafana:
		ready, err := m.resumeGrafana(ctx, owner, namespace, controller,
			workload.OriginalReplicas, workload.Grafana)
		if ready.UID == "" {
			return nil, err
		}

		checkpoint := (&v1alpha1.PodMigrationWorkloadStatus{
			Pod: workload.Pod, AffectedPods: workload.AffectedPods,
		}).DeepCopy()
		ref := localWorkloadReferences([]v1alpha1.ObjectReference{ready})[0]
		checkpoint.Pod = &ref
		// Grafana's representative Pod may acquire a new generated name.
		if len(checkpoint.AffectedPods) == 1 {
			checkpoint.AffectedPods[0] = ref
		}

		return checkpoint, err
	default:
		return nil, domain.NewError(domain.ErrorPrecondition, "resume workload",
			fmt.Sprintf("adapter %q is unsupported", workload.Adapter))
	}
}

func observedWorkloadPods(
	observed []v1alpha1.ObjectReference,
) *v1alpha1.PodMigrationWorkloadStatus {
	if len(observed) == 0 {
		return nil
	}

	refs := localWorkloadReferences(observed)
	selected := refs[0]

	return &v1alpha1.PodMigrationWorkloadStatus{Pod: &selected, AffectedPods: refs}
}

func recoveredWorkloadPods(
	workload v1alpha1.WorkloadSpec,
	observed []v1alpha1.ObjectReference,
) *v1alpha1.PodMigrationWorkloadStatus {
	var checkpoint *v1alpha1.PodMigrationWorkloadStatus
	for _, ref := range observed {
		if next := recoveredWorkloadPod(workload, ref); next != nil {
			checkpoint = next
			workload.Pod, workload.AffectedPods = next.Pod, next.AffectedPods
		}
	}

	return checkpoint
}
