package app

import (
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func podMigrationPhase(phase v1alpha1.WorkflowPhase) bool {
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved,
		domain.PhaseWarmCopying, domain.PhaseWarmCopied, domain.PhasePausing, domain.PhasePaused,
		domain.PhaseFinalSyncing, domain.PhaseFinalSynced, domain.PhaseActivating,
		domain.PhaseActivated, domain.PhaseResuming, domain.PhaseCompleted,
		domain.PhaseAborting, domain.PhaseAborted, domain.PhaseRollingBack, domain.PhaseRolledBack:
		return true
	default:
		return false
	}
}

func validatePodMigrationLifecycle(status v1alpha1.WorkflowStatus) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "pod migration", message)
	}
	if status.Phase != "" && status.Phase != domain.PhaseFailed &&
		!podMigrationPhase(status.Phase) {
		return invalid("phase does not belong to pod migration")
	}

	if status.ResumeFrom != "" && !podMigrationPhase(status.ResumeFrom) {
		return invalid("resume checkpoint does not belong to pod migration")
	}

	if status.Phase == domain.PhaseFailed && status.ResumeFrom == "" {
		return invalid("failed pod migration requires a resume checkpoint")
	}

	phase := workflowResumePhase(status)
	if status.FailureReason != "" &&
		(status.FailureReason != domain.FailureDestinationCapacityExhausted ||
			(phase != domain.PhaseFinalSyncing && phase != domain.PhaseWarmCopying)) {
		return invalid("pod migration failure reason does not belong to the current stage")
	}

	return nil
}

func validatePodMigrationOverrides(
	requests []v1alpha1.VolumeRequest, volumes map[string]v1alpha1.VolumeSpec,
) error {
	seen := make(map[string]bool, len(requests))
	for _, request := range requests {
		volume, exists := volumes[request.SourcePVC.Name]
		if !exists || seen[request.SourcePVC.Name] ||
			!reservationReferenceMatches(request.SourcePVC, volume.SourcePVC) ||
			(request.SourcePV != nil && !reservationReferenceMatches(*request.SourcePV, volume.SourcePV)) ||
			(request.DestinationPVC != nil && !reservationReferenceMatches(*request.DestinationPVC, volume.DestinationPVC)) {
			return domain.NewError(domain.ErrorValidation, "pod migration",
				"pod migration plan does not satisfy the requested volume identities")
		}

		seen[request.SourcePVC.Name] = true
	}

	return nil
}

func validatePodMigrationWorkload(
	request v1alpha1.LocalResourceReference,
	workload v1alpha1.WorkloadSpec,
) error {
	invalid := func(message string) error {
		return domain.NewError(domain.ErrorValidation, "pod migration", message)
	}
	if workload.Pod == nil || workload.Pod.Name == "" || workload.Pod.UID == "" ||
		request.Name == "" || !reservationReferenceMatches(request, *workload.Pod) {
		return invalid("planned workload must identify the requested Pod by name and UID")
	}

	if workload.OriginalObject != nil &&
		len(workload.OriginalObject.Raw) > domain.MaxOriginalPodSnapshotBytes {
		return invalid("workload original object exceeds the snapshot byte limit")
	}

	if workload.Adapter != v1alpha1.WorkloadStandalone &&
		(workload.Controller == nil || workload.Controller.Name == "" || workload.Controller.UID == "") {
		return invalid("planned workload requires its controller name and UID")
	}

	if (workload.KubeBlocks != nil && workload.Adapter != v1alpha1.WorkloadKubeBlocks) ||
		(workload.VMCluster != nil && workload.Adapter != v1alpha1.WorkloadVMCluster) ||
		(workload.Grafana != nil && workload.Adapter != v1alpha1.WorkloadGrafana) {
		return invalid("workload contains settings for an unrelated adapter")
	}

	seen := make(map[string]bool, len(workload.AffectedPods))

	selected := false
	for _, pod := range workload.AffectedPods {
		if pod.Name == "" || pod.UID == "" || seen[pod.Name] {
			return invalid("affected Pods require distinct names and nonempty UIDs")
		}

		seen[pod.Name] = true
		selected = selected || pod == *workload.Pod
	}

	return validatePodWorkloadAdapter(workload, selected)
}

func validatePodWorkloadAdapter(workload v1alpha1.WorkloadSpec, selected bool) error {
	invalid := func(message string) error { return domain.NewError(domain.ErrorValidation, "pod migration", message) }
	switch workload.Adapter {
	case v1alpha1.WorkloadStandalone, v1alpha1.WorkloadStatefulSet, v1alpha1.WorkloadVictoriaLogs:
	case v1alpha1.WorkloadDeployment:
		if workload.OriginalReplicas == nil || *workload.OriginalReplicas <= 0 || !selected {
			return invalid(
				"Deployment workload requires positive original replicas and the selected Pod in its affected set",
			)
		}
	case v1alpha1.WorkloadKubeBlocks:
		if workload.KubeBlocks == nil || workload.KubeBlocks.Cluster == "" ||
			workload.KubeBlocks.ClusterUID == "" {
			return invalid("KubeBlocks workload requires its Cluster name and UID")
		}
	case v1alpha1.WorkloadVMCluster:
		if workload.VMCluster == nil || workload.VMCluster.Name == "" ||
			workload.VMCluster.UID == "" {
			return invalid("VMCluster workload requires its name and UID")
		}
	case v1alpha1.WorkloadGrafana:
		if workload.Grafana == nil || workload.Grafana.Name == "" || workload.Grafana.UID == "" {
			return invalid("Grafana workload requires its name and UID")
		}
	default:
		return invalid(fmt.Sprintf("unsupported workload adapter %q", workload.Adapter))
	}

	return nil
}

func validatePodCheckpointReference(pod v1alpha1.ObjectReference, namespace string) error {
	if pod.Name == "" || pod.UID == "" || pod.Namespace != namespace {
		return domain.NewError(domain.ErrorValidation, "pod migration",
			"workload checkpoint requires a Pod name and UID in the source namespace")
	}

	return nil
}

func validatePodMigrationTransition(current, next v1alpha1.WorkflowPhase) error {
	edges := map[v1alpha1.WorkflowPhase][]v1alpha1.WorkflowPhase{
		"": {domain.PhaseAborted},
		domain.PhasePlanned: {
			domain.PhaseReserving,
			domain.PhaseAborting,
			domain.PhaseAborted,
		},
		domain.PhaseReserving: {domain.PhaseReserved, domain.PhaseAborting},
		domain.PhaseReserved: {
			domain.PhaseWarmCopying,
			domain.PhasePausing,
			domain.PhaseAborting,
		},
		domain.PhaseWarmCopying: {domain.PhaseWarmCopied, domain.PhaseAborting},
		domain.PhaseWarmCopied: {
			domain.PhaseWarmCopying,
			domain.PhasePausing,
			domain.PhaseAborting,
		},
		domain.PhasePausing:      {domain.PhasePaused, domain.PhaseAborting},
		domain.PhasePaused:       {domain.PhaseFinalSyncing, domain.PhaseAborting},
		domain.PhaseFinalSyncing: {domain.PhaseFinalSynced, domain.PhaseAborting},
		domain.PhaseFinalSynced: {
			domain.PhaseFinalSyncing,
			domain.PhaseActivating,
			domain.PhaseRollingBack,
			domain.PhaseAborting,
		},
		domain.PhaseActivating:  {domain.PhaseActivated, domain.PhaseRollingBack},
		domain.PhaseActivated:   {domain.PhaseResuming, domain.PhaseRollingBack},
		domain.PhaseResuming:    {domain.PhaseCompleted, domain.PhaseRollingBack},
		domain.PhaseCompleted:   {domain.PhaseRollingBack},
		domain.PhaseRollingBack: {domain.PhaseRolledBack},
		domain.PhaseAborting:    {domain.PhaseAborted},
	}
	if ((current == next || next == domain.PhaseFailed) && podMigrationPhase(current)) ||
		slices.Contains(edges[current], next) {
		return nil
	}

	return domain.NewError(domain.ErrorPrecondition, "pod migration",
		fmt.Sprintf("cannot transition from %s to %s", current, next))
}
