package cli

import (
	"fmt"
	"io"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func printPlanResult(
	cmd interface{ ErrOrStderr() io.Writer },
	runtime *commandRuntime,
	plan *domain.TransferPlan,
	advice func(domain.Check) string,
) error {
	if err := runtime.printer.Print(plan); err != nil {
		return reportPlanningError(cmd, err)
	}

	message := "\nPlanning completed without cluster mutations. Resolve the failed checks, then rerun the command."
	if plan.Summary().Ready {
		message = "\nDry-run completed without cluster mutations. Run the write command with the same inputs and --dry-run=false; provide --yes or typed approval when requested."
	}

	if _, err := fmt.Fprintln(cmd.ErrOrStderr(), message); err != nil {
		return err
	}

	if plan.Summary().Ready {
		return nil
	}

	return writePlanFailureGuidance(cmd.ErrOrStderr(), plan.Checks, advice)
}

func writePlanFailureGuidance(
	w io.Writer,
	checks []domain.Check,
	advise func(domain.Check) string,
) error {
	seen := make(map[string]struct{})
	for _, check := range checks {
		if check.Passed || check.Severity != domain.SeverityError {
			continue
		}

		advice := advise(check)
		if advice == "" {
			continue
		}

		if _, exists := seen[advice]; exists {
			continue
		}

		seen[advice] = struct{}{}
		if _, err := fmt.Fprintln(w, "  "+advice); err != nil {
			return err
		}
	}

	return nil
}

func offlineMigrationPlanFailureAdvice(check domain.Check) string {
	if check.Name == domain.CheckNamePVCConsumers {
		return "Offline PVC action: stop every consumer of the source PVC, then rerun migrate plan after the PVC has no active Pod references."
	}
	return commonPlanFailureAdvice(check)
}

func copyPlanFailureAdvice(check domain.Check) string {
	if check.Name == domain.CheckNameWarmCopyMount {
		return "Copy action: stop all active PVC consumers and rerun without --online, or use storage that explicitly supports a second same-node Pod mount."
	}
	return commonPlanFailureAdvice(check)
}

func podMigrationPlanAdvice(workload *v1alpha1.KubeBlocksSpec) func(domain.Check) string {
	return func(check domain.Check) string {
		if advice := workloadFailureAdvice(check); advice != "" {
			return advice
		}

		switch {
		case check.Name == domain.CheckNameControllerAdapter && strings.Contains(check.Message, "discover KubeBlocks"):
			return ""
		case check.Name == domain.CheckNameControllerAdapter:
			return "Workload action: use a supported workload adapter or the controller's native maintenance procedure, then rerun the plan; ordinary Deployments require no operator owner, and directly scaled Deployments and StatefulSets require no HorizontalPodAutoscaler."
		case check.Name == domain.CheckNamePVCConsumers:
			return "Real-time Pod action: stop every consumer outside the selected workload, then rerun migrate-pod plan; migrate-pod coordinates one workload and cannot cut over multiple independent workloads in one session."
		case workload != nil && (check.Name == domain.CheckNameStorageCapacity || check.Name == domain.CheckNameDestinationCapacity):
			return kubeBlocksRealtimeCapacityAdvice(workload)
		case check.Name == domain.CheckNameWarmCopyMount:
			if strings.Contains(check.Message, "OpenEBS LVM") {
				return "OpenEBS LVM action: rerun migrate-pod with --precopy-passes 0 to skip warm copy and proceed directly to controlled cutover and final sync, or explicitly pass --openebs-lvm-enable-shared to temporarily patch the matching LVMVolume before the mount probe."
			}
			return "Warm-copy action: rerun migrate-pod with --precopy-passes 0 to skip warm copy and proceed directly to controlled cutover and final sync, or use storage that explicitly supports a second same-node Pod mount."

		default:
			return commonPlanFailureAdvice(check)
		}
	}
}

func workloadFailureAdvice(check domain.Check) string {
	switch {
	case strings.Contains(check.Message, "PVC retention whenScaled is"):
		return "StatefulSet action: set persistentVolumeClaimRetentionPolicy.whenScaled=Retain and verify the StatefulSet before rerunning the plan."
	case strings.Contains(check.Message, "scale-down affects"):
		return "StatefulSet action: complete an application switchover, or explicitly acknowledge the restart with --allow-leader-downtime when the workload can tolerate it."
	case strings.Contains(check.Message, "switchoverCandidate applies only when the selected InstanceSet Pod has a leader role"):
		return "KubeBlocks action: remove --switchover-candidate when migrating a non-leader InstanceSet Pod, then rerun the plan."
	case strings.Contains(check.Message, "switchoverCandidate is supported only for InstanceSet-backed KubeBlocks components"):
		return "KubeBlocks action: remove --switchover-candidate for a legacy KubeBlocks component; its Stop/Start OpsRequest already pauses the affected Cluster or component."
	case strings.Contains(check.Message, "KubeBlocks Redis addon does not provide a Switchover action"):
		return "KubeBlocks Redis action: remove --switchover-candidate and rerun with --allow-leader-downtime."
	default:
		return ""
	}
}

func commonPlanFailureAdvice(check domain.Check) string {
	switch check.Name {
	case domain.CheckNamePVCConsumers:
		return "PVC action: stop unmanaged consumers, or select the owning workload with --pod, then verify that every PVC consumer belongs to the migration unit before rerunning the plan."
	case domain.CheckNameTargetNode:
		return "Node action: choose a Ready, schedulable target with --target-node, or correct the target node condition before rerunning the plan."
	case domain.CheckNameStorageTopology, domain.CheckNameStorageCapacity:
		return "Storage action: choose a compatible StorageClass or target node, then verify topology and capacity before rerunning the plan."
	case domain.CheckNameDestinationCapacity:
		return "Capacity action: correct --destination-capacity, or add --allow-volume-shrink only after verifying the copied data fits in every smaller destination PVC."
	case domain.CheckNameSourceUsage:
		return "Usage action: use a destination that is at least the source capacity, or independently verify the data size and rerun with --skip-source-usage-check."
	case domain.CheckNameMigrationNeeded:
		return "Migration action: the requested node and StorageClass already match; use --force-reprovision only for an intentional backing-PV replacement."
	default:
		return ""
	}
}

func kubeBlocksRealtimeCapacityAdvice(workload *v1alpha1.KubeBlocksSpec) string {
	if workload == nil || workload.Cluster == "" ||
		workload.Component == "" {
		return "KubeBlocks action: update the Cluster component volumeClaimTemplates storage request, then rerun migrate-pod."
	}

	return fmt.Sprintf(
		"KubeBlocks action: update Cluster %s component %s volumeClaimTemplates storage request, then rerun migrate-pod.",
		workload.Cluster,
		workload.Component,
	)
}
