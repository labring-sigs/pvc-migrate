package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func printPlanResult(
	cmd interface{ ErrOrStderr() io.Writer },
	runtime *commandRuntime,
	plan *domain.MigrationPlan,
) error {
	if err := runtime.printer.Print(plan); err != nil {
		return reportPlanningError(cmd, err)
	}

	message := "\nPlanning completed without cluster mutations. Resolve the failed checks, then rerun the command."
	if plan.Ready {
		message = "\nDry-run completed without cluster mutations. Run the write command with the same inputs and --dry-run=false; provide --yes or typed approval when requested."
	}

	if _, err := fmt.Fprintln(cmd.ErrOrStderr(), message); err != nil {
		return err
	}

	if plan.Ready {
		return nil
	}

	return writePlanFailureGuidance(cmd.ErrOrStderr(), plan)
}

func writePlanFailureGuidance(w io.Writer, plan *domain.MigrationPlan) error {
	if plan == nil {
		return nil
	}

	seen := make(map[string]struct{})
	for _, check := range plan.Checks {
		if check.Passed || check.Severity != domain.SeverityError {
			continue
		}

		advice, suppress := planFailureAdvice(plan, check)
		if suppress || advice == "" {
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

func planFailureAdvice(plan *domain.MigrationPlan, check domain.Check) (string, bool) {
	if advice := workloadFailureAdvice(check); advice != "" {
		return advice, false
	}

	if check.Name == "controller-adapter" &&
		strings.Contains(check.Message, "discover KubeBlocks") {
		return "", true
	}

	if advice := operationFailureAdvice(plan, check); advice != "" {
		return advice, false
	}

	return storageFailureAdvice(plan, check), false
}

func workloadFailureAdvice(check domain.Check) string {
	switch {
	case strings.Contains(check.Message, "PVC retention whenScaled is"):
		return "StatefulSet action: set persistentVolumeClaimRetentionPolicy.whenScaled=Retain and verify the StatefulSet before rerunning the plan."
	case strings.Contains(check.Message, "scale-down affects"):
		return "StatefulSet action: complete an application switchover, or explicitly acknowledge the restart with --allow-leader-downtime when the workload can tolerate it."
	case strings.Contains(check.Message, "--kubeblocks-candidate applies only when the selected InstanceSet Pod has a leader role"):
		return "KubeBlocks action: remove --kubeblocks-candidate when migrating a non-leader InstanceSet Pod, then rerun the plan."
	case strings.Contains(check.Message, "--kubeblocks-candidate is supported only for InstanceSet-backed KubeBlocks components"):
		return "KubeBlocks action: remove --kubeblocks-candidate for a legacy KubeBlocks component; its Stop/Start OpsRequest already pauses the affected Cluster or component."
	case strings.Contains(check.Message, "KubeBlocks Redis addon does not provide a Switchover action"):
		return "KubeBlocks Redis action: remove --kubeblocks-candidate and rerun with --allow-leader-downtime."
	default:
		return ""
	}
}

func operationFailureAdvice(plan *domain.MigrationPlan, check domain.Check) string {
	switch {
	case check.Name == "pvc-consumers" && isOfflineMigrationPlan(plan):
		return "Offline PVC action: stop every consumer of the source PVC, then rerun migrate plan after the PVC has no active Pod references."
	case check.Name == "pvc-consumers" && plan != nil && plan.SessionSpec.Operation() == domain.OperationMigratePod:
		return "Real-time Pod action: stop every consumer outside the selected workload, then rerun migrate-pod plan; migrate-pod coordinates one workload and cannot cut over multiple independent workloads in one session."
	case check.Name == "pvc-consumers":
		return "PVC action: stop unmanaged consumers, or select the owning workload with --pod, then verify that every PVC consumer belongs to the migration unit before rerunning the plan."
	case check.Name == "controller-adapter":
		return "Workload action: use a supported workload adapter or the controller's native maintenance procedure, then rerun the plan; ordinary Deployments require no operator owner, and directly scaled Deployments and StatefulSets require no HorizontalPodAutoscaler."
	case check.Name == "target-node":
		return "Node action: choose a Ready, schedulable target with --target-node, or correct the target node condition before rerunning the plan."
	default:
		return ""
	}
}

func storageFailureAdvice(plan *domain.MigrationPlan, check domain.Check) string {
	switch {
	case isKubeBlocksRealtimePlan(plan) && (check.Name == "storage-capacity" || check.Name == "destination-capacity"):
		return kubeBlocksRealtimeCapacityAdvice(plan)
	case check.Name == "storage-topology" || check.Name == "storage-capacity":
		return "Storage action: choose a compatible StorageClass or target node, then verify topology and capacity before rerunning the plan."
	case check.Name == "destination-capacity":
		return "Capacity action: correct --destination-capacity, or add --allow-volume-shrink only after verifying the copied data fits in every smaller destination PVC."
	case check.Name == "source-usage":
		return "Usage action: use a destination that is at least the source capacity, or independently verify the data size and rerun with --skip-source-usage-check."
	case check.Name == "migration-needed":
		return "Migration action: the requested node and StorageClass already match; use --force-reprovision only for an intentional backing-PV replacement."
	case check.Name == "warm-copy-mount" && plan.SessionSpec.Operation() == domain.OperationCopy:
		return "Copy action: stop all active PVC consumers and rerun without --online, or use storage that explicitly supports a second same-node Pod mount."
	case check.Name == "warm-copy-mount":
		if strings.Contains(check.Message, "OpenEBS LVM") {
			return "OpenEBS LVM action: rerun migrate-pod with --precopy-passes 0 to skip warm copy and proceed directly to controlled cutover and final sync, or explicitly pass --openebs-lvm-enable-shared to temporarily patch the matching LVMVolume before the mount probe."
		}
		return "Warm-copy action: rerun migrate-pod with --precopy-passes 0 to skip warm copy and proceed directly to controlled cutover and final sync, or use storage that explicitly supports a second same-node Pod mount."
	default:
		return ""
	}
}

func isOfflineMigrationPlan(plan *domain.MigrationPlan) bool {
	return plan != nil && plan.SessionSpec.Operation() == domain.OperationMigrate
}

func isKubeBlocksRealtimePlan(plan *domain.MigrationPlan) bool {
	if plan == nil || plan.SessionSpec.Operation() != domain.OperationMigratePod {
		return false
	}

	workload := plan.SessionSpec.Workload()

	return workload.Adapter == domain.WorkloadKubeBlocks && workload.KubeBlocks != nil
}

func kubeBlocksRealtimeCapacityAdvice(plan *domain.MigrationPlan) string {
	workload := plan.SessionSpec.Workload()
	if workload.KubeBlocks == nil || workload.KubeBlocks.Cluster == "" ||
		workload.KubeBlocks.Component == "" {
		return "KubeBlocks action: update the Cluster component volumeClaimTemplates storage request, then rerun migrate-pod."
	}

	return fmt.Sprintf(
		"KubeBlocks action: update Cluster %s component %s volumeClaimTemplates storage request, then rerun migrate-pod.",
		workload.KubeBlocks.Cluster,
		workload.KubeBlocks.Component,
	)
}
