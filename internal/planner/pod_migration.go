package planner

import (
	"context"
	"fmt"
	"slices"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (p *Planner) PlanNamespacedPodMigration(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	image string,
) (*domain.TransferPlan, error) {
	if object == nil || object.Name == "" || object.Namespace == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"plan pod migration",
			"pod migration name and namespace are required",
		)
	}

	if !migrationCanPlan(object.Status.WorkflowStatus, object.Status.Plan != nil,
		len(object.Status.Volumes), object.DeletionTimestamp != nil) ||
		object.Status.Workload != nil || object.Status.WarmPassesCompleted != 0 ||
		object.Status.OriginalPodSnapshotHash != "" || len(object.Status.OpenEBSLVMSharedMounts) != 0 {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"plan pod migration",
			"only an unplanned pod migration can be planned",
		)
	}

	spec := object.Spec.DeepCopy()
	if err := validatePodMigrationSpec(*spec); err != nil {
		return nil, err
	}

	report, resolved, err := p.resolvePodMigration(
		ctx,
		object.Name,
		*spec,
		object.Namespace,
		object.Namespace,
		object.Namespace,
		image,
	)
	if err != nil {
		return nil, err
	}

	if report.Ready {
		object.Status.Plan = resolved.DeepCopy()
		if resolved.Workload.Adapter == v1alpha1.WorkloadStandalone &&
			resolved.Workload.OriginalObject != nil {
			object.Status.OriginalPodSnapshotHash = kube.PodSnapshotHash(
				resolved.Workload.OriginalObject.Raw,
			)
		}
	}

	return report, nil
}

func (p *Planner) resolvePodMigration(
	ctx context.Context,
	name string,
	spec v1alpha1.PodMigrationSpec,
	sourceNamespace, temporaryNamespace, sessionNamespace, image string,
) (*domain.TransferPlan, *v1alpha1.PodMigrationPlan, error) {
	options := transferInput{
		SessionID:            name,
		SourceNamespace:      sourceNamespace,
		TemporaryNamespace:   temporaryNamespace,
		SessionNamespace:     sessionNamespace,
		DestinationNamespace: sourceNamespace,
		StagingNamespace:     temporaryNamespace,
		ToolImage:            image,
	}

	if err := domain.ValidateUnusedStoragePolicy(spec.UnusedStoragePolicy); err != nil {
		return nil, nil, err
	}

	if p.controllers == nil {
		return nil, nil, domain.NewError(
			domain.ErrorInternal,
			"plan pod migration",
			"controller manager is unavailable",
		)
	}

	state := p.newTransferPlanState(
		options,
		domain.OperationMigratePod,
		spec.TransferOptions,
		spec.Volumes,
	)

	sourcePod, err := p.selectPlanVolumes(ctx, &state, spec.Volumes, &spec.Pod)
	if err != nil {
		return nil, nil, err
	}

	workload := p.discoverPodMigrationWorkload(
		ctx,
		state.plan,
		state.options.SourceNamespace,
		spec.Pod, spec.SwitchoverCandidate, spec.AllowLeaderDowntime,
		sourcePod,
	)
	state.plan.Workload = *workload.DeepCopy()

	if err := p.loadPlanContext(ctx, &state); err != nil {
		return nil, nil, err
	}

	sources := p.loadPlanVolumeInputs(ctx, &state)

	validSources := make([]planVolumeInput, 0, len(sources))
	for _, source := range sources {
		if checkPodMigrationCapacity(
			state.plan,
			source.pvc,
			source.capacity,
			state.requestedCapacities[source.index],
			workload.Adapter == v1alpha1.WorkloadKubeBlocks ||
				isKubeBlocksPod(sourcePod),
			workload.KubeBlocks,
		) {
			validSources = append(validSources, source)
		}
	}

	inputs := p.planVolumes(ctx, &state, validSources)
	recordTransferScopeChecks(
		state.plan,
		state.options.SourceNamespace,
		state.volumeSpecs,
		domain.SeverityWarning,
	)
	inspectShared, patchShared := p.checkPodMigrationPlanConsumers(
		ctx,
		&state,
		sourcePod,
		workload.Adapter,
		workload.AffectedPods,
		inputs,
		spec.OpenEBSLVMEnableShared,
	)

	p.selectPlanTarget(
		&state, workload.Adapter, sourcePod, availabilityZone(state.inventory.sourceNode),
	)

	if state.targetNode != nil {
		p.checkPodTargetScheduling(state.plan, sourcePod, workload.Adapter, state.targetNode)
	}

	plan, err := p.completeTransferPlan(ctx, &state, spec.Volumes)
	if err != nil {
		return nil, nil, err
	}

	if spec.OpenEBSLVMEnableShared && len(state.pvcNames) > 0 {
		namespace, namespaceErr := p.client.CoreV1().Namespaces().Get(
			ctx, state.options.SourceNamespace, metav1.GetOptions{},
		)
		if !state.autoTargetNode {
			nodes, nodesErr := p.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if nodes != nil {
				state.inventory.nodes = nodes.Items
			}

			state.inventory.nodesErr = nodesErr
		}

		checkPodSharedRWOScheduling(
			&state, sourcePod, workload.AffectedPods, namespace, namespaceErr,
		)
	}

	if sourcePod != nil {
		checkPodMigrationAvailabilityZone(
			plan, state.options.SourceNode, state.inventory.sourceNode, state.targetNode,
		)
	}

	checkPodMigrationNeeded(
		plan, sourcePod, state.options.TargetNode,
		state.storageClassChanged, len(state.plannedVolumes), len(state.pvcNames),
		spec.ForceReprovision,
		p.guidanceAudience(),
	)

	p.checkActivationPVCPolicies(ctx, plan, state.options.SourceNamespace, state.volumeSpecs)

	if len(state.plannedVolumes) > 0 && sourcePod != nil {
		p.checkPodDependencies(ctx, plan, sourcePod)

		for _, issue := range podMigrationIssues(
			sourcePod.Spec, state.options.SourceNode, state.options.TargetNode,
		) {
			plan.AddCheck(failed(domain.CheckNamePodScheduling, issue))
		}

		recreatedSpec := sourcePod.Spec.DeepCopy()

		recreatedSpec.NodeName = state.options.TargetNode
		for _, issue := range recreationSchedulingIssues(
			ctx, p.client, sourcePod, state.options.TargetNode, *recreatedSpec,
		) {
			if spec.AllowPlacementViolation {
				plan.AddCheck(warned(domain.CheckNamePodScheduling, issue))
			} else {
				plan.AddCheck(failed(domain.CheckNamePodScheduling, issue))
			}
		}
	}

	p.finalizePlanResources(ctx, &state, transferChartResourceEstimates(
		state.options.SourceNamespace, state.options.StagingNamespace,
		state.options.Strategies, len(state.plannedVolumes),
	), podMigrationProbePodPeaks(
		state.options.TargetNode,
		state.options.Strategies,
		state.plannedVolumes,
		spec.PrecopyPasses,
	))

	resolved := v1alpha1.PodMigrationPlan{
		UnusedStoragePolicy:    state.options.UnusedStoragePolicy,
		Volumes:                state.volumeSpecs,
		SourceNode:             state.options.SourceNode,
		TargetNode:             state.options.TargetNode,
		ToolImage:              state.options.ToolImage,
		Strategies:             state.options.Strategies,
		VerifyChecksum:         state.options.VerifyChecksum,
		DeleteExtraneous:       state.options.DeleteExtraneousValue(),
		SkipSourceUsageCheck:   state.options.SkipSourceUsageCheck,
		Workload:               *state.plan.Workload.DeepCopy(),
		PrecopyPasses:          spec.PrecopyPasses,
		OpenEBSLVMEnableShared: spec.OpenEBSLVMEnableShared,
	}
	if len(resolved.Volumes) > 0 {
		p.checkPodMigrationPermissions(
			ctx,
			plan,
			name,
			sourceNamespace, temporaryNamespace, sessionNamespace,
			resolved.Strategies, resolved.Workload,
			inspectShared,
			patchShared,
		)
	}

	return plan, &resolved, nil
}

func podMigrationProbePodPeaks(
	targetNode string,
	strategies []string,
	volumes []domain.PlannedVolume,
	precopyPasses int,
) map[string]int {
	final := migrationProbePodPeaks(targetNode, strategies, volumes)
	if precopyPasses <= 0 {
		return final
	}

	return mergeProbePodPeaks(final, transferProbePods(targetNode, strategies, volumes, true))
}

func (p *Planner) checkPodMigrationPlanConsumers(
	ctx context.Context,
	state *planState,
	sourcePod *corev1.Pod,
	workloadKind v1alpha1.WorkloadKind,
	affectedPods []v1alpha1.LocalResourceReference,
	inputs []planVolumeInput,
	enableShared bool,
) (bool, bool) {
	inspectShared, patchShared := false, false
	for index, input := range inputs {
		p.checkPVCFinalizers(state.plan, input.pvc)

		consumers, listed := collectPVCConsumers(
			state.plan, input.pvc, state.inventory.namespacePods, state.inventory.namespacePodsErr,
			kube.PodPreventsSafePVCDeletion,
		)
		if !listed {
			continue
		}

		checkPodMigrationConsumers(
			state.plan,
			input.pvc,
			sourcePod,
			workloadKind,
			affectedPods,
			consumers,
		)
		concurrentConsumers := migrationUnitConsumerCount(
			affectedPods,
			sourcePod,
			consumers,
		)
		state.plannedVolumes[index].ConcurrentConsumers = concurrentConsumers
		state.volumeSpecs[index].ConcurrentConsumers = concurrentConsumers

		// The cutover co-mounts the source for the final-sync tool probe
		// while the workload still runs, whatever the precopy pass count, so
		// the shared-mount compatibility check cannot be skipped for
		// zero-pass plans.
		inspect, patch := p.checkWarmCopyMountCompatibility(
			ctx,
			state.plan,
			domain.OperationMigratePod,
			enableShared,
			input.pvc,
			input.pv,
			input.sourceClass,
			state.storageClasses[input.sourceClass],
			state.storageClassErrors[input.sourceClass],
			consumers,
		)
		inspectShared = inspectShared || inspect
		patchShared = patchShared || patch

		volume := state.volumeSpecs[index]
		if concurrentConsumers > 1 && slices.Contains(volume.AccessModes, corev1.ReadWriteOnce) &&
			state.plannedVolumes[index].CSIProvisioner == kube.OpenEBSLVMCSIDriver {
			inspectShared = true

			if enableShared {
				patchShared = true

				state.plan.AddCheck(passed(domain.CheckNameDestinationSharedMount,
					fmt.Sprintf(
						"execution will verify the provisioned destination PV and set its OpenEBS LVMVolume spec.shared=yes so %d consumers can mount RWO PVC %s/%s on one node",
						concurrentConsumers,
						input.pvc.Namespace,
						input.pvc.Name,
					),
				))
			} else {
				state.plan.AddCheck(failed(domain.CheckNameDestinationSharedMount,
					fmt.Sprintf(
						"destination OpenEBS LVM volume for RWO PVC %s/%s must support %d concurrent consumers; rerun with --openebs-lvm-enable-shared to authorize spec.shared=yes after provisioning",
						input.pvc.Namespace,
						input.pvc.Name,
						concurrentConsumers,
					),
				))
			}
		}

		if workloadKind == v1alpha1.WorkloadStandalone {
			for _, owner := range input.pvc.OwnerReferences {
				if owner.Kind == domain.KindPod {
					state.plan.AddCheck(failed(domain.CheckNamePVCOwnership,
						fmt.Sprintf("standalone Pod migration cannot preserve Pod-owned PVC %s/%s",
							input.pvc.Namespace, input.pvc.Name,
						),
					))
				}
			}
		}
	}

	return inspectShared, patchShared
}

func checkPodMigrationAvailabilityZone(
	plan checkRecorder,
	sourceNodeName string,
	sourceNode, targetNode *corev1.Node,
) {
	if targetNode == nil {
		return
	}

	sourceZone := availabilityZone(sourceNode)

	targetZone := availabilityZone(targetNode)
	if sourceZone == "" || targetZone == "" {
		plan.AddCheck(warned(
			domain.CheckNameAvailabilityZone,
			"source or target node has no availability-zone label; cross-zone Pod migration could not be verified",
		))

		return
	}

	if sourceZone != targetZone {
		plan.AddCheck(failed(
			domain.CheckNameAvailabilityZone,
			fmt.Sprintf(
				"real-time Pod migration cannot cross availability zones: source node %s is in %s, target node %s is in %s; use copy --online for cross-zone replication",
				sourceNodeName,
				sourceZone,
				targetNode.Name,
				targetZone,
			),
		))

		return
	}

	plan.AddCheck(passed(
		domain.CheckNameAvailabilityZone,
		"real-time Pod migration stays in availability zone "+sourceZone,
	))
}

func checkPodMigrationNeeded(
	plan checkRecorder,
	pod *corev1.Pod,
	targetNode string,
	storageClassChanged bool,
	plannedVolumes, selectedVolumes int,
	forceReprovision bool,
	audience domain.Presentation,
) {
	if pod == nil || pod.Spec.NodeName == "" || pod.Spec.NodeName != targetNode ||
		storageClassChanged || plannedVolumes == 0 || plannedVolumes != selectedVolumes {
		return
	}

	message := fmt.Sprintf(
		"Pod %s/%s already uses target node %s and every PVC already uses the requested StorageClass",
		pod.Namespace,
		pod.Name,
		targetNode,
	)
	if forceReprovision {
		plan.AddCheck(warned(domain.CheckNameForceReprovision,
			message+"; "+audience.FieldRef("forceReprovision")+
				" will replace the backing PVs"))
		return
	}

	plan.AddCheck(failed(domain.CheckNameMigrationNeeded,
		message+"; "+audience.FieldUse("forceReprovision")+
			" to intentionally replace the backing PVs"))
}

func (p *Planner) discoverPodMigrationWorkload(
	ctx context.Context,
	plan checkRecorder,
	namespace string,
	expected v1alpha1.LocalResourceReference,
	switchoverCandidate string,
	allowLeaderDowntime bool,
	pod *corev1.Pod,
) v1alpha1.WorkloadSpec {
	if pod == nil {
		return v1alpha1.WorkloadSpec{Adapter: v1alpha1.WorkloadNone}
	}

	workload, err := p.controllers.DiscoverPod(
		ctx, pod, namespace, expected, switchoverCandidate, allowLeaderDowntime,
		p.guidanceAudience(),
	)
	if err != nil {
		plan.AddCheck(failed(domain.CheckNameControllerAdapter, err.Error()))
		return v1alpha1.WorkloadSpec{Adapter: v1alpha1.WorkloadNone}
	}

	plan.AddCheck(passed(domain.CheckNameControllerAdapter,
		fmt.Sprintf("%s provides pause and resume semantics", workload.Adapter)))

	controllerKind := ""
	if workload.Controller != nil {
		controllerKind = workload.Controller.Kind
	}

	if message := kubeBlocksRoleWarning(
		controllerKind,
		workload.KubeBlocks,
	); message != "" {
		plan.AddCheck(warned(domain.CheckNameDatabaseRole, message))
	}

	if workload.KubeBlocks != nil {
		message := "KubeBlocks migration uses a Stop/Start OpsRequest for the legacy Cluster; its components share the downtime window and source PVCs remain retained"
		if strings.HasPrefix(workload.KubeBlocks.OpsAPIVersion, "operations.kubeblocks.io/") {
			message = "KubeBlocks migration uses a component-scoped Stop/Start OpsRequest for the legacy component; the component shares the downtime window and source PVCs remain retained"
		}

		if controllerKind == domain.KindInstanceSet {
			message = "KubeBlocks migration pauses InstanceSet reconciliation and stops only the selected Pod; sibling instances remain running while InstanceSet self-healing is suspended"
		}

		plan.AddCheck(warned(domain.CheckNameDatabasePauseScope, message))
	}

	return workload
}

func validatePodMigrationSpec(spec v1alpha1.PodMigrationSpec) error {
	if spec.Pod.Name == "" {
		return domain.NewError(domain.ErrorValidation, "plan pod migration", "Pod name is required")
	}

	if spec.PrecopyPasses < 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"plan pod migration",
			"warm-copy passes cannot be negative",
		)
	}

	for _, volume := range spec.Volumes {
		if volume.DestinationPVC != nil {
			return domain.NewError(domain.ErrorValidation, "plan pod migration",
				"Pod migration preserves PVC names; destinationPVC is not supported")
		}
	}

	return nil
}
