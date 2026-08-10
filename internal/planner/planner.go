package planner

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

type Options struct {
	SessionID            string
	Operation            domain.Operation
	SourceNamespace      string
	TemporaryNamespace   string
	DestinationNamespace string
	SessionNamespace     string
	StagingNamespace     string
	ToolImage            string
	CapacityAwareness    domain.CapacityAwareness
	SourcePVCs           []string
	DestinationPVCs      []string
	PodName              string
	SourceNode           string
	TargetNode           string
	DestinationClass     string
	Strategies           []string
	Online               bool
	VerifyChecksum       bool
	DeleteExtraneous     bool
	SwitchoverCandidate  string
	AllowLeaderDowntime  bool
}

type Planner struct {
	client      kubernetes.Interface
	controllers *controller.Manager
}

func New(client kubernetes.Interface, controllers *controller.Manager) *Planner {
	return &Planner{client: client, controllers: controllers}
}

func (p *Planner) Plan(ctx context.Context, options Options) (*domain.MigrationPlan, error) {
	autoStrategyRequested := len(options.Strategies) == 0 || (len(options.Strategies) == 1 && containsStrategy(options.Strategies, domain.StrategyAuto))
	autoTargetNodeRequested := isAutoNode(options.TargetNode)
	options = applyDefaults(options)
	if autoTargetNodeRequested {
		options.TargetNode = ""
	}
	plan := &domain.MigrationPlan{
		APIVersion:           domain.SessionAPIVersion,
		Kind:                 domain.MigrationPlanKind,
		SessionID:            options.SessionID,
		SourceNamespace:      options.SourceNamespace,
		TemporaryNamespace:   options.TemporaryNamespace,
		DestinationNamespace: options.DestinationNamespace,
		SessionNamespace:     options.SessionNamespace,
		ToolImage:            options.ToolImage,
		CapacityAwareness:    options.CapacityAwareness,
		TargetNode:           options.TargetNode,
		Ready:                true,
		TemporaryUsage: domain.ResourceEstimate{
			ByStorageClass:     map[string]string{},
			PVCsByStorageClass: map[string]int{},
		},
		RollbackRetention: domain.ResourceEstimate{
			ByStorageClass:     map[string]string{},
			PVCsByStorageClass: map[string]int{},
		},
	}
	if _, err := kube.NormalizeToolImage(options.ToolImage); err != nil {
		plan.AddCheck(failed("tool-image", err.Error()))
	}
	if problems := validation.IsDNS1123Label(options.SessionID); len(problems) > 0 {
		plan.AddCheck(failed("session-id", strings.Join(problems, "; ")))
	}
	for _, strategy := range options.Strategies {
		if !supportedStrategy(strategy) {
			plan.AddCheck(failed("strategy", fmt.Sprintf("unsupported pv-migrate strategy %q", strategy)))
		}
	}
	if !validCapacityAwareness(options.CapacityAwareness) {
		plan.AddCheck(failed("capacity-awareness", fmt.Sprintf("unsupported capacity awareness mode %q; use auto, require, or off", options.CapacityAwareness)))
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "source namespace", value: options.SourceNamespace},
		{name: "staging namespace", value: options.StagingNamespace},
		{name: "session namespace", value: options.SessionNamespace},
	} {
		if problems := validation.IsDNS1123Label(field.value); len(problems) > 0 {
			plan.AddCheck(failed("namespace", fmt.Sprintf("%s %q is invalid: %s", field.name, field.value, strings.Join(problems, "; "))))
		}
	}
	if options.Operation == domain.OperationCopy {
		if options.Online {
			plan.AddCheck(warned("copy-mode", "online copy performs one finite warm pass with file-level consistency while source Pods may keep writing"))
		} else {
			plan.AddCheck(passed("copy-mode", "offline copy requires every source PVC to have zero active Pod consumers"))
		}
	}

	var workload domain.WorkloadSpec
	pvcNames := uniqueSorted(options.SourcePVCs)
	var sourcePod *corev1.Pod
	if options.PodName != "" {
		if len(options.SourcePVCs) > 0 {
			plan.AddCheck(failed("source-pvc", "--source-pvc cannot be combined with --pod; the Pod PVC set is migrated as one unit"))
		}
		if options.Operation == domain.OperationCopy {
			workload = domain.WorkloadSpec{Adapter: domain.WorkloadNone}
			plan.AddCheck(passed("controller-adapter", "copy does not mutate or pause the selected workload"))
		} else {
			if p.controllers == nil {
				return nil, domain.NewError(domain.ErrorInternal, "plan", "controller manager is unavailable")
			}
			discovered, err := p.controllers.Discover(ctx, controller.DiscoverOptions{
				Namespace:           options.SourceNamespace,
				PodName:             options.PodName,
				SwitchoverCandidate: options.SwitchoverCandidate,
				AllowLeaderDowntime: options.AllowLeaderDowntime,
			})
			if err != nil {
				plan.AddCheck(failed("controller-adapter", err.Error()))
			} else {
				workload = discovered
				plan.AddCheck(passed("controller-adapter", fmt.Sprintf("%s provides pause and resume semantics", workload.Adapter)))
				if workload.KubeBlocks != nil && isRiskRole(workload.KubeBlocks.Role) {
					plan.AddCheck(warned("database-role", fmt.Sprintf("selected KubeBlocks instance role=%s; switchover target=%s", workload.KubeBlocks.Role, workload.KubeBlocks.SwitchoverCandidate)))
				}
				if workload.KubeBlocks != nil {
					message := "KubeBlocks migration stops the selected Cluster component through componentSpecs[].stop; that component shares the downtime window and source PVCs remain retained"
					if workload.Controller.Kind == domain.KindInstanceSet {
						message = "KubeBlocks migration pauses InstanceSet reconciliation and stops only the selected Pod; sibling instances remain running while InstanceSet self-healing is suspended"
					}
					plan.AddCheck(warned("database-pause-scope", message))
				}
			}
		}
		pod, err := p.client.CoreV1().Pods(options.SourceNamespace).Get(ctx, options.PodName, metav1.GetOptions{})
		if err != nil {
			plan.AddCheck(failed("source-pod", err.Error()))
		} else {
			sourcePod = pod
			if options.SourceNode == "" {
				options.SourceNode = pod.Spec.NodeName
			}
			if options.SourceNode != pod.Spec.NodeName {
				plan.AddCheck(failed("source-node", fmt.Sprintf("Pod %s/%s runs on %s, requested source node is %s", pod.Namespace, pod.Name, pod.Spec.NodeName, options.SourceNode)))
			}
			pvcNames = podPVCNames(pod)
			if len(pvcNames) == 0 {
				plan.AddCheck(failed("source-pod", "Pod has no PVC volumes"))
			} else {
				plan.AddCheck(passed("source-pod", fmt.Sprintf("Pod references %d PVC(s)", len(pvcNames))))
			}
		}
	} else {
		workload = domain.WorkloadSpec{Adapter: domain.WorkloadNone}
		if len(pvcNames) == 0 {
			plan.AddCheck(failed("source-pvc", "at least one source PVC is required"))
		}
	}
	plan.Workload = workload

	var targetNode *corev1.Node
	if options.TargetNode != "" {
		node, err := p.client.CoreV1().Nodes().Get(ctx, options.TargetNode, metav1.GetOptions{})
		if err != nil {
			plan.AddCheck(failed("target-node", fmt.Sprintf("read target node: %v", err)))
		} else {
			targetNode = node
			if !nodeReady(node) || node.Spec.Unschedulable {
				plan.AddCheck(failed("target-node", fmt.Sprintf("node %s must be Ready and schedulable", node.Name)))
			} else {
				plan.AddCheck(passed("target-node", fmt.Sprintf("node %s is Ready and schedulable", node.Name)))
			}
			p.checkPodTargetScheduling(plan, sourcePod, workload, node)
		}
	}
	if len(options.DestinationPVCs) > 0 && len(options.DestinationPVCs) != len(pvcNames) {
		plan.AddCheck(failed("destination-pvc", fmt.Sprintf("%d destination PVC name(s) supplied for %d source PVC(s)", len(options.DestinationPVCs), len(pvcNames))))
	}

	volumeSpecs := make([]domain.VolumeSpec, 0, len(pvcNames))
	plannedVolumes := make([]domain.PlannedVolume, 0, len(pvcNames))
	copyConsumerNodes := map[string]struct{}{}
	unmanagedConsumerNames := map[string]struct{}{}
	var namespacePods []corev1.Pod
	var namespacePodsErr error
	podsLoaded := false
	storageClasses := make(map[string]*storagev1.StorageClass)
	storageClassErrors := make(map[string]error)
	sourcePVs := make(map[string]*corev1.PersistentVolume)
	mountTopologyConflict := ""
	totalStorage := resource.MustParse("0")
	storageByClass := map[string]resource.Quantity{}
	pvcsByClass := map[string]int{}
	for index, pvcName := range pvcNames {
		pvc, err := p.client.CoreV1().PersistentVolumeClaims(options.SourceNamespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			plan.AddCheck(failed("source-pvc", fmt.Sprintf("read PVC %s/%s: %v", options.SourceNamespace, pvcName, err)))
			continue
		}
		if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
			plan.AddCheck(failed("source-pvc", fmt.Sprintf("PVC %s/%s must be Bound", pvc.Namespace, pvc.Name)))
			continue
		}
		mode := corev1.PersistentVolumeFilesystem
		if pvc.Spec.VolumeMode != nil {
			mode = *pvc.Spec.VolumeMode
		}
		if mode != corev1.PersistentVolumeFilesystem {
			plan.AddCheck(failed("volume-mode", fmt.Sprintf("PVC %s/%s uses %s; the embedded pv-migrate engine supports Filesystem", pvc.Namespace, pvc.Name, mode)))
		}
		if !kube.HasWritableAccessMode(pvc.Spec.AccessModes) {
			plan.AddCheck(failed("access-mode", fmt.Sprintf("PVC %s/%s has no writable access mode for the destination copy", pvc.Namespace, pvc.Name)))
		}
		pv, err := p.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			plan.AddCheck(failed("source-pv", fmt.Sprintf("read PV %s: %v", pvc.Spec.VolumeName, err)))
			continue
		}
		p.checkSessionOwnership(ctx, plan, options.SessionNamespace, pvc, pv)
		sourcePVs[pv.Name] = pv
		capacity, ok := pv.Spec.Capacity[corev1.ResourceStorage]
		if !ok || capacity.Sign() <= 0 {
			plan.AddCheck(failed("capacity", fmt.Sprintf("PV %s has no positive storage capacity", pv.Name)))
			continue
		}
		totalStorage.Add(capacity)
		sourceClass := ""
		if pvc.Spec.StorageClassName != nil {
			sourceClass = *pvc.Spec.StorageClassName
		}
		destinationClass := options.DestinationClass
		if destinationClass == "" {
			destinationClass = sourceClass
		}
		if destinationClass == "" {
			plan.AddCheck(failed("storage-class", fmt.Sprintf("PVC %s/%s has no storageClassName and no destination class was supplied", pvc.Namespace, pvc.Name)))
			continue
		}
		sc, cached := storageClasses[destinationClass]
		var storageClassErr error
		if cached {
			storageClassErr = storageClassErrors[destinationClass]
		} else {
			sc, storageClassErr = p.client.StorageV1().StorageClasses().Get(ctx, destinationClass, metav1.GetOptions{})
			storageClasses[destinationClass] = sc
			storageClassErrors[destinationClass] = storageClassErr
		}
		if storageClassErr != nil {
			plan.AddCheck(failed("storage-class", fmt.Sprintf("read StorageClass %s: %v", destinationClass, storageClassErr)))
			continue
		}
		bindingMode := storagev1.VolumeBindingImmediate
		if sc.VolumeBindingMode != nil {
			bindingMode = *sc.VolumeBindingMode
		}
		classQuantity := storageByClass[destinationClass]
		classQuantity.Add(capacity)
		storageByClass[destinationClass] = classQuantity
		pvcsByClass[destinationClass]++
		destinationName := destinationPVCName(options, pvc.Name, index)
		if problems := validation.IsDNS1123Subdomain(destinationName); len(problems) > 0 {
			plan.AddCheck(failed("destination-pvc", fmt.Sprintf("generated PVC name %q is invalid: %s", destinationName, strings.Join(problems, "; "))))
			continue
		}
		destinationRef := domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolumeClaim, Namespace: options.StagingNamespace, Name: destinationName}
		accessModes := append([]corev1.PersistentVolumeAccessMode(nil), pvc.Spec.AccessModes...)
		plannedVolumes = append(plannedVolumes, domain.PlannedVolume{
			SourcePVC:      pvcReference(pvc),
			SourcePV:       pvReference(pv),
			DestinationPVC: destinationRef,
			Capacity:       capacity.String(),
			AccessModes:    accessModes,
			VolumeMode:     mode,
			StorageClass:   destinationClass,
			BindingMode:    bindingMode,
			CSIProvisioner: sc.Provisioner,
		})
		volumeSpecs = append(volumeSpecs, domain.VolumeSpec{
			SourcePVC:           pvcReference(pvc),
			SourcePV:            pvReference(pv),
			SourceReclaimPolicy: pv.Spec.PersistentVolumeReclaimPolicy,
			SourcePVCSpec:       *pvc.Spec.DeepCopy(),
			SourcePVCMetadata: domain.PVCMetadata{
				Labels:          maps.Clone(pvc.Labels),
				Annotations:     filteredPVCAnnotations(pvc.Annotations),
				OwnerReferences: append([]metav1.OwnerReference(nil), pvc.OwnerReferences...),
			},
			DestinationPVC: destinationRef,
			Capacity:       capacity.String(),
			StorageClass:   destinationClass,
			AccessModes:    accessModes,
			VolumeMode:     mode,
		})
		if !podsLoaded {
			pods, listErr := p.client.CoreV1().Pods(options.SourceNamespace).List(ctx, metav1.ListOptions{})
			if listErr != nil {
				namespacePodsErr = listErr
			} else {
				namespacePods = pods.Items
			}
			podsLoaded = true
		}
		consumers := p.checkPVCReferencesFromPods(plan, pvc, sourcePod, options.Operation, options.Online, namespacePods, namespacePodsErr)
		if options.Operation == domain.OperationMigrate && sourcePod == nil {
			for _, consumer := range consumers {
				unmanagedConsumerNames[consumer.Name] = struct{}{}
			}
		}
		if options.Operation == domain.OperationCopy && options.Online && sourcePod == nil {
			for _, consumer := range consumers {
				if consumer.Spec.NodeName != "" {
					copyConsumerNodes[consumer.Spec.NodeName] = struct{}{}
				}
			}
		}
		if workload.Adapter == domain.WorkloadStandalone {
			for _, owner := range pvc.OwnerReferences {
				if owner.Kind == domain.KindPod {
					plan.AddCheck(failed("pvc-ownership", fmt.Sprintf("standalone Pod migration cannot preserve Pod-owned PVC %s/%s", pvc.Namespace, pvc.Name)))
				}
			}
		}
	}
	if len(unmanagedConsumerNames) > 0 {
		names := slices.Collect(maps.Keys(unmanagedConsumerNames))
		sort.Strings(names)
		plan.AddCheck(failed("controller-adapter", fmt.Sprintf("PVC-only migration has active Pod consumer(s) %s; use --pod to select a workload that pvc-migrate can pause before final sync", strings.Join(names, ","))))
	}
	if options.Operation == domain.OperationCopy && options.Online && sourcePod == nil {
		options.SourceNode = inferOnlineCopySourceNode(plan, options.SourceNode, copyConsumerNodes)
	}
	capacityInventory := &storageCapacityInventory{mode: options.CapacityAwareness}
	if len(plannedVolumes) > 0 {
		capacityInventory = p.loadStorageCapacity(ctx, options.CapacityAwareness)
	}
	if autoTargetNodeRequested {
		targetNode = p.selectTargetNode(ctx, plan, workload, sourcePod, options.SourceNode, plannedVolumes, sourcePVs, storageClasses, capacityInventory)
		if targetNode != nil {
			options.TargetNode = targetNode.Name
			plan.TargetNode = targetNode.Name
			p.checkPodTargetScheduling(plan, sourcePod, workload, targetNode)
		}
	}
	if targetNode != nil {
		for _, volume := range plannedVolumes {
			pv := sourcePVs[volume.SourcePV.Name]
			if pv != nil && !kube.PVSupportsNode(pv, targetNode) && mountTopologyConflict == "" {
				mountTopologyConflict = fmt.Sprintf("source PV %s node affinity excludes target node %s", pv.Name, targetNode.Name)
			}
			sc := storageClasses[volume.StorageClass]
			if sc == nil {
				continue
			}
			if !matchesAllowedTopologies(sc, targetNode) {
				plan.AddCheck(failed("storage-topology", fmt.Sprintf("node %s does not satisfy StorageClass %s allowedTopologies", targetNode.Name, sc.Name)))
			} else {
				plan.AddCheck(passed("storage-topology", fmt.Sprintf("StorageClass %s bindingMode=%s topology is compatible", sc.Name, volume.BindingMode)))
			}
			p.checkCSINode(ctx, plan, sc, targetNode)
		}
	}
	if targetNode != nil && len(plannedVolumes) > 0 && validCapacityAwareness(options.CapacityAwareness) && options.CapacityAwareness != domain.CapacityAwarenessOff {
		p.checkStorageCapacity(plan, targetNode, plannedVolumes, capacityInventory, options.CapacityAwareness)
	}
	if options.SourceNode != "" {
		node, err := p.client.CoreV1().Nodes().Get(ctx, options.SourceNode, metav1.GetOptions{})
		switch {
		case err != nil:
			plan.AddCheck(failed("source-node", fmt.Sprintf("read source node: %v", err)))
		case !nodeReady(node) || node.Spec.Unschedulable:
			plan.AddCheck(failed("source-node", fmt.Sprintf("node %s must be Ready and schedulable for the source helper", node.Name)))
		default:
			plan.AddCheck(passed("source-node", fmt.Sprintf("node %s is Ready and schedulable", node.Name)))
		}
	}
	plan.Volumes = plannedVolumes
	plan.TemporaryUsage.StorageRequests = totalStorage.String()
	plan.TemporaryUsage.PVCs = len(plannedVolumes)
	plan.TemporaryUsage.Pods = max(1, 2*len(plannedVolumes))
	plan.TemporaryUsage.Jobs = max(1, len(plannedVolumes))
	plan.TemporaryUsage.Services = max(1, len(plannedVolumes))
	// The clusterip strategy creates one Secret for the source sshd release
	// and one for the destination rsync release per PVC.
	plan.TemporaryUsage.Secrets = max(1, 2*len(plannedVolumes))
	plan.TemporaryUsage.ConfigMaps = 1
	plan.TemporaryUsage.ServiceAccounts = max(1, 2*len(plannedVolumes))
	plan.RollbackRetention.StorageRequests = totalStorage.String()
	plan.RollbackRetention.PVCs = len(plannedVolumes)
	for class, quantity := range storageByClass {
		plan.TemporaryUsage.ByStorageClass[class] = quantity.String()
		plan.RollbackRetention.ByStorageClass[class] = quantity.String()
		plan.TemporaryUsage.PVCsByStorageClass[class] = pvcsByClass[class]
		plan.RollbackRetention.PVCsByStorageClass[class] = pvcsByClass[class]
	}
	if len(plannedVolumes) > 0 {
		p.checkLimitRanges(ctx, plan, options.StagingNamespace, plannedVolumes)
		p.checkQuotas(ctx, plan, options.StagingNamespace, plan.TemporaryUsage)
		if options.SourceNamespace != options.StagingNamespace {
			// clusterip puts the sshd Deployment, Service, Secret, and
			// ServiceAccount in the source namespace. Check those object
			// quotas independently from the destination PVC reservation.
			sourceHelpers := domain.ResourceEstimate{
				StorageRequests:    "0",
				Pods:               len(plannedVolumes),
				Services:           len(plannedVolumes),
				Secrets:            len(plannedVolumes),
				ServiceAccounts:    len(plannedVolumes),
				ByStorageClass:     map[string]string{},
				PVCsByStorageClass: map[string]int{},
			}
			p.checkLimitRanges(ctx, plan, options.SourceNamespace, nil)
			p.checkQuotas(ctx, plan, options.SourceNamespace, sourceHelpers)
		}
		if options.SessionNamespace != options.StagingNamespace && options.SessionNamespace != options.SourceNamespace {
			// Session persistence creates one ConfigMap and the mutating
			// workflow also uses one Lease object in this namespace.
			sessionResources := domain.ResourceEstimate{
				StorageRequests:    "0",
				ConfigMaps:         1,
				Leases:             1,
				ByStorageClass:     map[string]string{},
				PVCsByStorageClass: map[string]int{},
			}
			p.checkQuotas(ctx, plan, options.SessionNamespace, sessionResources)
		}
		p.checkNetworkPolicies(ctx, plan, options.SourceNamespace, options.StagingNamespace)
		p.checkRBAC(ctx, plan, options.SourceNamespace, options.StagingNamespace, options.SessionNamespace, workload)
		if sourcePod != nil {
			p.checkPodDependencies(ctx, plan, sourcePod)
			for _, issue := range podMigrationIssues(sourcePod.Spec, options.SourceNode, options.TargetNode) {
				plan.AddCheck(failed("pod-scheduling", issue))
			}
		}
	}
	options.Strategies = filterStrategies(plan, options, mountTopologyConflict)
	if len(options.Strategies) == 0 {
		plan.AddCheck(failed("strategy", "no selected pv-migrate strategy can handle the requested source and destination"))
	} else if autoStrategyRequested {
		plan.AddCheck(passed("strategy-selection", fmt.Sprintf("auto selected strategy order: %s", strings.Join(options.Strategies, ","))))
	}
	plan.SourceNode = options.SourceNode
	plan.Strategies = slices.Clone(options.Strategies)
	plan.SessionSpec = domain.NewSessionSpec(options.Operation, domain.SessionCommon{
		SourceNamespace:      options.SourceNamespace,
		TemporaryNamespace:   options.TemporaryNamespace,
		DestinationNamespace: options.DestinationNamespace,
		SessionNamespace:     options.SessionNamespace,
		Volumes:              volumeSpecs,
	}, workload, options.Online, domain.SessionWorkflowOptions{
		SourceNode:       options.SourceNode,
		TargetNode:       options.TargetNode,
		Strategies:       append([]string(nil), options.Strategies...),
		VerifyChecksum:   options.VerifyChecksum,
		DeleteExtraneous: options.DeleteExtraneous,
		ToolImage:        options.ToolImage,
	})
	if len(volumeSpecs) != len(pvcNames) {
		plan.Ready = false
	}
	return plan, nil
}

func applyDefaults(options Options) Options {
	if options.Operation == "" {
		options.Operation = domain.OperationMigrate
	}
	if options.SourceNamespace == "" {
		options.SourceNamespace = "default"
	}
	if options.DestinationNamespace == "" {
		options.DestinationNamespace = options.SourceNamespace
	}
	if options.TemporaryNamespace == "" {
		options.TemporaryNamespace = "pvc-migrate-system"
	}
	if options.SessionNamespace == "" {
		options.SessionNamespace = options.TemporaryNamespace
	}
	if options.StagingNamespace == "" {
		options.StagingNamespace = options.DestinationNamespace
	}
	if options.ToolImage == "" {
		options.ToolImage = kube.DefaultToolImageRepository + ":main"
	}
	if options.CapacityAwareness == "" {
		options.CapacityAwareness = domain.CapacityAwarenessAuto
	}
	if len(options.Strategies) == 0 || (len(options.Strategies) == 1 && containsStrategy(options.Strategies, domain.StrategyAuto)) {
		options.Strategies = autoStrategies(options.SourceNamespace, options.StagingNamespace)
	}
	return options
}

func isAutoNode(value string) bool {
	return value == "" || strings.EqualFold(value, domain.AutoValue)
}

func (p *Planner) checkPodTargetScheduling(plan *domain.MigrationPlan, sourcePod *corev1.Pod, workload domain.WorkloadSpec, node *corev1.Node) {
	if sourcePod == nil {
		return
	}
	schedulingSpec := sourcePod.Spec
	if workload.Adapter == domain.WorkloadStandalone {
		schedulingSpec = *sourcePod.Spec.DeepCopy()
		// The standalone adapter clears both direct and hostname placement from
		// the recreated Pod before applying the selected target node.
		schedulingSpec.NodeName = ""
		if hostname := schedulingSpec.NodeSelector[corev1.LabelHostname]; hostname != "" && hostname != node.Labels[corev1.LabelHostname] {
			delete(schedulingSpec.NodeSelector, corev1.LabelHostname)
			plan.AddCheck(warned("pod-scheduling", fmt.Sprintf("standalone Pod hostname selector %s will be replaced with target hostname %s", hostname, node.Labels[corev1.LabelHostname])))
		}
	}
	issues := schedulingIssues(schedulingSpec, node)
	if len(issues) > 0 {
		plan.AddCheck(failed("pod-scheduling", strings.Join(issues, "; ")))
	} else {
		plan.AddCheck(passed("pod-scheduling", "target node satisfies nodeSelector, required nodeAffinity, and taints"))
	}
	if workload.Adapter == domain.WorkloadStandalone {
		resourceIssues, known := resourceFitIssues(schedulingSpec, node)
		if len(resourceIssues) > 0 {
			plan.AddCheck(failed("pod-resources", strings.Join(resourceIssues, "; ")))
		} else if !known {
			plan.AddCheck(warned("pod-resources", fmt.Sprintf("target node %s does not publish all allocatable resources needed to verify standalone Pod placement", node.Name)))
		}
	}
}

type targetNodeCandidate struct {
	node            *corev1.Node
	distinctFromSrc bool
	taintPenalty    int
	sourcePVMatches int
	capacityKnown   int
	capacityUnknown int
	capacitySurplus resource.Quantity
	resourceUnknown int
}

func (p *Planner) selectTargetNode(ctx context.Context, plan *domain.MigrationPlan, workload domain.WorkloadSpec, sourcePod *corev1.Pod, sourceNode string, volumes []domain.PlannedVolume, sourcePVs map[string]*corev1.PersistentVolume, storageClasses map[string]*storagev1.StorageClass, capacityInventory *storageCapacityInventory) *corev1.Node {
	if len(volumes) == 0 {
		plan.AddCheck(failed("target-node", "target node auto-selection requires at least one valid source PVC"))
		return nil
	}
	nodes, err := p.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		plan.AddCheck(failed("target-node", fmt.Sprintf("list nodes for auto-selection: %v", err)))
		return nil
	}
	candidates := make([]targetNodeCandidate, 0, len(nodes.Items))
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if !nodeReady(node) || node.Spec.Unschedulable {
			continue
		}
		if sourcePod != nil && len(schedulingIssuesForTarget(sourcePod, workload, node)) > 0 {
			continue
		}
		resourceUnknown := 0
		if sourcePod != nil && workload.Adapter == domain.WorkloadStandalone {
			spec := *sourcePod.Spec.DeepCopy()
			spec.NodeName = ""
			resourceIssues, known := resourceFitIssues(spec, node)
			if len(resourceIssues) > 0 {
				continue
			}
			if !known {
				resourceUnknown = 1
			}
		}
		if sourcePod != nil && len(podMigrationIssues(sourcePod.Spec, sourceNode, node.Name)) > 0 {
			continue
		}
		compatible := true
		sourcePVMatches := 0
		for _, volume := range volumes {
			if sc := storageClasses[volume.StorageClass]; sc != nil && !matchesAllowedTopologies(sc, node) {
				compatible = false
				break
			}
			if pv := sourcePVs[volume.SourcePV.Name]; pv != nil && kube.PVSupportsNode(pv, node) {
				sourcePVMatches++
			}
		}
		if !compatible {
			continue
		}
		capacityKnown, capacityUnknown, capacitySurplus, capacityCompatible := capacityScore(capacityInventory, node, volumes)
		if !capacityCompatible {
			continue
		}
		candidates = append(candidates, targetNodeCandidate{node: node, distinctFromSrc: sourceNode == "" || node.Name != sourceNode, taintPenalty: hardTaintCount(node), sourcePVMatches: sourcePVMatches, capacityKnown: capacityKnown, capacityUnknown: capacityUnknown, capacitySurplus: capacitySurplus, resourceUnknown: resourceUnknown})
	}
	if len(candidates) == 0 {
		message := "no Ready and schedulable node satisfies Pod scheduling and destination StorageClass topology"
		if capacityInventory != nil && capacityInventory.loaded && len(capacityInventory.items) > 0 {
			message += " with sufficient CSI-reported capacity"
		}
		plan.AddCheck(failed("target-node", message))
		return nil
	}
	slices.SortStableFunc(candidates, compareTargetNodeCandidates)
	selected := candidates[0]
	reasons := []string{"topology-compatible Ready node"}
	if selected.capacityKnown > 0 {
		reasons = append(reasons, fmt.Sprintf("CSI capacity verified for %d StorageClass(es)", selected.capacityKnown))
	}
	if sourceNode != "" {
		switch {
		case selected.node.Name != sourceNode:
			reasons = append(reasons, fmt.Sprintf("distinct from source %s", sourceNode))
		case len(candidates) == 1:
			reasons = append(reasons, fmt.Sprintf("only compatible node; source node %s is retained", sourceNode))
		}
	}
	plan.AddCheck(passed("target-node-selection", fmt.Sprintf("auto selected target node %s (%s)", selected.node.Name, strings.Join(reasons, ", "))))
	return selected.node
}

func compareTargetNodeCandidates(a, b targetNodeCandidate) int {
	if a.capacityKnown != b.capacityKnown {
		if a.capacityKnown > b.capacityKnown {
			return -1
		}
		return 1
	}
	if a.capacityUnknown != b.capacityUnknown {
		if a.capacityUnknown < b.capacityUnknown {
			return -1
		}
		return 1
	}
	if a.resourceUnknown != b.resourceUnknown {
		if a.resourceUnknown < b.resourceUnknown {
			return -1
		}
		return 1
	}
	if comparison := a.capacitySurplus.Cmp(b.capacitySurplus); comparison != 0 {
		return -comparison
	}
	if a.distinctFromSrc != b.distinctFromSrc {
		if a.distinctFromSrc {
			return -1
		}
		return 1
	}
	if a.taintPenalty != b.taintPenalty {
		if a.taintPenalty < b.taintPenalty {
			return -1
		}
		return 1
	}
	if a.sourcePVMatches != b.sourcePVMatches {
		if a.sourcePVMatches > b.sourcePVMatches {
			return -1
		}
		return 1
	}
	return strings.Compare(a.node.Name, b.node.Name)
}

func hardTaintCount(node *corev1.Node) int {
	count := 0
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			count++
		}
	}
	return count
}

func schedulingIssuesForTarget(sourcePod *corev1.Pod, workload domain.WorkloadSpec, node *corev1.Node) []string {
	spec := sourcePod.Spec
	if workload.Adapter == domain.WorkloadStandalone {
		spec = *sourcePod.Spec.DeepCopy()
		spec.NodeName = ""
		if hostname := spec.NodeSelector[corev1.LabelHostname]; hostname != "" && hostname != node.Labels[corev1.LabelHostname] {
			delete(spec.NodeSelector, corev1.LabelHostname)
		}
	}
	issues := schedulingIssues(spec, node)
	if workload.Adapter == domain.WorkloadStandalone {
		resourceIssues, _ := resourceFitIssues(spec, node)
		issues = append(issues, resourceIssues...)
	}
	return issues
}

func containsStrategy(strategies []string, wanted string) bool {
	for _, strategy := range strategies {
		if strategy == wanted {
			return true
		}
	}
	return false
}

func autoStrategies(sourceNamespace, destinationNamespace string) []string {
	if sourceNamespace == destinationNamespace {
		return []string{domain.StrategyMount, domain.StrategyClusterIP}
	}
	return []string{domain.StrategyClusterIP, domain.StrategyLocal}
}

// filterStrategies removes constraints that are deterministically known from
// the Kubernetes inventory before a helper resource is created. A fallback
// strategy remains valid when one earlier strategy cannot handle the topology.
func filterStrategies(plan *domain.MigrationPlan, options Options, mountTopologyConflict string) []string {
	filtered := make([]string, 0, len(options.Strategies))
	for _, strategy := range options.Strategies {
		if strategy == domain.StrategyMount {
			switch {
			case options.SourceNamespace != options.StagingNamespace:
				plan.AddCheck(warned("strategy", "mount skipped: source and destination PVCs are in different namespaces; use clusterip or local"))
				continue
			case mountTopologyConflict != "":
				plan.AddCheck(warned("strategy", "mount skipped: "+mountTopologyConflict+"; use clusterip, nodeport, loadbalancer, or local"))
				continue
			}
		}
		filtered = append(filtered, strategy)
	}
	return filtered
}

func supportedStrategy(strategy string) bool {
	switch strategy {
	case domain.StrategyMount, domain.StrategyClusterIP, domain.StrategyLoadBalancer, domain.StrategyNodePort, domain.StrategyLocal:
		return true
	default:
		return false
	}
}

func destinationPVCName(options Options, source string, index int) string {
	if index < len(options.DestinationPVCs) && options.DestinationPVCs[index] != "" {
		return options.DestinationPVCs[index]
	}
	if options.Operation == domain.OperationCopy && options.SourceNamespace != options.StagingNamespace {
		return source
	}
	suffix := options.SessionID
	if len(suffix) > 12 {
		suffix = suffix[:12]
	}
	suffix = strings.TrimRight(suffix, "-.")
	if suffix == "" {
		suffix = "session"
	}
	maxSource := 253 - len(suffix) - len("-migrated-")
	if len(source) > maxSource {
		source = strings.TrimRight(source[:maxSource], "-.")
	}
	return source + "-migrated-" + suffix
}

func passed(name, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityInfo, Passed: true, Message: message}
}

func warned(name, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityWarning, Passed: true, Message: message}
}

func failed(name, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityError, Passed: false, Message: message}
}

func pvcReference(pvc *corev1.PersistentVolumeClaim) domain.ObjectReference {
	return domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolumeClaim, Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID, ResourceVersion: pvc.ResourceVersion}
}

func pvReference(pv *corev1.PersistentVolume) domain.ObjectReference {
	return domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolume, Name: pv.Name, UID: pv.UID, ResourceVersion: pv.ResourceVersion}
}

func nodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func matchesAllowedTopologies(sc *storagev1.StorageClass, node *corev1.Node) bool {
	if len(sc.AllowedTopologies) == 0 {
		return true
	}
	for _, term := range sc.AllowedTopologies {
		matches := true
		for _, expression := range term.MatchLabelExpressions {
			actual, exists := node.Labels[expression.Key]
			if !exists || !contains(expression.Values, actual) {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func (p *Planner) checkCSINode(ctx context.Context, plan *domain.MigrationPlan, sc *storagev1.StorageClass, node *corev1.Node) {
	csiNode, err := p.client.StorageV1().CSINodes().Get(ctx, node.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		plan.AddCheck(warned("csi-node", fmt.Sprintf("node %s has no CSINode object; provisioner %s must validate node support during reservation", node.Name, sc.Provisioner)))
		return
	}
	if err != nil {
		plan.AddCheck(failed("csi-node", fmt.Sprintf("read CSINode %s: %v", node.Name, err)))
		return
	}
	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == sc.Provisioner {
			plan.AddCheck(passed("csi-node", fmt.Sprintf("CSI driver %s is registered on %s", sc.Provisioner, node.Name)))
			return
		}
	}
	plan.AddCheck(warned("csi-node", fmt.Sprintf("provisioner %s is absent from CSINode %s; an in-tree or external provisioner may still support it", sc.Provisioner, node.Name)))
}

func (p *Planner) checkPVCReferences(ctx context.Context, plan *domain.MigrationPlan, pvc *corev1.PersistentVolumeClaim, sourcePod *corev1.Pod, operation domain.Operation, online bool) []*corev1.Pod {
	pods, err := p.client.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return p.checkPVCReferencesFromPods(plan, pvc, sourcePod, operation, online, nil, err)
	}
	return p.checkPVCReferencesFromPods(plan, pvc, sourcePod, operation, online, pods.Items, nil)
}

func (p *Planner) checkPVCReferencesFromPods(plan *domain.MigrationPlan, pvc *corev1.PersistentVolumeClaim, sourcePod *corev1.Pod, operation domain.Operation, online bool, pods []corev1.Pod, listErr error) []*corev1.Pod {
	if listErr != nil {
		plan.AddCheck(failed("pvc-consumers", fmt.Sprintf("list Pods for PVC %s/%s: %v", pvc.Namespace, pvc.Name, listErr)))
		return nil
	}
	consumers := make([]*corev1.Pod, 0)
	for i := range pods {
		pod := &pods[i]
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed && kube.PodUsesPVC(pod, pvc.Name) {
			consumers = append(consumers, pod)
		}
	}
	consumerNames := make([]string, 0, len(consumers))
	for _, consumer := range consumers {
		consumerNames = append(consumerNames, consumer.Name)
	}
	sort.Strings(consumerNames)
	if operation == domain.OperationCopy && !online && len(consumers) > 0 {
		plan.AddCheck(failed("pvc-consumers", fmt.Sprintf("offline copy requires PVC %s/%s to have zero active Pod consumers; found %s; use --online for a finite warm copy", pvc.Namespace, pvc.Name, strings.Join(consumerNames, ","))))
		return consumers
	}
	if sourcePod != nil {
		others := make([]string, 0)
		for _, consumer := range consumers {
			if consumer.Name != sourcePod.Name {
				others = append(others, consumer.Name)
			}
		}
		sort.Strings(others)
		switch {
		case len(others) > 0:
			plan.AddCheck(failed("pvc-consumers", fmt.Sprintf("PVC %s/%s is shared with Pod(s): %s", pvc.Namespace, pvc.Name, strings.Join(others, ","))))
		case operation == domain.OperationCopy && online && kube.HasAccessMode(pvc.Spec.AccessModes, corev1.ReadWriteOncePod):
			plan.AddCheck(failed("pvc-consumers", fmt.Sprintf("active RWOP PVC %s/%s cannot be warm-copied", pvc.Namespace, pvc.Name)))
		case operation == domain.OperationCopy && online:
			plan.AddCheck(warned("pvc-consumers", fmt.Sprintf("PVC %s/%s is active on selected Pod %s; online copy has file-level consistency", pvc.Namespace, pvc.Name, sourcePod.Name)))
		default:
			plan.AddCheck(passed("pvc-consumers", fmt.Sprintf("PVC %s/%s belongs to the selected migration unit", pvc.Namespace, pvc.Name)))
		}
		return consumers
	}
	if len(consumers) == 0 {
		plan.AddCheck(passed("pvc-consumers", fmt.Sprintf("PVC %s/%s is offline", pvc.Namespace, pvc.Name)))
		return consumers
	}
	if operation == domain.OperationMigrate {
		return consumers
	}
	if kube.HasAccessMode(pvc.Spec.AccessModes, corev1.ReadWriteOncePod) {
		plan.AddCheck(failed("pvc-consumers", fmt.Sprintf("active RWOP PVC %s/%s cannot be warm-copied", pvc.Namespace, pvc.Name)))
		return consumers
	}
	plan.AddCheck(warned("pvc-consumers", fmt.Sprintf("PVC %s/%s is active on Pod(s) %s; warm copy has file-level consistency until final sync", pvc.Namespace, pvc.Name, strings.Join(consumerNames, ","))))
	return consumers
}

func inferOnlineCopySourceNode(plan *domain.MigrationPlan, requested string, consumerNodes map[string]struct{}) string {
	nodes := make([]string, 0, len(consumerNodes))
	for node := range consumerNodes {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	if len(nodes) == 0 {
		return requested
	}
	if len(nodes) > 1 {
		plan.AddCheck(failed("source-node", fmt.Sprintf("online copy consumers run on multiple nodes (%s); copy each node group in a separate session", strings.Join(nodes, ","))))
		return requested
	}
	if requested != "" && requested != nodes[0] {
		plan.AddCheck(failed("source-node", fmt.Sprintf("online copy consumer runs on %s, requested source node is %s", nodes[0], requested)))
		return requested
	}
	if requested == "" {
		plan.AddCheck(passed("source-node-inference", fmt.Sprintf("inferred source helper node %s from active PVC consumers", nodes[0])))
		return nodes[0]
	}
	return requested
}

func podPVCNames(pod *corev1.Pod) []string {
	values := make([]string, 0)
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			values = append(values, volume.PersistentVolumeClaim.ClaimName)
		}
	}
	return uniqueSorted(values)
}

func uniqueSorted(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func filteredPVCAnnotations(input map[string]string) map[string]string {
	result := map[string]string{}
	for key, value := range input {
		switch key {
		case "pv.kubernetes.io/bind-completed",
			"pv.kubernetes.io/bound-by-controller",
			"volume.kubernetes.io/selected-node",
			"volume.kubernetes.io/storage-provisioner",
			"volume.beta.kubernetes.io/storage-provisioner",
			"kubectl.kubernetes.io/last-applied-configuration",
			kube.SessionKey:
			continue
		default:
			result[key] = value
		}
	}
	return result
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func isRiskRole(role string) bool {
	switch strings.ToLower(role) {
	case "leader", "primary", "master":
		return true
	default:
		return false
	}
}
