package planner

import (
	"context"
	"fmt"
	"maps"
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
	options = applyDefaults(options)
	plan := &domain.MigrationPlan{
		APIVersion:           domain.SessionAPIVersion,
		Kind:                 "MigrationPlan",
		SessionID:            options.SessionID,
		SourceNamespace:      options.SourceNamespace,
		TemporaryNamespace:   options.TemporaryNamespace,
		DestinationNamespace: options.DestinationNamespace,
		SessionNamespace:     options.SessionNamespace,
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
	if problems := validation.IsDNS1123Label(options.SessionID); len(problems) > 0 {
		plan.AddCheck(failed("session-id", strings.Join(problems, "; ")))
	}
	for _, strategy := range options.Strategies {
		if !supportedStrategy(strategy) {
			plan.AddCheck(failed("strategy", fmt.Sprintf("unsupported pv-migrate strategy %q", strategy)))
		}
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
					plan.AddCheck(warned("database-pause-scope", "KubeBlocks migration stops every Cluster component through componentSpecs[].stop; all components share the downtime window and source PVCs remain retained"))
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

	if options.TargetNode == "" {
		plan.AddCheck(failed("target-node", "target node is required for deterministic provisioning and topology checks"))
	}
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
			if sourcePod != nil {
				issues := schedulingIssues(sourcePod.Spec, node)
				if len(issues) > 0 {
					plan.AddCheck(failed("pod-scheduling", strings.Join(issues, "; ")))
				} else {
					plan.AddCheck(passed("pod-scheduling", "target node satisfies nodeSelector, required nodeAffinity, and taints"))
				}
			}
		}
	}
	if len(options.DestinationPVCs) > 0 && len(options.DestinationPVCs) != len(pvcNames) {
		plan.AddCheck(failed("destination-pvc", fmt.Sprintf("%d destination PVC name(s) supplied for %d source PVC(s)", len(options.DestinationPVCs), len(pvcNames))))
	}

	volumeSpecs := make([]domain.VolumeSpec, 0, len(pvcNames))
	plannedVolumes := make([]domain.PlannedVolume, 0, len(pvcNames))
	copyConsumerNodes := map[string]struct{}{}
	var namespacePods []corev1.Pod
	var namespacePodsErr error
	podsLoaded := false
	storageClasses := make(map[string]*storagev1.StorageClass)
	storageClassErrors := make(map[string]error)
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
		if targetNode != nil && !matchesAllowedTopologies(sc, targetNode) {
			plan.AddCheck(failed("storage-topology", fmt.Sprintf("node %s does not satisfy StorageClass %s allowedTopologies", targetNode.Name, sc.Name)))
		} else {
			plan.AddCheck(passed("storage-topology", fmt.Sprintf("StorageClass %s bindingMode=%s topology is compatible", sc.Name, bindingMode)))
		}
		if targetNode != nil {
			p.checkCSINode(ctx, plan, sc, targetNode)
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
		destinationRef := domain.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: options.StagingNamespace, Name: destinationName}
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
		if options.Operation == domain.OperationCopy && options.Online && sourcePod == nil {
			for _, consumer := range consumers {
				if consumer.Spec.NodeName != "" {
					copyConsumerNodes[consumer.Spec.NodeName] = struct{}{}
				}
			}
		}
		if workload.Adapter == domain.WorkloadStandalone {
			for _, owner := range pvc.OwnerReferences {
				if owner.Kind == "Pod" {
					plan.AddCheck(failed("pvc-ownership", fmt.Sprintf("standalone Pod migration cannot preserve Pod-owned PVC %s/%s", pvc.Namespace, pvc.Name)))
				}
			}
		}
	}
	if options.Operation == domain.OperationCopy && options.Online && sourcePod == nil {
		options.SourceNode = inferOnlineCopySourceNode(plan, options.SourceNode, copyConsumerNodes)
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
	if len(options.Strategies) == 0 {
		options.Strategies = []string{"mount", "clusterip"}
	}
	return options
}

func supportedStrategy(strategy string) bool {
	switch strategy {
	case "mount", "clusterip", "loadbalancer", "nodeport", "local":
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
	return domain.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID, ResourceVersion: pvc.ResourceVersion}
}

func pvReference(pv *corev1.PersistentVolume) domain.ObjectReference {
	return domain.ObjectReference{APIVersion: "v1", Kind: "PersistentVolume", Name: pv.Name, UID: pv.UID, ResourceVersion: pv.ResourceVersion}
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
			kube.SessionAnnotation:
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
