package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	kubeBlocksClusterAPIVersion = domain.KubeBlocksAppsGroup + "/v1alpha1"
	kubeBlocksOpsAPIVersion     = "operations.kubeblocks.io/v1alpha1"
	kubeBlocksGroupSuffix       = "kubeblocks.io"
	vmClusterAPIVersion         = "operator.victoriametrics.com/v1beta1"
	grafanaAPIVersion           = "grafana.integreatly.org/v1beta1"
	grafanaAPIGroup             = "grafana.integreatly.org"
	clusterResource             = "clusters"
	instanceSetResource         = "instancesets"
	vmClusterResource           = "vmclusters"
	grafanaResource             = "grafanas"
	pauseSessionAnnotation      = kube.PauseSessionAnnotation
	kubeBlocksComponentLabel    = "apps.kubeblocks.io/component-name"
	kubeBlocksRoleLabel         = "kubeblocks.io/role"
	kubeBlocksAppsRoleLabel     = "apps.kubeblocks.io/role"
	genericRoleLabel            = "role"
)

type DiscoverOptions struct {
	Namespace           string
	PodName             string
	SwitchoverCandidate string
	AllowLeaderDowntime bool
}

type kubeBlocksInstanceSetState struct {
	Paused           bool
	PausedConfigured bool
	UID              types.UID
	Role             string
	LeaderRoles      map[string]bool
	HasLeaderRole    bool
}

type Manager struct {
	typed     kubernetes.Interface
	dynamic   dynamic.Interface
	discovery discovery.DiscoveryInterface
	poll      time.Duration
}

func NewManager(typed kubernetes.Interface, dynamicClient dynamic.Interface, discoveryClient discovery.DiscoveryInterface) *Manager {
	return &Manager{typed: typed, dynamic: dynamicClient, discovery: discoveryClient, poll: time.Second}
}

func (m *Manager) Discover(ctx context.Context, options DiscoverOptions) (domain.WorkloadSpec, error) {
	pod, err := m.typed.CoreV1().Pods(options.Namespace).Get(ctx, options.PodName, metav1.GetOptions{})
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorKubernetes, "discover workload", fmt.Sprintf("read Pod %s/%s", options.Namespace, options.PodName), err)
	}
	return m.DiscoverPod(ctx, pod, options)
}

// DiscoverPod resolves a workload from a caller-owned Pod snapshot.
func (m *Manager) DiscoverPod(ctx context.Context, pod *corev1.Pod, options DiscoverOptions) (domain.WorkloadSpec, error) {
	if pod == nil {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorValidation, "discover workload", "Pod is nil")
	}
	if options.Namespace == "" {
		options.Namespace = pod.Namespace
	}
	if options.PodName == "" {
		options.PodName = pod.Name
	}
	if pod.Namespace != options.Namespace || pod.Name != options.PodName {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorConflict, "discover workload", fmt.Sprintf("Pod snapshot %s/%s does not match requested %s/%s", pod.Namespace, pod.Name, options.Namespace, options.PodName))
	}
	if pod.Annotations[corev1.MirrorPodAnnotationKey] != "" {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover workload", "static mirror Pods are unsupported")
	}
	owner := controllerOwner(pod.OwnerReferences)
	if owner == nil {
		if err := requireReadyPod(pod, options.Namespace, options.PodName); err != nil {
			return domain.WorkloadSpec{}, err
		}
		return standaloneWorkload(pod)
	}
	groupVersion, parseErr := schema.ParseGroupVersion(owner.APIVersion)
	if parseErr != nil {
		return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorPrecondition, "discover workload", "parse controller apiVersion", parseErr)
	}
	var sts *appsv1.StatefulSet
	var err error
	if owner.Kind == domain.KindStatefulSet && groupVersion.Group == appsv1.GroupName {
		sts, err = m.typed.AppsV1().StatefulSets(options.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorKubernetes, "discover workload", "read StatefulSet", err)
		}
		if reason := unsupportedStatefulSetReason(sts); reason != "" {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover workload", reason)
		}
		if isVictoriaLogsStatefulSet(sts) {
			return m.victoriaLogsWorkload(ctx, pod, sts)
		}
	}
	if owner.Kind == domain.KindJob && groupVersion.Group == batchv1.GroupName {
		job, getErr := m.typed.BatchV1().Jobs(options.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorKubernetes, "discover workload", "read Job", getErr)
		}
		if parent := controllerOwner(job.OwnerReferences); parent != nil && parent.Kind == domain.KindBackup {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover workload", fmt.Sprintf("Backup-owned archive-WAL Job %s/%s is a backup workload and cannot be migrated", options.Namespace, job.Name))
		}
	}
	if err := requireReadyPod(pod, options.Namespace, options.PodName); err != nil {
		return domain.WorkloadSpec{}, err
	}
	if owner.Kind == domain.KindStatefulSet && groupVersion.Group == appsv1.GroupName {
		parent := controllerOwner(sts.OwnerReferences)
		if parent != nil {
			parentGV, parentErr := schema.ParseGroupVersion(parent.APIVersion)
			if parentErr != nil {
				return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorPrecondition, "discover workload", "parse parent controller apiVersion", parentErr)
			}
			switch parent.Kind {
			case domain.KindVMCluster:
				if parentGV.Group == "operator.victoriametrics.com" {
					return m.vmClusterWorkload(ctx, pod, owner, parent, sts, options)
				}
			case domain.KindComponent:
				if strings.Contains(parentGV.Group, "kubeblocks.io") {
					return m.kubeBlocksWorkload(ctx, pod, owner, options)
				}
			}
			if strings.Contains(parentGV.Group, "kubeblocks.io") {
				return m.kubeBlocksWorkload(ctx, pod, owner, options)
			}
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover workload", fmt.Sprintf("StatefulSet is generated by unsupported controller %s/%s", parent.APIVersion, parent.Kind))
		}
		return m.statefulSetWorkload(ctx, pod, sts, options)
	}
	if owner.Kind == domain.KindReplicaSet && groupVersion.Group == appsv1.GroupName {
		rs, getErr := m.typed.AppsV1().ReplicaSets(options.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorKubernetes, "discover workload", "read ReplicaSet", getErr)
		}
		deployment := controllerOwner(rs.OwnerReferences)
		if deployment == nil || deployment.Kind != domain.KindDeployment {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover workload", "ReplicaSet has no Deployment controller")
		}
		deploymentObject, getErr := m.typed.AppsV1().Deployments(options.Namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorKubernetes, "discover workload", "read Deployment", getErr)
		}
		grafanaOwner := controllerOwner(deploymentObject.OwnerReferences)
		if grafanaOwner != nil {
			grafanaGV, _ := schema.ParseGroupVersion(grafanaOwner.APIVersion)
			if grafanaOwner.Kind == domain.KindGrafana && grafanaGV.Group == grafanaAPIGroup {
				return m.grafanaWorkload(ctx, pod, deploymentObject, grafanaOwner)
			}
		}
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover workload", fmt.Sprintf("Deployment %s/%s has no safe pause adapter", options.Namespace, deployment.Name))
	}
	if owner.Kind == domain.KindInstanceSet && strings.Contains(groupVersion.Group, kubeBlocksGroupSuffix) {
		return m.kubeBlocksWorkload(ctx, pod, owner, options)
	}
	return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover workload", fmt.Sprintf("controller %s/%s has no safe pause adapter", owner.APIVersion, owner.Kind))
}

func (m *Manager) Pause(ctx context.Context, session *domain.Session) error {
	switch session.Spec.Workload().Adapter {
	case domain.WorkloadNone:
		return nil
	case domain.WorkloadStandalone:
		return m.pauseStandalone(ctx, session)
	case domain.WorkloadStatefulSet:
		return m.pauseStatefulSet(ctx, session)
	case domain.WorkloadVictoriaLogs:
		return m.pauseVictoriaLogs(ctx, session)
	case domain.WorkloadKubeBlocks:
		return m.pauseKubeBlocks(ctx, session)
	case domain.WorkloadVMCluster:
		return m.pauseVMCluster(ctx, session)
	case domain.WorkloadGrafana:
		return m.pauseGrafana(ctx, session)
	default:
		return domain.NewError(domain.ErrorPrecondition, "pause workload", fmt.Sprintf("adapter %q is unsupported", session.Spec.Workload().Adapter))
	}
}

func (m *Manager) Resume(ctx context.Context, session *domain.Session) error {
	switch session.Spec.Workload().Adapter {
	case domain.WorkloadNone:
		return nil
	case domain.WorkloadStandalone:
		return m.resumeStandalone(ctx, session)
	case domain.WorkloadStatefulSet:
		return m.resumeStatefulSet(ctx, session)
	case domain.WorkloadVictoriaLogs:
		return m.resumeVictoriaLogs(ctx, session)
	case domain.WorkloadKubeBlocks:
		return m.resumeKubeBlocks(ctx, session)
	case domain.WorkloadVMCluster:
		return m.resumeVMCluster(ctx, session)
	case domain.WorkloadGrafana:
		return m.resumeGrafana(ctx, session)
	default:
		return domain.NewError(domain.ErrorPrecondition, "resume workload", fmt.Sprintf("adapter %q is unsupported", session.Spec.Workload().Adapter))
	}
}

func (m *Manager) VerifyPaused(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Adapter == domain.WorkloadNone {
		return nil
	}
	if err := m.verifyPauseControl(ctx, session); err != nil {
		return err
	}
	references := workload.AffectedPods
	if len(references) == 0 {
		references = []domain.ObjectReference{workload.Pod}
	}
	seen := make(map[string]struct{}, len(references))
	uniqueReferences := make([]domain.ObjectReference, 0, len(references))
	for _, reference := range references {
		key := reference.Namespace + "/" + reference.Name
		if _, ok := seen[key]; ok || reference.Name == "" {
			continue
		}
		seen[key] = struct{}{}
		uniqueReferences = append(uniqueReferences, reference)
	}
	_, errors := m.readPodReferences(ctx, uniqueReferences)
	for index, reference := range uniqueReferences {
		err := errors[index]
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read workload Pod", err)
		}
		return domain.NewError(domain.ErrorPrecondition, "verify paused", fmt.Sprintf("Pod %s/%s is still present", reference.Namespace, reference.Name))
	}
	return nil
}

func (m *Manager) readPodReferences(ctx context.Context, references []domain.ObjectReference) ([]*corev1.Pod, []error) {
	pods := make([]*corev1.Pod, len(references))
	errors := make([]error, len(references))
	parallel.For(len(references), func(index int) {
		reference := references[index]
		pods[index], errors[index] = m.typed.CoreV1().Pods(reference.Namespace).Get(ctx, reference.Name, metav1.GetOptions{})
		if errors[index] == nil && (pods[index] == nil || pods[index].Name == "") {
			errors[index] = domain.NewError(domain.ErrorKubernetes, "read Pod", fmt.Sprintf("Pod %s/%s returned an empty object", reference.Namespace, reference.Name))
		}
	})
	return pods, errors
}

func (m *Manager) readPods(ctx context.Context, namespace string, names []string) ([]*corev1.Pod, []error) {
	references := make([]domain.ObjectReference, len(names))
	for index, name := range names {
		references[index] = domain.ObjectReference{Namespace: namespace, Name: name}
	}
	return m.readPodReferences(ctx, references)
}

func (m *Manager) verifyPauseControl(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if m.dynamic == nil {
		return nil
	}
	switch workload.Adapter {
	case domain.WorkloadVictoriaLogs:
		if workload.Controller.Kind != domain.KindStatefulSet {
			return domain.NewError(domain.ErrorInternal, "verify paused", "Victoria Logs session lacks StatefulSet controller state")
		}
		sts, err := m.typed.AppsV1().StatefulSets(workload.Controller.Namespace).Get(ctx, workload.Controller.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read Victoria Logs StatefulSet", err)
		}
		if workload.Controller.UID != "" && sts.UID != workload.Controller.UID {
			return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("Victoria Logs StatefulSet %s/%s UID changed", sts.Namespace, sts.Name))
		}
		if sts.Annotations[pauseSessionAnnotation] != session.ID {
			return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("Victoria Logs StatefulSet %s/%s pause ownership changed", sts.Namespace, sts.Name))
		}
		if replicas := statefulSetReplicas(sts); replicas != 0 {
			return domain.NewError(domain.ErrorPrecondition, "verify paused", fmt.Sprintf("Victoria Logs StatefulSet %s/%s replicas=%d", sts.Namespace, sts.Name, replicas))
		}
	case domain.WorkloadVMCluster:
		vm := workload.VMCluster
		if vm == nil {
			return domain.NewError(domain.ErrorInternal, "verify paused", "session lacks VMCluster state")
		}
		gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
		if err != nil {
			return err
		}
		object, err := m.dynamic.Resource(gvr).Namespace(workload.Pod.Namespace).Get(ctx, vm.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read VMCluster", err)
		}
		if vm.UID != "" && object.GetUID() != vm.UID {
			return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()))
		}
		if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
			return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("VMCluster %s/%s pause ownership changed", object.GetNamespace(), object.GetName()))
		}
		component, _, nestedErr := unstructured.NestedMap(object.Object, "spec", vm.Component)
		if nestedErr != nil {
			return nestedErr
		}
		paused, _, _ := unstructured.NestedBool(component, "paused")
		if !paused {
			return domain.NewError(domain.ErrorPrecondition, "verify paused", fmt.Sprintf("VMCluster component %s is not paused", vm.Component))
		}
	case domain.WorkloadGrafana:
		grafana := workload.Grafana
		if grafana == nil {
			return domain.NewError(domain.ErrorInternal, "verify paused", "session lacks Grafana state")
		}
		gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
		if err != nil {
			return err
		}
		object, err := m.dynamic.Resource(gvr).Namespace(workload.Pod.Namespace).Get(ctx, grafana.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read Grafana", err)
		}
		if grafana.UID != "" && object.GetUID() != grafana.UID {
			return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()))
		}
		if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
			return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("Grafana %s/%s suspend ownership changed", object.GetNamespace(), object.GetName()))
		}
		suspended, _, _ := unstructured.NestedBool(object.Object, "spec", "suspend")
		if !suspended {
			return domain.NewError(domain.ErrorPrecondition, "verify paused", "Grafana reconciliation is not suspended")
		}
	case domain.WorkloadKubeBlocks:
		kb := workload.KubeBlocks
		if kb == nil {
			return domain.NewError(domain.ErrorInternal, "verify paused", "session lacks KubeBlocks state")
		}
		if workload.Controller.Kind == domain.KindInstanceSet {
			return m.verifyKubeBlocksInstanceSetPaused(ctx, session)
		}
		apiVersion := kb.ClusterAPIVersion
		if apiVersion == "" {
			apiVersion = kubeBlocksClusterAPIVersion
		}
		gvr, err := kube.ParseGroupVersionResource(apiVersion, clusterResource)
		if err != nil {
			return err
		}
		object, err := m.dynamic.Resource(gvr).Namespace(workload.Pod.Namespace).Get(ctx, kb.Cluster, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read KubeBlocks Cluster", err)
		}
		if kb.ClusterUID != "" && object.GetUID() != kb.ClusterUID {
			return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("KubeBlocks Cluster %s/%s UID changed", object.GetNamespace(), object.GetName()))
		}
		if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
			return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("KubeBlocks Cluster %s/%s pause ownership changed", object.GetNamespace(), object.GetName()))
		}
		components, ok, nestedErr := unstructured.NestedSlice(object.Object, "spec", "componentSpecs")
		if nestedErr != nil || !ok {
			return domain.NewError(domain.ErrorPrecondition, "verify paused", "KubeBlocks Cluster has no componentSpecs")
		}
		componentFound := false
		for index := range components {
			component, ok := components[index].(map[string]any)
			if !ok {
				return domain.NewError(domain.ErrorPrecondition, "verify paused", fmt.Sprintf("KubeBlocks componentSpecs[%d] is malformed", index))
			}
			name, _, _ := unstructured.NestedString(component, "name")
			if name != kb.Component {
				continue
			}
			componentFound = true
			stopped, _, _ := unstructured.NestedBool(component, "stop")
			if !stopped {
				return domain.NewError(domain.ErrorPrecondition, "verify paused", fmt.Sprintf("KubeBlocks component %s is not stopped", name))
			}
			break
		}
		if !componentFound {
			return domain.NewError(domain.ErrorPrecondition, "verify paused", fmt.Sprintf("KubeBlocks Cluster has no component %s", kb.Component))
		}
	}
	return nil
}

func standaloneWorkload(pod *corev1.Pod) (domain.WorkloadSpec, error) {
	raw, err := json.Marshal(pod)
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorInternal, "discover standalone Pod", "encode Pod", err)
	}
	return domain.WorkloadSpec{
		Adapter:        domain.WorkloadStandalone,
		Pod:            podReference(pod),
		OriginalObject: raw,
	}, nil
}

func (m *Manager) statefulSetWorkload(ctx context.Context, pod *corev1.Pod, sts *appsv1.StatefulSet, options DiscoverOptions) (domain.WorkloadSpec, error) {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	ordinal, err := podOrdinal(pod, sts.Name)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}
	if ordinal >= replicas {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover StatefulSet", fmt.Sprintf("Pod ordinal %d is outside replicas %d", ordinal, replicas))
	}
	if policy := sts.Spec.PersistentVolumeClaimRetentionPolicy; policy != nil && policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover StatefulSet", fmt.Sprintf("PVC retention whenScaled is %s", policy.WhenScaled))
	}
	affected := make([]domain.ObjectReference, 0, replicas-ordinal)
	names := make([]string, 0, replicas-ordinal)
	for current := ordinal; current < replicas; current++ {
		names = append(names, fmt.Sprintf("%s-%d", sts.Name, current))
	}
	candidates, getErrors := m.readPods(ctx, pod.Namespace, names)
	for index, name := range names {
		candidate, getErr := candidates[index], getErrors[index]
		if getErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorPrecondition, "discover StatefulSet", fmt.Sprintf("affected Pod %s/%s is unavailable", pod.Namespace, name), getErr)
		}
		if candidate.Status.Phase != corev1.PodRunning || !podReady(candidate) {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover StatefulSet", fmt.Sprintf("affected Pod %s/%s must be Running and Ready", pod.Namespace, name))
		}
		if isLeaderRole(podRole(candidate)) && !options.AllowLeaderDowntime {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover StatefulSet", fmt.Sprintf("scale-down affects %s with role %s; complete an application switchover and pass --allow-leader-downtime", name, podRole(candidate)))
		}
		affected = append(affected, podReference(candidate))
	}
	return domain.WorkloadSpec{
		Adapter:          domain.WorkloadStatefulSet,
		Pod:              podReference(pod),
		Controller:       objectReference(domain.AppsAPIVersion, domain.KindStatefulSet, sts.Namespace, sts.Name, sts.UID, sts.ResourceVersion),
		OriginalReplicas: &replicas,
		Ordinal:          &ordinal,
		AffectedPods:     affected,
	}, nil
}

func (m *Manager) victoriaLogsWorkload(ctx context.Context, pod *corev1.Pod, sts *appsv1.StatefulSet) (domain.WorkloadSpec, error) {
	replicas := statefulSetReplicas(sts)
	if policy := sts.Spec.PersistentVolumeClaimRetentionPolicy; policy != nil && policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover Victoria Logs", fmt.Sprintf("PVC retention whenScaled is %s", policy.WhenScaled))
	}
	affected := make([]domain.ObjectReference, 0, replicas)
	names := make([]string, 0, replicas)
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		names = append(names, fmt.Sprintf("%s-%d", sts.Name, ordinal))
	}
	candidates, getErrors := m.readPods(ctx, pod.Namespace, names)
	for index, name := range names {
		candidate, err := candidates[index], getErrors[index]
		if err != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorPrecondition, "discover Victoria Logs", fmt.Sprintf("affected Pod %s/%s is unavailable", pod.Namespace, name), err)
		}
		if candidate.Status.Phase != corev1.PodRunning || !podReady(candidate) {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover Victoria Logs", fmt.Sprintf("affected Pod %s/%s must be Running and Ready", pod.Namespace, name))
		}
		affected = append(affected, podReference(candidate))
	}
	zero := int32(0)
	return domain.WorkloadSpec{
		Adapter:          domain.WorkloadVictoriaLogs,
		Pod:              podReference(pod),
		Controller:       objectReference(domain.AppsAPIVersion, domain.KindStatefulSet, sts.Namespace, sts.Name, sts.UID, sts.ResourceVersion),
		OriginalReplicas: &replicas,
		Ordinal:          &zero,
		AffectedPods:     affected,
	}, nil
}

func (m *Manager) kubeBlocksWorkload(ctx context.Context, pod *corev1.Pod, owner *metav1.OwnerReference, options DiscoverOptions) (domain.WorkloadSpec, error) {
	cluster := pod.Labels[kube.AppInstanceLabel]
	component := kubeBlocksComponent(pod)
	if cluster == "" || component == "" {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", "Pod lacks cluster or component identity labels")
	}
	if reason := unsupportedKubeBlocksReason(pod, nil, component); reason != "" {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", reason)
	}
	opsAPIVersion := ""
	for _, candidate := range []string{kubeBlocksClusterAPIVersion, kubeBlocksOpsAPIVersion} {
		if kube.HasAPIResource(m.discovery, candidate, "opsrequests") {
			opsAPIVersion = candidate
			break
		}
	}
	if opsAPIVersion == "" {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", "no served OpsRequest API was found")
	}
	role := podRole(pod)
	var switchoverCandidate *corev1.Pod
	if options.SwitchoverCandidate != "" {
		candidate, err := m.typed.CoreV1().Pods(pod.Namespace).Get(ctx, options.SwitchoverCandidate, metav1.GetOptions{})
		if err != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", "read switchover candidate", err)
		}
		if candidate.Labels[kube.AppInstanceLabel] != cluster || kubeBlocksComponent(candidate) != component || !podReady(candidate) {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", "switchover candidate must be a Ready Pod in the same component")
		}
		switchoverCandidate = candidate
	}
	if m.dynamic == nil {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", "dynamic client is required for Cluster pause control")
	}
	if isLeaderRole(role) && !options.AllowLeaderDowntime && switchoverCandidate == nil {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", m.kubeBlocksLeaderGuidance(ctx, pod, cluster, component, role, opsAPIVersion))
	}
	clusterGVR, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}
	clusterObject, err := m.dynamic.Resource(clusterGVR).Namespace(pod.Namespace).Get(ctx, cluster, metav1.GetOptions{})
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorKubernetes, "discover KubeBlocks", "read Cluster", err)
	}
	components, ok, err := unstructured.NestedSlice(clusterObject.Object, "spec", "componentSpecs")
	if err != nil || !ok || len(components) == 0 {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", "Cluster has no componentSpecs")
	}
	if reason := unsupportedKubeBlocksReason(pod, components, component); reason != "" {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", reason)
	}
	originalStops := make(map[string]bool, 1)
	componentFound := false
	for index := range components {
		componentSpec, componentOK := components[index].(map[string]any)
		if !componentOK {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("componentSpecs[%d] is malformed", index))
		}
		name, nameOK, nameErr := unstructured.NestedString(componentSpec, "name")
		if nameErr != nil || !nameOK || name == "" {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("componentSpecs[%d] has no name", index))
		}
		_, _, componentDefErr := unstructured.NestedString(componentSpec, "componentDefRef")
		if componentDefErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("read component %s definition", name), componentDefErr)
		}
		stopped, _, stopErr := unstructured.NestedBool(componentSpec, "stop")
		if stopErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("read component %s stop state", name), stopErr)
		}
		if name == component {
			originalStops[name] = stopped
			componentFound = true
		}
	}
	if !componentFound {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("Cluster componentSpecs has no component %s", component))
	}
	instanceSet, err := m.discoverKubeBlocksInstanceSet(ctx, pod.Namespace, owner, pod.Name)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}
	if role != "" && instanceSet.Role != "" && !strings.EqualFold(role, instanceSet.Role) {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorConflict, "discover KubeBlocks", fmt.Sprintf("selected instance role changed from %s to %s during discovery; rerun the plan", role, instanceSet.Role))
	}
	if role == "" {
		role = instanceSet.Role
	}
	roleIsLeader := isLeaderRole(role)
	if instanceSet.HasLeaderRole {
		if role == "" {
			if !options.AllowLeaderDowntime {
				return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("selected instance %s role is unavailable while InstanceSet %s declares leader roles; wait for the KubeBlocks role probe to recover, or use --allow-leader-downtime to acknowledge a possible leader outage", pod.Name, owner.Name))
			}
			role = "unknown"
			roleIsLeader = false
		} else if knownLeader, knownRole := instanceSet.LeaderRoles[strings.ToLower(role)]; knownRole {
			roleIsLeader = knownLeader
		} else if !options.AllowLeaderDowntime {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("selected instance %s reports role %s, which is absent from InstanceSet %s role definitions; wait for role status to converge, or use --allow-leader-downtime", pod.Name, role, owner.Name))
		}
	}
	if roleIsLeader && !options.AllowLeaderDowntime && switchoverCandidate == nil {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", m.kubeBlocksLeaderGuidance(ctx, pod, cluster, component, role, opsAPIVersion))
	}
	if roleIsLeader && switchoverCandidate != nil {
		candidateRole := podRole(switchoverCandidate)
		if instanceSet.HasLeaderRole {
			candidateIsLeader, knownRole := instanceSet.LeaderRoles[strings.ToLower(candidateRole)]
			if !knownRole || candidateIsLeader {
				return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("switchover candidate %s must have a known non-leader role in InstanceSet %s", switchoverCandidate.Name, owner.Name))
			}
		}
		if err := m.validateKubeBlocksSwitchover(ctx, pod.Namespace, cluster, component, pod.Name, switchoverCandidate.Name, opsAPIVersion); err != nil {
			return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("automatic switchover for selected instance %s was rejected by the served OpsRequest API: %v; use the component's native switchover procedure or --allow-leader-downtime", pod.Name, err))
		}
	}
	if owner.Kind == domain.KindInstanceSet && instanceSet.Paused {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf("InstanceSet %s/%s is already paused; set spec.paused=false and wait for Pod %s/%s to become Ready before retrying", pod.Namespace, owner.Name, pod.Namespace, pod.Name),
		)
	}
	controllerUID := owner.UID
	if instanceSet.UID != "" {
		controllerUID = instanceSet.UID
	}
	return domain.WorkloadSpec{
		Adapter:    domain.WorkloadKubeBlocks,
		Pod:        podReference(pod),
		Controller: objectReference(owner.APIVersion, owner.Kind, pod.Namespace, owner.Name, controllerUID, ""),
		KubeBlocks: &domain.KubeBlocksSpec{
			Cluster:                  cluster,
			Component:                component,
			Instance:                 pod.Name,
			Role:                     role,
			SwitchoverCandidate:      options.SwitchoverCandidate,
			OpsAPIVersion:            opsAPIVersion,
			ClusterAPIVersion:        kubeBlocksClusterAPIVersion,
			ClusterUID:               clusterObject.GetUID(),
			OriginalStops:            originalStops,
			OriginalPaused:           instanceSet.Paused,
			OriginalPausedConfigured: instanceSet.PausedConfigured,
		},
	}, nil
}

func (m *Manager) discoverKubeBlocksInstanceSet(ctx context.Context, namespace string, owner *metav1.OwnerReference, podName string) (kubeBlocksInstanceSetState, error) {
	state := kubeBlocksInstanceSetState{}
	if owner.Kind != domain.KindInstanceSet {
		return state, nil
	}
	if m.dynamic == nil {
		return state, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", "dynamic client is required for InstanceSet reconciliation control")
	}
	gvr, err := kube.ParseGroupVersionResource(owner.APIVersion, instanceSetResource)
	if err != nil {
		return state, err
	}
	resource := m.dynamic.Resource(gvr).Namespace(namespace)
	object, err := resource.Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return state, domain.WrapError(domain.ErrorKubernetes, "discover KubeBlocks", "read InstanceSet", err)
	}
	if owner.UID != "" && object.GetUID() != "" && object.GetUID() != owner.UID {
		return state, domain.NewError(domain.ErrorConflict, "discover KubeBlocks", fmt.Sprintf("InstanceSet %s/%s UID changed", namespace, owner.Name))
	}
	state.UID = object.GetUID()
	state.LeaderRoles, state.HasLeaderRole, err = kubeBlocksLeaderRoles(object)
	if err != nil {
		return state, err
	}
	state.Role, err = kubeBlocksMemberRole(object, podName)
	if err != nil {
		return state, err
	}
	paused, found, err := unstructured.NestedBool(object.Object, "spec", "paused")
	if err != nil {
		return state, domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", "read InstanceSet paused state", err)
	}
	state.Paused = paused
	state.PausedConfigured = found
	if !found {
		probe := object.DeepCopy()
		if err := unstructured.SetNestedField(probe.Object, true, "spec", "paused"); err != nil {
			return state, err
		}
		result, updateErr := resource.Update(ctx, probe, metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}})
		if updateErr != nil {
			return state, domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", "probe InstanceSet spec.paused support", updateErr)
		}
		if _, supported, nestedErr := unstructured.NestedBool(result.Object, "spec", "paused"); nestedErr != nil || !supported {
			return state, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("InstanceSet %s/%s does not support spec.paused", namespace, owner.Name))
		}
	}
	return state, nil
}

func kubeBlocksLeaderRoles(instanceSet *unstructured.Unstructured) (map[string]bool, bool, error) {
	roles, found, err := unstructured.NestedSlice(instanceSet.Object, "spec", "roles")
	if err != nil {
		return nil, false, domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", "read InstanceSet role definitions", err)
	}
	if !found || len(roles) == 0 {
		return nil, false, nil
	}
	result := make(map[string]bool, len(roles))
	hasLeader := false
	for index, value := range roles {
		role, ok := value.(map[string]any)
		if !ok {
			return nil, false, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("InstanceSet spec.roles[%d] is malformed", index))
		}
		name, _, nameErr := unstructured.NestedString(role, "name")
		leader, _, leaderErr := unstructured.NestedBool(role, "isLeader")
		if nameErr != nil || leaderErr != nil || name == "" {
			return nil, false, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("InstanceSet spec.roles[%d] has invalid role identity", index))
		}
		result[strings.ToLower(name)] = leader
		hasLeader = hasLeader || leader
	}
	return result, hasLeader, nil
}

func kubeBlocksMemberRole(instanceSet *unstructured.Unstructured, podName string) (string, error) {
	members, found, err := unstructured.NestedSlice(instanceSet.Object, "status", "membersStatus")
	if err != nil {
		return "", domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", "read InstanceSet member roles", err)
	}
	if !found {
		return "", nil
	}
	for index, value := range members {
		member, ok := value.(map[string]any)
		if !ok {
			return "", domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("InstanceSet status.membersStatus[%d] is malformed", index))
		}
		name, _, nameErr := unstructured.NestedString(member, "podName")
		if nameErr != nil {
			return "", domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("read InstanceSet member %d Pod name", index), nameErr)
		}
		if name != podName {
			continue
		}
		role, _, roleErr := unstructured.NestedString(member, "role", "name")
		if roleErr != nil {
			return "", domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", fmt.Sprintf("read InstanceSet member %s role", podName), roleErr)
		}
		return strings.ToLower(role), nil
	}
	return "", nil
}

func kubeBlocksComponent(pod *corev1.Pod) string {
	component := pod.Labels[kubeBlocksComponentLabel]
	if component == "" {
		component = pod.Labels[kube.AppComponentLabel]
	}
	return component
}

func (m *Manager) kubeBlocksLeaderGuidance(ctx context.Context, selected *corev1.Pod, cluster, component, role, opsAPIVersion string) string {
	candidate := m.readyKubeBlocksCandidate(ctx, selected, cluster, component)
	if candidate == "" {
		candidate = "REPLACE_WITH_READY_SECONDARY_POD"
	}
	if candidate != "REPLACE_WITH_READY_SECONDARY_POD" {
		if err := m.validateKubeBlocksSwitchover(ctx, selected.Namespace, cluster, component, selected.Name, candidate, opsAPIVersion); err != nil {
			return fmt.Sprintf("selected instance %s has role %s; the served OpsRequest API rejected automatic switchover to %s: %v. Use the component's native switchover procedure, or use --allow-leader-downtime to acknowledge the leader outage", selected.Name, role, candidate, err)
		}
	}
	return fmt.Sprintf(
		"selected instance %s has role %s; use --kubeblocks-candidate %s for an automatic switchover, or complete a native switchover first and rerun the plan. Use --allow-leader-downtime to acknowledge the leader outage. KubeBlocks commands: kbcli cluster promote %s --namespace %s --instance %s --candidate %s; or %s",
		selected.Name, role, candidate, cluster, selected.Namespace, selected.Name, candidate, kubeBlocksSwitchoverCommand(selected.Namespace, cluster, component, selected.Name, candidate, opsAPIVersion),
	)
}

func (m *Manager) readyKubeBlocksCandidate(ctx context.Context, selected *corev1.Pod, cluster, component string) string {
	pods, err := m.typed.CoreV1().Pods(selected.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil || pods == nil {
		return ""
	}
	candidates := make([]string, 0, len(pods.Items))
	for index := range pods.Items {
		candidate := &pods.Items[index]
		if candidate.Name == selected.Name || candidate.Labels[kube.AppInstanceLabel] != cluster || kubeBlocksComponent(candidate) != component || !podReady(candidate) || isLeaderRole(podRole(candidate)) {
			continue
		}
		candidates = append(candidates, candidate.Name)
	}
	sort.Strings(candidates)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}

func kubeBlocksSwitchoverCommand(namespace, cluster, component, selected, candidate, opsAPIVersion string) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "kubectl create -f - <<'YAML'\napiVersion: %s\nkind: OpsRequest\nmetadata:\n  generateName: %s-switchover-\n  namespace: %s\nspec:\n  clusterName: %s\n  type: Switchover\n  switchover:\n  - componentName: %s\n", opsAPIVersion, cluster, namespace, cluster, component)
	if strings.HasPrefix(opsAPIVersion, "operations.kubeblocks.io/") {
		fmt.Fprintf(&builder, "    instanceName: %s\n    candidateName: %s\n", selected, candidate)
	} else {
		fmt.Fprintf(&builder, "    instanceName: %s\n", candidate)
	}
	builder.WriteString("YAML")
	return builder.String()
}

func (m *Manager) validateKubeBlocksSwitchover(ctx context.Context, namespace, cluster, component, selected, candidate, opsAPIVersion string) error {
	gvr, err := opsGVR(opsAPIVersion)
	if err != nil {
		return err
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": opsAPIVersion,
		"kind":       "OpsRequest",
		"metadata": map[string]any{
			"generateName": "pvc-migrate-switchover-",
			"namespace":    namespace,
		},
		"spec": kubeBlocksSwitchoverSpec(opsAPIVersion, cluster, component, selected, candidate),
	}}
	_, err = m.dynamic.Resource(gvr).Namespace(namespace).Create(ctx, object, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	return err
}

func kubeBlocksSwitchoverSpec(opsAPIVersion, cluster, component, selected, candidate string) map[string]any {
	switchover := map[string]any{"componentName": component, "instanceName": candidate}
	if strings.HasPrefix(opsAPIVersion, "operations.kubeblocks.io/") {
		switchover["instanceName"] = selected
		switchover["candidateName"] = candidate
	}
	return map[string]any{
		"clusterName": cluster,
		"type":        "Switchover",
		"switchover":  []any{switchover},
	}
}

func unsupportedKubeBlocksReason(pod *corev1.Pod, components []any, selectedComponent string) string {
	values := []string{
		selectedComponent,
		pod.Labels[kube.AppNameLabel],
		pod.Labels[kube.AppComponentLabel],
		pod.Labels[kubeBlocksComponentLabel],
	}
	for _, container := range append(append([]corev1.Container(nil), pod.Spec.InitContainers...), pod.Spec.Containers...) {
		values = append(values, container.Name, container.Image)
	}
	for index := range components {
		component, ok := components[index].(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(component, "name")
		if name != selectedComponent {
			continue
		}
		definition, _, _ := unstructured.NestedString(component, "componentDefRef")
		values = append(values, name, definition)
	}
	for _, value := range values {
		lower := strings.ToLower(value)
		switch {
		case strings.Contains(lower, "minio"):
			return fmt.Sprintf("KubeBlocks MinIO component %s requires MinIO's native drive or pool maintenance", selectedComponent)
		case strings.Contains(lower, "cockroach"), strings.Contains(lower, "crdb"):
			return fmt.Sprintf("KubeBlocks CockroachDB component %s requires CockroachDB drain and decommission", selectedComponent)
		case strings.Contains(lower, "archive-wal"), lower == "wal", strings.Contains(lower, "wal-tool"):
			return fmt.Sprintf("KubeBlocks archive-WAL component %s is a backup workload and cannot be migrated", selectedComponent)
		}
	}
	return ""
}

func (m *Manager) vmClusterWorkload(ctx context.Context, pod *corev1.Pod, owner, parent *metav1.OwnerReference, sts *appsv1.StatefulSet, options DiscoverOptions) (domain.WorkloadSpec, error) {
	if sts == nil {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorInternal, "discover VMCluster", "StatefulSet is required")
	}
	base, err := m.statefulSetWorkload(ctx, pod, sts, options)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}
	component := ""
	for _, candidate := range []string{"vmstorage", "vmselect", "vminsert"} {
		if strings.Contains(strings.ToLower(sts.Name), candidate) {
			component = candidate
			break
		}
	}
	if component == "" {
		component = sts.Labels[kube.AppComponentLabel]
	}
	if component != "vmstorage" && component != "vmselect" && component != "vminsert" {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover VMCluster", fmt.Sprintf("StatefulSet %s/%s has no supported VMCluster component", pod.Namespace, sts.Name))
	}
	originalPaused := false
	originalPausedConfigured := false
	originalClusterPaused := false
	originalClusterPausedConfigured := false
	var vmUID types.UID
	if m.dynamic != nil {
		gvr, parseErr := kube.ParseGroupVersionResource(vmClusterAPIVersion, vmClusterResource)
		if parseErr != nil {
			return domain.WorkloadSpec{}, parseErr
		}
		vm, getErr := m.dynamic.Resource(gvr).Namespace(pod.Namespace).Get(ctx, parent.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorKubernetes, "discover VMCluster", "read VMCluster", getErr)
		}
		vmUID = vm.GetUID()
		originalPaused, _, _ = unstructured.NestedBool(vm.Object, "spec", component, "paused")
		_, originalPausedConfigured, _ = unstructured.NestedBool(vm.Object, "spec", component, "paused")
		originalClusterPaused, _, _ = unstructured.NestedBool(vm.Object, "spec", "paused")
		_, originalClusterPausedConfigured, _ = unstructured.NestedBool(vm.Object, "spec", "paused")
	}
	return domain.WorkloadSpec{
		Adapter:          domain.WorkloadVMCluster,
		Pod:              base.Pod,
		Controller:       base.Controller,
		OriginalReplicas: base.OriginalReplicas,
		Ordinal:          base.Ordinal,
		AffectedPods:     base.AffectedPods,
		VMCluster: &domain.VMClusterSpec{
			APIVersion:                      vmClusterAPIVersion,
			Name:                            parent.Name,
			UID:                             vmUID,
			Component:                       component,
			OriginalPaused:                  originalPaused,
			OriginalPausedConfigured:        originalPausedConfigured,
			OriginalClusterPaused:           originalClusterPaused,
			OriginalClusterPausedConfigured: originalClusterPausedConfigured,
			OriginalReplicas:                valueOrDefault(base.OriginalReplicas, 1),
		},
	}, nil
}

func (m *Manager) grafanaWorkload(ctx context.Context, pod *corev1.Pod, deployment *appsv1.Deployment, owner *metav1.OwnerReference) (domain.WorkloadSpec, error) {
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas <= 0 {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover Grafana", fmt.Sprintf("Deployment %s/%s has no positive replica count", deployment.Namespace, deployment.Name))
	}
	if m.dynamic == nil {
		return domain.WorkloadSpec{}, domain.NewError(domain.ErrorPrecondition, "discover Grafana", "dynamic client is required for Grafana pause control")
	}
	gvr, err := kube.ParseGroupVersionResource(grafanaAPIVersion, grafanaResource)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}
	grafana, err := m.dynamic.Resource(gvr).Namespace(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorKubernetes, "discover Grafana", "read Grafana", err)
	}
	suspended, suspendConfigured, nestedErr := unstructured.NestedBool(grafana.Object, "spec", "suspend")
	if nestedErr != nil {
		return domain.WorkloadSpec{}, domain.WrapError(domain.ErrorPrecondition, "discover Grafana", "read reconciliation suspend state", nestedErr)
	}
	return domain.WorkloadSpec{
		Adapter:          domain.WorkloadGrafana,
		Pod:              podReference(pod),
		Controller:       objectReference(domain.AppsAPIVersion, domain.KindDeployment, deployment.Namespace, deployment.Name, deployment.UID, deployment.ResourceVersion),
		OriginalReplicas: deployment.Spec.Replicas,
		AffectedPods:     []domain.ObjectReference{podReference(pod)},
		Grafana:          &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: owner.Name, UID: grafana.GetUID(), OriginalSuspend: suspended, OriginalSuspendConfigured: suspendConfigured, OriginalReplicas: *deployment.Spec.Replicas},
	}, nil
}

func valueOrDefault(value *int32, fallback int32) int32 {
	if value == nil {
		return fallback
	}
	return *value
}

func statefulSetReplicas(sts *appsv1.StatefulSet) int32 {
	if sts.Spec.Replicas == nil {
		return 1
	}
	return *sts.Spec.Replicas
}

func isCockroachStatefulSet(sts *appsv1.StatefulSet) bool {
	return strings.EqualFold(sts.Labels[kube.AppNameLabel], "cockroachdb") ||
		strings.EqualFold(sts.Labels["app"], "cockroachdb")
}

func isMinIOStatefulSet(sts *appsv1.StatefulSet) bool {
	for _, value := range []string{
		sts.Labels["app"],
		sts.Labels[kube.AppNameLabel],
		sts.Labels[kube.AppComponentLabel],
	} {
		if strings.EqualFold(value, "minio") {
			return true
		}
	}
	return false
}

func isVictoriaLogsStatefulSet(sts *appsv1.StatefulSet) bool {
	if !strings.EqualFold(sts.Labels[kube.AppNameLabel], "victoria-logs-cluster") {
		return false
	}
	return strings.EqualFold(sts.Labels[kube.AppComponentLabel], "vlstorage") ||
		strings.EqualFold(sts.Labels["app"], "vlstorage") ||
		strings.Contains(strings.ToLower(sts.Name), "-vlstorage")
}

func requireReadyPod(pod *corev1.Pod, namespace, name string) error {
	if pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
		return domain.NewError(domain.ErrorPrecondition, "discover workload", fmt.Sprintf("Pod %s/%s must be Running and Ready", namespace, name))
	}
	return nil
}

// unsupportedStatefulSetReason rejects workloads whose reconciliation or
// storage lifecycle requires an application-specific migration procedure.
// Ordinary StatefulSets, including Helm-rendered ones, use the native
// StatefulSet adapter below. Helm metadata alone does not imply a controller
// that will reconcile replica changes during the migration window.
func unsupportedStatefulSetReason(sts *appsv1.StatefulSet) string {
	if isCockroachStatefulSet(sts) {
		return fmt.Sprintf("CockroachDB StatefulSet %s/%s requires CockroachDB drain and decommission", sts.Namespace, sts.Name)
	}
	if isMinIOStatefulSet(sts) {
		return fmt.Sprintf("MinIO StatefulSet %s/%s requires MinIO drive or pool maintenance", sts.Namespace, sts.Name)
	}
	parent := controllerOwner(sts.OwnerReferences)
	if parent != nil {
		parentGV, err := schema.ParseGroupVersion(parent.APIVersion)
		if err == nil {
			switch parent.Kind {
			case domain.KindBackup:
				return fmt.Sprintf("Backup-owned archive-WAL StatefulSet %s/%s is a backup workload and cannot be migrated", sts.Namespace, sts.Name)
			case "Tenant":
				if parentGV.Group == "minio.min.io" {
					return fmt.Sprintf("MinIO Tenant StatefulSet %s/%s requires MinIO drive or pool maintenance", sts.Namespace, sts.Name)
				}
			case domain.KindVMCluster:
				if parentGV.Group == "operator.victoriametrics.com" {
					return ""
				}
			case domain.KindComponent, domain.KindInstanceSet:
				if strings.Contains(parentGV.Group, "kubeblocks.io") {
					return ""
				}
			}
		}
	}
	return ""
}

func controllerOwner(owners []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range owners {
		if owners[i].Controller != nil && *owners[i].Controller {
			return &owners[i]
		}
	}
	return nil
}

func podReference(pod *corev1.Pod) domain.ObjectReference {
	return objectReference(domain.CoreAPIVersion, domain.KindPod, pod.Namespace, pod.Name, pod.UID, pod.ResourceVersion)
}

func objectReference(apiVersion, kind, namespace, name string, uid types.UID, resourceVersion string) domain.ObjectReference {
	return domain.ObjectReference{APIVersion: apiVersion, Kind: kind, Namespace: namespace, Name: name, UID: uid, ResourceVersion: resourceVersion}
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podRole(pod *corev1.Pod) string {
	for _, key := range []string{kubeBlocksRoleLabel, kubeBlocksAppsRoleLabel, genericRoleLabel} {
		if value := strings.ToLower(pod.Labels[key]); value != "" {
			return value
		}
	}
	return ""
}

func isLeaderRole(role string) bool {
	switch strings.ToLower(role) {
	case "leader", "primary", "master":
		return true
	default:
		return false
	}
}

func podOrdinal(pod *corev1.Pod, statefulSetName string) (int32, error) {
	value := pod.Labels[appsv1.PodIndexLabel]
	if value == "" {
		value = strings.TrimPrefix(pod.Name, statefulSetName+"-")
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed < 0 || pod.Name != fmt.Sprintf("%s-%d", statefulSetName, parsed) {
		return 0, domain.NewError(domain.ErrorPrecondition, "discover StatefulSet", fmt.Sprintf("cannot derive ordinal from Pod %s", pod.Name))
	}
	return int32(parsed), nil
}

func opsGVR(apiVersion string) (schema.GroupVersionResource, error) {
	return kube.ParseGroupVersionResource(apiVersion, "opsrequests")
}

func (m *Manager) createAndWaitOps(ctx context.Context, session *domain.Session, action string, spec map[string]any) error {
	kb := session.Spec.Workload().KubeBlocks
	gvr, err := opsGVR(kb.OpsAPIVersion)
	if err != nil {
		return err
	}
	name := operationName(session.ID, action)
	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
	existing, getErr := resource.Get(ctx, name, metav1.GetOptions{})
	create := apierrors.IsNotFound(getErr)
	if getErr == nil {
		phase, _, _ := unstructured.NestedString(existing.Object, "status", "phase")
		if phase == "Failed" || phase == "Cancelled" || phase == "Aborted" {
			uid := existing.GetUID()
			if err := resource.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
				return domain.WrapError(domain.ErrorKubernetes, "KubeBlocks operation", fmt.Sprintf("delete failed OpsRequest %s", name), err)
			}
			if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("failed OpsRequest %s deletion", name), func(waitCtx context.Context) (bool, error) {
				_, err := resource.Get(waitCtx, name, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}); err != nil {
				return err
			}
			create = true
		}
	}
	if create {
		object := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": kb.OpsAPIVersion,
			"kind":       "OpsRequest",
			"metadata": map[string]any{
				"name":      name,
				"namespace": session.Spec.Workload().Pod.Namespace,
				"labels": map[string]any{
					kube.ManagedByLabel: kube.ManagedByValue,
					kube.SessionKey:     session.ID,
				},
			},
			"spec": spec,
		}}
		_, err = resource.Create(ctx, object, metav1.CreateOptions{})
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "KubeBlocks operation", fmt.Sprintf("create OpsRequest %s", name), err)
		}
	} else if getErr != nil {
		return domain.WrapError(domain.ErrorKubernetes, "KubeBlocks operation", fmt.Sprintf("read OpsRequest %s", name), getErr)
	}
	return kube.WaitFor(ctx, m.poll, fmt.Sprintf("KubeBlocks OpsRequest %s", name), func(waitCtx context.Context) (bool, error) {
		current, readErr := resource.Get(waitCtx, name, metav1.GetOptions{})
		if readErr != nil {
			return false, domain.WrapError(domain.ErrorKubernetes, "KubeBlocks operation", "read OpsRequest status", readErr)
		}
		phase, _, _ := unstructured.NestedString(current.Object, "status", "phase")
		switch phase {
		case "Succeed":
			return true, nil
		case "Failed", "Cancelled", "Aborted":
			return false, domain.NewError(domain.ErrorPrecondition, "KubeBlocks operation", fmt.Sprintf("OpsRequest %s ended in phase %s", name, phase))
		default:
			return false, nil
		}
	})
}

func operationName(sessionID, action string) string {
	return kube.BoundedName("pvc-migrate", sessionID, action)
}

func (m *Manager) patchStatefulSetReplicas(ctx context.Context, ref domain.ObjectReference, replicas int32, allowedCurrent ...int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := m.typed.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if ref.UID != "" && sts.UID != ref.UID {
			return domain.NewError(domain.ErrorConflict, "scale StatefulSet", fmt.Sprintf("StatefulSet %s/%s UID changed", ref.Namespace, ref.Name))
		}
		current := int32(1)
		if sts.Spec.Replicas != nil {
			current = *sts.Spec.Replicas
		}
		if current == replicas {
			return nil
		}
		allowed := false
		for _, candidate := range allowedCurrent {
			if current == candidate {
				allowed = true
				break
			}
		}
		if !allowed {
			return domain.NewError(domain.ErrorConflict, "scale StatefulSet", fmt.Sprintf("StatefulSet %s/%s replicas changed to %d", ref.Namespace, ref.Name, current))
		}
		sts.Spec.Replicas = &replicas
		_, err = m.typed.AppsV1().StatefulSets(ref.Namespace).Update(ctx, sts, metav1.UpdateOptions{})
		return err
	})
}

func (m *Manager) pauseStatefulSet(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Ordinal == nil || workload.OriginalReplicas == nil {
		return domain.NewError(domain.ErrorInternal, "pause StatefulSet", "session lacks replica state")
	}
	if err := m.patchStatefulSetReplicas(ctx, workload.Controller, *workload.Ordinal, *workload.OriginalReplicas); err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}
		return domain.WrapError(domain.ErrorKubernetes, "pause StatefulSet", "scale down", err)
	}
	for _, pod := range workload.AffectedPods {
		if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("Pod %s/%s deletion", pod.Namespace, pod.Name), func(waitCtx context.Context) (bool, error) {
			_, getErr := m.typed.CoreV1().Pods(pod.Namespace).Get(waitCtx, pod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}
			return false, getErr
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) pauseVictoriaLogs(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Controller.Kind != domain.KindStatefulSet || workload.OriginalReplicas == nil {
		return domain.NewError(domain.ErrorInternal, "pause Victoria Logs", "session lacks StatefulSet replica state")
	}
	if err := m.patchVictoriaLogsReplicas(ctx, session, 0, false); err != nil {
		return err
	}
	for _, pod := range workload.AffectedPods {
		if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("Pod %s/%s deletion", pod.Namespace, pod.Name), func(waitCtx context.Context) (bool, error) {
			_, getErr := m.typed.CoreV1().Pods(pod.Namespace).Get(waitCtx, pod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}
			return false, getErr
		}); err != nil {
			return err
		}
	}
	return m.VerifyPaused(ctx, session)
}

func (m *Manager) resumeStatefulSet(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil || workload.Ordinal == nil {
		return domain.NewError(domain.ErrorInternal, "resume StatefulSet", "session lacks replica state")
	}
	if err := m.patchStatefulSetReplicas(ctx, workload.Controller, *workload.OriginalReplicas, *workload.Ordinal); err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}
		return domain.WrapError(domain.ErrorKubernetes, "resume StatefulSet", "restore replicas", err)
	}
	for _, ref := range workload.AffectedPods {
		if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("Pod %s/%s readiness", ref.Namespace, ref.Name), func(waitCtx context.Context) (bool, error) {
			pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return podReady(pod), nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) resumeVictoriaLogs(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Controller.Kind != domain.KindStatefulSet || workload.OriginalReplicas == nil {
		return domain.NewError(domain.ErrorInternal, "resume Victoria Logs", "session lacks StatefulSet replica state")
	}
	if err := m.patchVictoriaLogsReplicas(ctx, session, *workload.OriginalReplicas, true); err != nil {
		return err
	}
	for _, ref := range workload.AffectedPods {
		if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("Pod %s/%s readiness", ref.Namespace, ref.Name), func(waitCtx context.Context) (bool, error) {
			pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return podReady(pod), nil
		}); err != nil {
			return err
		}
	}
	return m.clearVictoriaLogsPauseOwner(ctx, session)
}

func (m *Manager) patchVictoriaLogsReplicas(ctx context.Context, session *domain.Session, replicas int32, resuming bool) error {
	ref := session.Spec.Workload().Controller
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := m.typed.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "Victoria Logs pause", "read StatefulSet", err)
		}
		if ref.UID != "" && sts.UID != ref.UID {
			return domain.NewError(domain.ErrorConflict, "Victoria Logs pause", fmt.Sprintf("StatefulSet %s/%s UID changed", ref.Namespace, ref.Name))
		}
		annotations := sts.GetAnnotations()
		owner := annotations[pauseSessionAnnotation]
		if owner != "" && owner != session.ID {
			return domain.NewError(domain.ErrorConflict, "Victoria Logs pause", fmt.Sprintf("StatefulSet %s/%s pause is owned by session %s", ref.Namespace, ref.Name, owner))
		}
		current := statefulSetReplicas(sts)
		if resuming {
			if owner != session.ID {
				return domain.NewError(domain.ErrorConflict, "Victoria Logs resume", fmt.Sprintf("StatefulSet %s/%s is not owned by session %s", ref.Namespace, ref.Name, session.ID))
			}
			if current != 0 && current != replicas {
				return domain.NewError(domain.ErrorConflict, "Victoria Logs resume", fmt.Sprintf("StatefulSet %s/%s replicas changed to %d", ref.Namespace, ref.Name, current))
			}
		} else {
			if owner == session.ID && current == replicas {
				return nil
			}
			if owner == "" && current != *session.Spec.Workload().OriginalReplicas {
				return domain.NewError(domain.ErrorConflict, "Victoria Logs pause", fmt.Sprintf("StatefulSet %s/%s replicas changed to %d", ref.Namespace, ref.Name, current))
			}
		}
		changed := current != replicas
		if changed {
			sts.Spec.Replicas = &replicas
		}
		if !resuming {
			if annotations == nil {
				annotations = map[string]string{}
			}
			if annotations[pauseSessionAnnotation] != session.ID {
				annotations[pauseSessionAnnotation] = session.ID
				changed = true
			}
		}
		if !changed {
			return nil
		}
		sts.SetAnnotations(annotations)
		_, err = m.typed.AppsV1().StatefulSets(ref.Namespace).Update(ctx, sts, metav1.UpdateOptions{})
		return err
	})
}

func (m *Manager) clearVictoriaLogsPauseOwner(ctx context.Context, session *domain.Session) error {
	ref := session.Spec.Workload().Controller
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := m.typed.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "Victoria Logs resume", "read StatefulSet", err)
		}
		if ref.UID != "" && sts.UID != ref.UID {
			return domain.NewError(domain.ErrorConflict, "Victoria Logs resume", fmt.Sprintf("StatefulSet %s/%s UID changed", ref.Namespace, ref.Name))
		}
		annotations := sts.GetAnnotations()
		if annotations[pauseSessionAnnotation] != session.ID {
			return domain.NewError(domain.ErrorConflict, "Victoria Logs resume", fmt.Sprintf("StatefulSet %s/%s pause ownership changed", ref.Namespace, ref.Name))
		}
		delete(annotations, pauseSessionAnnotation)
		sts.SetAnnotations(annotations)
		_, err = m.typed.AppsV1().StatefulSets(ref.Namespace).Update(ctx, sts, metav1.UpdateOptions{})
		return err
	})
}

func (m *Manager) pauseVMCluster(ctx context.Context, session *domain.Session) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(domain.ErrorInternal, "pause VMCluster", "session lacks VMCluster state")
	}
	workload := session.Spec.Workload()
	if workload.Ordinal == nil || workload.OriginalReplicas == nil {
		return domain.NewError(domain.ErrorInternal, "pause VMCluster", "session lacks StatefulSet replica state")
	}
	if err := m.setVMClusterPaused(ctx, session, true); err != nil {
		return err
	}
	// Keep lower ordinals available while preventing the operator from
	// restoring the StatefulSet to its original replica count.
	if err := m.setVMClusterReplicaCount(ctx, session, *workload.Ordinal); err != nil {
		if restoreErr := m.restoreVMClusterPause(ctx, session); restoreErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "pause VMCluster", fmt.Sprintf("set component replicas: %v; restore component pause state", err), restoreErr)
		}
		return err
	}
	if err := m.patchStatefulSetReplicas(ctx, workload.Controller, *workload.Ordinal, *workload.OriginalReplicas); err != nil {
		if restoreErr := m.restoreVMClusterPause(ctx, session); restoreErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "pause VMCluster", fmt.Sprintf("scale component StatefulSet: %v; restore component pause state", err), restoreErr)
		}
		return workloadScaleError("pause VMCluster", "scale component StatefulSet", err)
	}
	for _, pod := range workload.AffectedPods {
		if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("Pod %s/%s deletion", pod.Namespace, pod.Name), func(waitCtx context.Context) (bool, error) {
			_, getErr := m.typed.CoreV1().Pods(pod.Namespace).Get(waitCtx, pod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}
			return false, getErr
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) resumeVMCluster(ctx context.Context, session *domain.Session) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(domain.ErrorInternal, "resume VMCluster", "session lacks VMCluster state")
	}
	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil || workload.Ordinal == nil {
		return domain.NewError(domain.ErrorInternal, "resume VMCluster", "session lacks StatefulSet replica state")
	}
	if err := m.setVMClusterReplicaCount(ctx, session, *workload.OriginalReplicas); err != nil {
		return err
	}
	if err := m.patchStatefulSetReplicas(ctx, workload.Controller, *workload.OriginalReplicas, *workload.Ordinal); err != nil {
		return workloadScaleError("resume VMCluster", "restore component StatefulSet", err)
	}
	for _, ref := range workload.AffectedPods {
		if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("Pod %s/%s readiness", ref.Namespace, ref.Name), func(waitCtx context.Context) (bool, error) {
			pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return podReady(pod), nil
		}); err != nil {
			return err
		}
	}
	if err := m.restoreVMClusterPause(ctx, session); err != nil {
		return err
	}
	return m.waitForVMClusterOperational(ctx, session)
}

func (m *Manager) waitForVMClusterOperational(ctx context.Context, session *domain.Session) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(domain.ErrorInternal, "wait for VMCluster", "session lacks VMCluster state")
	}
	if m.dynamic == nil {
		return domain.NewError(domain.ErrorPrecondition, "wait for VMCluster", "dynamic client is required for convergence checks")
	}
	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}
	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
	return kube.WaitFor(ctx, m.poll, fmt.Sprintf("VMCluster %s/%s convergence", session.Spec.Workload().Pod.Namespace, vm.Name), func(waitCtx context.Context) (bool, error) {
		object, getErr := resource.Get(waitCtx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return false, domain.WrapError(domain.ErrorKubernetes, "wait for VMCluster", "read VMCluster", getErr)
		}
		if vm.UID != "" && object.GetUID() != vm.UID {
			return false, domain.NewError(domain.ErrorConflict, "wait for VMCluster", fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()))
		}
		observedGeneration, found, nestedErr := unstructured.NestedInt64(object.Object, "status", "observedGeneration")
		if nestedErr != nil {
			return false, domain.WrapError(domain.ErrorPrecondition, "wait for VMCluster", "read observed generation", nestedErr)
		}
		if !found || observedGeneration < object.GetGeneration() {
			return false, nil
		}
		currentClusterPaused, clusterPausedFound, nestedErr := unstructured.NestedBool(object.Object, "spec", "paused")
		if nestedErr != nil {
			return false, domain.WrapError(domain.ErrorPrecondition, "wait for VMCluster", "read top-level pause state", nestedErr)
		}
		if vm.OriginalClusterPausedConfigured {
			if !clusterPausedFound || currentClusterPaused != vm.OriginalClusterPaused {
				return false, domain.NewError(domain.ErrorConflict, "wait for VMCluster", fmt.Sprintf("VMCluster %s/%s top-level paused changed from expected %t to %t", object.GetNamespace(), object.GetName(), vm.OriginalClusterPaused, currentClusterPaused))
			}
		} else if clusterPausedFound && currentClusterPaused {
			return false, domain.NewError(domain.ErrorConflict, "wait for VMCluster", fmt.Sprintf("VMCluster %s/%s was paused externally during migration", object.GetNamespace(), object.GetName()))
		}
		clusterStatus, _, nestedErr := unstructured.NestedString(object.Object, "status", "clusterStatus")
		if nestedErr != nil {
			return false, domain.WrapError(domain.ErrorPrecondition, "wait for VMCluster", "read cluster status", nestedErr)
		}
		updateStatus, _, nestedErr := unstructured.NestedString(object.Object, "status", "updateStatus")
		if nestedErr != nil {
			return false, domain.WrapError(domain.ErrorPrecondition, "wait for VMCluster", "read update status", nestedErr)
		}
		if vm.OriginalClusterPausedConfigured && vm.OriginalClusterPaused {
			// A top-level paused VMCluster intentionally remains outside the
			// operator's operational state machine. The observed generation still
			// fences us against an object replacement or a stale read.
			return true, nil
		}
		return strings.EqualFold(clusterStatus, "operational") && strings.EqualFold(updateStatus, "operational"), nil
	})
}

func (m *Manager) restoreVMClusterPause(ctx context.Context, session *domain.Session) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(domain.ErrorInternal, "restore VMCluster pause", "session lacks VMCluster state")
	}
	if m.dynamic == nil {
		return domain.NewError(domain.ErrorPrecondition, "restore VMCluster pause", "dynamic client is required for component pause control")
	}
	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
		object, getErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "restore VMCluster pause", "read VMCluster", getErr)
		}
		if vm.UID != "" && object.GetUID() != vm.UID {
			return domain.NewError(domain.ErrorConflict, "restore VMCluster pause", fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()))
		}
		componentObject, ok, nestedErr := unstructured.NestedMap(object.Object, "spec", vm.Component)
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "restore VMCluster pause", "read component pause state", nestedErr)
		}
		if !ok {
			return domain.NewError(domain.ErrorPrecondition, "restore VMCluster pause", fmt.Sprintf("VMCluster component %s is absent", vm.Component))
		}
		current, _, nestedErr := unstructured.NestedBool(componentObject, "paused")
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "restore VMCluster pause", "read component pause state", nestedErr)
		}
		annotations := object.GetAnnotations()
		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(domain.ErrorConflict, "restore VMCluster pause", fmt.Sprintf("VMCluster %s/%s pause is owned by session %s", object.GetNamespace(), object.GetName(), pauseOwner))
		}
		if pauseOwner == "" {
			if current != vm.OriginalPaused {
				return domain.NewError(domain.ErrorConflict, "restore VMCluster pause", fmt.Sprintf("VMCluster component %s paused changed from expected %t to %t", vm.Component, vm.OriginalPaused, current))
			}
			return nil
		}
		if !current && vm.OriginalPaused {
			return domain.NewError(domain.ErrorConflict, "restore VMCluster pause", fmt.Sprintf("VMCluster component %s paused changed while session was active", vm.Component))
		}
		if current != vm.OriginalPaused {
			if err := unstructured.SetNestedField(componentObject, vm.OriginalPaused, "paused"); err != nil {
				return err
			}
			if err := unstructured.SetNestedField(object.Object, componentObject, "spec", vm.Component); err != nil {
				return err
			}
		}
		if vm.OriginalReplicas > 0 {
			if _, found, nestedErr := unstructured.NestedInt64(componentObject, "replicaCount"); nestedErr != nil {
				return domain.WrapError(domain.ErrorPrecondition, "restore VMCluster pause", "read component replica count", nestedErr)
			} else if found {
				if err := unstructured.SetNestedField(componentObject, int64(vm.OriginalReplicas), "replicaCount"); err != nil {
					return err
				}
				if err := unstructured.SetNestedField(object.Object, componentObject, "spec", vm.Component); err != nil {
					return err
				}
			}
		}
		delete(annotations, pauseSessionAnnotation)
		object.SetAnnotations(annotations)
		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}
			return domain.WrapError(domain.ErrorKubernetes, "restore VMCluster pause", "clear component pause owner", updateErr)
		}
		return nil
	})
}

func (m *Manager) setVMClusterReplicaCount(ctx context.Context, session *domain.Session, replicas int32) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(domain.ErrorInternal, "set VMCluster replicas", "session lacks VMCluster state")
	}
	if m.dynamic == nil {
		return domain.NewError(domain.ErrorPrecondition, "set VMCluster replicas", "dynamic client is required for component replica control")
	}
	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
		object, getErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "set VMCluster replicas", "read VMCluster", getErr)
		}
		if vm.UID != "" && object.GetUID() != vm.UID {
			return domain.NewError(domain.ErrorConflict, "set VMCluster replicas", fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()))
		}
		componentObject, ok, nestedErr := unstructured.NestedMap(object.Object, "spec", vm.Component)
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "set VMCluster replicas", "read component replica count", nestedErr)
		}
		if !ok {
			return domain.NewError(domain.ErrorPrecondition, "set VMCluster replicas", fmt.Sprintf("VMCluster component %s is absent", vm.Component))
		}
		if _, found, nestedErr := unstructured.NestedInt64(componentObject, "replicaCount"); nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "set VMCluster replicas", "read component replica count", nestedErr)
		} else if !found {
			return nil
		}
		annotations := object.GetAnnotations()
		if owner := annotations[pauseSessionAnnotation]; owner != "" && owner != session.ID {
			return domain.NewError(domain.ErrorConflict, "set VMCluster replicas", fmt.Sprintf("VMCluster %s/%s pause is owned by session %s", object.GetNamespace(), object.GetName(), owner))
		}
		if err := unstructured.SetNestedField(componentObject, int64(replicas), "replicaCount"); err != nil {
			return err
		}
		if err := unstructured.SetNestedField(object.Object, componentObject, "spec", vm.Component); err != nil {
			return err
		}
		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}
			return domain.WrapError(domain.ErrorKubernetes, "set VMCluster replicas", "update component replica count", updateErr)
		}
		return nil
	})
}

func (m *Manager) setVMClusterPaused(ctx context.Context, session *domain.Session, paused bool) error {
	vm := session.Spec.Workload().VMCluster
	if m.dynamic == nil {
		return domain.NewError(domain.ErrorPrecondition, "VMCluster pause", "dynamic client is required for component pause control")
	}
	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
		object, getErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "VMCluster pause", "read VMCluster", getErr)
		}
		if vm.UID != "" && object.GetUID() != vm.UID {
			return domain.NewError(domain.ErrorConflict, "VMCluster pause", fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()))
		}
		componentObject, ok, nestedErr := unstructured.NestedMap(object.Object, "spec", vm.Component)
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "VMCluster pause", "read component pause state", nestedErr)
		}
		if !ok {
			return domain.NewError(domain.ErrorPrecondition, "VMCluster pause", fmt.Sprintf("VMCluster component %s is absent", vm.Component))
		}
		current, _, nestedErr := unstructured.NestedBool(componentObject, "paused")
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "VMCluster pause", "read component pause state", nestedErr)
		}
		annotations := object.GetAnnotations()
		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(domain.ErrorConflict, "VMCluster pause", fmt.Sprintf("VMCluster %s/%s pause is owned by session %s", object.GetNamespace(), object.GetName(), pauseOwner))
		}
		if paused && pauseOwner == "" && current != vm.OriginalPaused {
			return domain.NewError(domain.ErrorConflict, "VMCluster pause", fmt.Sprintf("VMCluster component %s paused changed from expected %t to %t", vm.Component, vm.OriginalPaused, current))
		}
		if !paused && pauseOwner == "" {
			if current != vm.OriginalPaused {
				return domain.NewError(domain.ErrorConflict, "VMCluster pause", fmt.Sprintf("VMCluster component %s paused changed from expected %t to %t", vm.Component, vm.OriginalPaused, current))
			}
			return nil
		}
		if pauseOwner == session.ID && paused && current {
			return nil
		}
		if pauseOwner == session.ID && paused && !current {
			return domain.NewError(domain.ErrorConflict, "VMCluster pause", fmt.Sprintf("VMCluster component %s paused changed while session was active", vm.Component))
		}
		if pauseOwner == session.ID && !paused && !current {
			return domain.NewError(domain.ErrorConflict, "VMCluster pause", fmt.Sprintf("VMCluster component %s paused changed while session was active", vm.Component))
		}
		if err := unstructured.SetNestedField(componentObject, paused, "paused"); err != nil {
			return err
		}
		if err := unstructured.SetNestedField(object.Object, componentObject, "spec", vm.Component); err != nil {
			return err
		}
		if paused {
			if annotations == nil {
				annotations = map[string]string{}
			}
			annotations[pauseSessionAnnotation] = session.ID
		} else {
			delete(annotations, pauseSessionAnnotation)
		}
		object.SetAnnotations(annotations)
		_, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{})
		if apierrors.IsConflict(updateErr) {
			return updateErr
		}
		if updateErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "VMCluster pause", "update component paused state", updateErr)
		}
		return nil
	})
}

func (m *Manager) pauseGrafana(ctx context.Context, session *domain.Session) error {
	grafana := session.Spec.Workload().Grafana
	if grafana == nil || session.Spec.Workload().OriginalReplicas == nil {
		return domain.NewError(domain.ErrorInternal, "pause Grafana", "session lacks Grafana state")
	}
	if err := m.setGrafanaPaused(ctx, session, true); err != nil {
		return err
	}
	if err := m.patchDeploymentReplicas(ctx, session.Spec.Workload().Controller, 0, *session.Spec.Workload().OriginalReplicas); err != nil {
		if restoreErr := m.restoreGrafanaPause(ctx, session); restoreErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "pause Grafana", fmt.Sprintf("scale Deployment: %v; restore Grafana suspend state", err), restoreErr)
		}
		return workloadScaleError("pause Grafana", "scale Deployment", err)
	}
	for _, ref := range session.Spec.Workload().AffectedPods {
		if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("Pod %s/%s deletion", ref.Namespace, ref.Name), func(waitCtx context.Context) (bool, error) {
			_, getErr := m.typed.CoreV1().Pods(ref.Namespace).Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}
			return false, getErr
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) resumeGrafana(ctx context.Context, session *domain.Session) error {
	grafana := session.Spec.Workload().Grafana
	if grafana == nil || session.Spec.Workload().OriginalReplicas == nil {
		return domain.NewError(domain.ErrorInternal, "resume Grafana", "session lacks Grafana state")
	}
	if err := m.patchDeploymentReplicas(ctx, session.Spec.Workload().Controller, *session.Spec.Workload().OriginalReplicas, 0); err != nil {
		return workloadScaleError("resume Grafana", "restore Deployment replicas", err)
	}
	if err := m.restoreGrafanaPause(ctx, session); err != nil {
		return err
	}
	var ready *corev1.Pod
	if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("Grafana Deployment %s/%s readiness", session.Spec.Workload().Controller.Namespace, session.Spec.Workload().Controller.Name), func(waitCtx context.Context) (bool, error) {
		deployment, err := m.typed.AppsV1().Deployments(session.Spec.Workload().Controller.Namespace).Get(waitCtx, session.Spec.Workload().Controller.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil {
			return false, err
		}
		pods, err := m.typed.CoreV1().Pods(deployment.Namespace).List(waitCtx, metav1.ListOptions{LabelSelector: selector.String()})
		if err != nil {
			return false, err
		}
		for index := range pods.Items {
			if podReady(&pods.Items[index]) {
				ready = &pods.Items[index]
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return err
	}
	if ready != nil {
		session.Spec.WorkloadPtr().Pod = podReference(ready)
	}
	return nil
}

func (m *Manager) restoreGrafanaPause(ctx context.Context, session *domain.Session) error {
	grafana := session.Spec.Workload().Grafana
	if grafana == nil {
		return domain.NewError(domain.ErrorInternal, "restore Grafana pause", "session lacks Grafana state")
	}
	if m.dynamic == nil {
		return domain.NewError(domain.ErrorPrecondition, "restore Grafana pause", "dynamic client is required for deployment pause control")
	}
	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
		object, getErr := resource.Get(ctx, grafana.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "restore Grafana pause", "read Grafana", getErr)
		}
		if grafana.UID != "" && object.GetUID() != grafana.UID {
			return domain.NewError(domain.ErrorConflict, "restore Grafana pause", fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()))
		}
		current, _, nestedErr := unstructured.NestedBool(object.Object, "spec", "suspend")
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "restore Grafana suspend", "read reconciliation suspend state", nestedErr)
		}
		annotations := object.GetAnnotations()
		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(domain.ErrorConflict, "restore Grafana suspend", fmt.Sprintf("Grafana %s/%s suspend is owned by session %s", object.GetNamespace(), object.GetName(), pauseOwner))
		}
		if pauseOwner == "" {
			if current != grafana.OriginalSuspend {
				return domain.NewError(domain.ErrorConflict, "restore Grafana suspend", fmt.Sprintf("Grafana suspend changed from expected %t to %t", grafana.OriginalSuspend, current))
			}
			return nil
		}
		if !current && grafana.OriginalSuspend {
			return domain.NewError(domain.ErrorConflict, "restore Grafana suspend", "Grafana suspend state changed while session was active")
		}
		if current != grafana.OriginalSuspend {
			if grafana.OriginalSuspendConfigured {
				if err := unstructured.SetNestedField(object.Object, grafana.OriginalSuspend, "spec", "suspend"); err != nil {
					return err
				}
			} else {
				unstructured.RemoveNestedField(object.Object, "spec", "suspend")
			}
		}
		delete(annotations, pauseSessionAnnotation)
		object.SetAnnotations(annotations)
		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}
			return domain.WrapError(domain.ErrorKubernetes, "restore Grafana suspend", "clear reconciliation suspend owner", updateErr)
		}
		return nil
	})
}

func (m *Manager) setGrafanaPaused(ctx context.Context, session *domain.Session, paused bool) error {
	grafana := session.Spec.Workload().Grafana
	if m.dynamic == nil {
		return domain.NewError(domain.ErrorPrecondition, "Grafana suspend", "dynamic client is required for reconciliation suspend control")
	}
	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
		object, getErr := resource.Get(ctx, grafana.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "Grafana suspend", "read Grafana", getErr)
		}
		if grafana.UID != "" && object.GetUID() != grafana.UID {
			return domain.NewError(domain.ErrorConflict, "Grafana suspend", fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()))
		}
		current, _, nestedErr := unstructured.NestedBool(object.Object, "spec", "suspend")
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "Grafana suspend", "read reconciliation suspend state", nestedErr)
		}
		annotations := object.GetAnnotations()
		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(domain.ErrorConflict, "Grafana suspend", fmt.Sprintf("Grafana %s/%s suspend is owned by session %s", object.GetNamespace(), object.GetName(), pauseOwner))
		}
		if pauseOwner == "" && current != grafana.OriginalSuspend {
			return domain.NewError(domain.ErrorConflict, "Grafana suspend", fmt.Sprintf("Grafana suspend changed from expected %t to %t", grafana.OriginalSuspend, current))
		}
		if pauseOwner == session.ID && current == paused {
			return nil
		}
		if pauseOwner == session.ID && paused && !current {
			return domain.NewError(domain.ErrorConflict, "Grafana suspend", "Grafana suspend state changed while session was active")
		}
		if err := unstructured.SetNestedField(object.Object, paused, "spec", "suspend"); err != nil {
			return err
		}
		if paused {
			if annotations == nil {
				annotations = map[string]string{}
			}
			annotations[pauseSessionAnnotation] = session.ID
		} else {
			delete(annotations, pauseSessionAnnotation)
		}
		object.SetAnnotations(annotations)
		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}
			return domain.WrapError(domain.ErrorKubernetes, "Grafana suspend", "update reconciliation suspend state", updateErr)
		}
		return nil
	})
}

func workloadScaleError(operation, message string, err error) error {
	if domain.CategoryOf(err) == domain.ErrorConflict {
		return err
	}
	return domain.WrapError(domain.ErrorKubernetes, operation, message, err)
}

func (m *Manager) patchDeploymentReplicas(ctx context.Context, ref domain.ObjectReference, replicas int32, allowedCurrent ...int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := m.typed.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if ref.UID != "" && deployment.UID != ref.UID {
			return domain.NewError(domain.ErrorConflict, "scale Deployment", fmt.Sprintf("Deployment %s/%s UID changed", ref.Namespace, ref.Name))
		}
		current := int32(1)
		if deployment.Spec.Replicas != nil {
			current = *deployment.Spec.Replicas
		}
		if current == replicas {
			return nil
		}
		allowed := false
		for _, candidate := range allowedCurrent {
			if current == candidate {
				allowed = true
				break
			}
		}
		if !allowed {
			return domain.NewError(domain.ErrorConflict, "scale Deployment", fmt.Sprintf("Deployment %s/%s replicas changed to %d", ref.Namespace, ref.Name, current))
		}
		deployment.Spec.Replicas = &replicas
		_, err = m.typed.AppsV1().Deployments(ref.Namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		return err
	})
}

func (m *Manager) pauseStandalone(ctx context.Context, session *domain.Session) error {
	ref := session.Spec.Workload().Pod
	pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "pause standalone Pod", "read Pod", err)
	}
	if ref.UID != "" && pod.UID != ref.UID {
		return domain.NewError(domain.ErrorConflict, "pause standalone Pod", fmt.Sprintf("Pod %s/%s UID changed", ref.Namespace, ref.Name))
	}
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}
	if err := m.typed.CoreV1().Pods(ref.Namespace).Delete(ctx, ref.Name, options); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "pause standalone Pod", "delete Pod", err)
	}
	return kube.WaitFor(ctx, m.poll, fmt.Sprintf("Pod %s/%s deletion", ref.Namespace, ref.Name), func(waitCtx context.Context) (bool, error) {
		_, getErr := m.typed.CoreV1().Pods(ref.Namespace).Get(waitCtx, ref.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(getErr), ignoreNotFound(getErr)
	})
}

func (m *Manager) resumeStandalone(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	existing, err := m.typed.CoreV1().Pods(workload.Pod.Namespace).Get(ctx, workload.Pod.Name, metav1.GetOptions{})
	if err == nil {
		if existing.Annotations[kube.SessionKey] != session.ID {
			return domain.NewError(domain.ErrorConflict, "resume standalone Pod", fmt.Sprintf("Pod %s/%s was recreated outside this session", existing.Namespace, existing.Name))
		}
		if podReady(existing) {
			session.Spec.WorkloadPtr().Pod = podReference(existing)
			return nil
		}
	} else if !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "resume standalone Pod", "read Pod", err)
	}
	var pod corev1.Pod
	if err := json.Unmarshal(workload.OriginalObject, &pod); err != nil {
		return domain.WrapError(domain.ErrorInternal, "resume standalone Pod", "decode saved Pod", err)
	}
	pod.ResourceVersion = ""
	pod.UID = ""
	pod.GenerateName = ""
	pod.Generation = 0
	pod.CreationTimestamp = metav1.Time{}
	pod.DeletionTimestamp = nil
	pod.DeletionGracePeriodSeconds = nil
	pod.ManagedFields = nil
	pod.OwnerReferences = nil
	pod.Finalizers = nil
	pod.Status = corev1.PodStatus{}
	pod.Spec.NodeName = ""
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[kube.SessionKey] = session.ID
	options := session.Spec.WorkflowOptions()
	resumeNode := options.TargetNode
	if session.Status.Phase == domain.PhaseRollingBack || session.Status.Phase == domain.PhaseAborting {
		resumeNode = options.SourceNode
	}
	if resumeNode != "" {
		node, getErr := m.typed.CoreV1().Nodes().Get(ctx, resumeNode, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "resume standalone Pod", "read resume node", getErr)
		}
		hostname := node.Labels[corev1.LabelHostname]
		if hostname == "" {
			return domain.NewError(domain.ErrorPrecondition, "resume standalone Pod", fmt.Sprintf("node %s lacks kubernetes.io/hostname", resumeNode))
		}
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = map[string]string{}
		}
		pod.Spec.NodeSelector[corev1.LabelHostname] = hostname
	}
	if _, err := m.typed.CoreV1().Pods(pod.Namespace).Create(ctx, &pod, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return domain.WrapError(domain.ErrorKubernetes, "resume standalone Pod", "create Pod", err)
		}
		// The initial Get and Create are a TOCTOU window. Revalidate ownership
		// after AlreadyExists so an unrelated actor cannot be adopted.
		existing, getErr := m.typed.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "resume standalone Pod", "read concurrently created Pod", getErr)
		}
		if existing.Annotations[kube.SessionKey] != session.ID {
			return domain.NewError(domain.ErrorConflict, "resume standalone Pod", fmt.Sprintf("Pod %s/%s was created outside this session", existing.Namespace, existing.Name))
		}
	}
	var ready *corev1.Pod
	if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("Pod %s/%s readiness", pod.Namespace, pod.Name), func(waitCtx context.Context) (bool, error) {
		current, getErr := m.typed.CoreV1().Pods(pod.Namespace).Get(waitCtx, pod.Name, metav1.GetOptions{})
		if getErr != nil {
			return false, getErr
		}
		if podReady(current) {
			ready = current
			return true, nil
		}
		return false, nil
	}); err != nil {
		return err
	}
	session.Spec.WorkloadPtr().Pod = podReference(ready)
	return nil
}

func (m *Manager) pauseKubeBlocks(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(domain.ErrorInternal, "pause KubeBlocks", "session lacks KubeBlocks state")
	}
	pod, err := m.typed.CoreV1().Pods(session.Spec.Workload().Pod.Namespace).Get(ctx, kb.Instance, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "pause KubeBlocks", "read instance Pod", err)
	}
	if session.Status.Phase == domain.PhasePausing && err == nil && isLeaderRole(podRole(pod)) && kb.SwitchoverCandidate != "" {
		spec := kubeBlocksSwitchoverSpec(kb.OpsAPIVersion, kb.Cluster, kb.Component, kb.Instance, kb.SwitchoverCandidate)
		if err := m.createAndWaitOps(ctx, session, "switchover", spec); err != nil {
			return err
		}
		current, getErr := m.typed.CoreV1().Pods(session.Spec.Workload().Pod.Namespace).Get(ctx, kb.Instance, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "pause KubeBlocks", "verify switchover role", getErr)
		}
		if isLeaderRole(podRole(current)) {
			return domain.NewError(domain.ErrorPrecondition, "pause KubeBlocks", fmt.Sprintf("instance %s retained role %s after switchover", kb.Instance, podRole(current)))
		}
	}
	if err := m.setKubeBlocksPaused(ctx, session, true); err != nil {
		return err
	}
	if session.Spec.Workload().Controller.Kind == domain.KindInstanceSet {
		if err := m.deleteKubeBlocksInstancePod(ctx, session); err != nil {
			return err
		}
		return m.VerifyPaused(ctx, session)
	}
	if err := kube.WaitFor(ctx, m.poll, fmt.Sprintf("KubeBlocks Pod %s/%s deletion", session.Spec.Workload().Pod.Namespace, session.Spec.Workload().Pod.Name), func(waitCtx context.Context) (bool, error) {
		_, getErr := m.typed.CoreV1().Pods(session.Spec.Workload().Pod.Namespace).Get(waitCtx, session.Spec.Workload().Pod.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return true, nil
		}
		return false, getErr
	}); err != nil {
		return err
	}
	return m.VerifyPaused(ctx, session)
}

func (m *Manager) resumeKubeBlocks(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(domain.ErrorInternal, "resume KubeBlocks", "session lacks KubeBlocks state")
	}
	if session.Spec.Workload().Controller.Kind == domain.KindInstanceSet && kb.OriginalPaused {
		return domain.NewError(domain.ErrorPrecondition, "resume KubeBlocks", "an initially paused InstanceSet cannot safely recreate the migrated Pod; set spec.paused=false and verify the Pod is Ready before recovery")
	}
	if err := m.setKubeBlocksPaused(ctx, session, false); err != nil {
		return err
	}
	return kube.WaitFor(ctx, m.poll, fmt.Sprintf("KubeBlocks Pod %s readiness", kb.Instance), func(waitCtx context.Context) (bool, error) {
		pod, err := m.typed.CoreV1().Pods(session.Spec.Workload().Pod.Namespace).Get(waitCtx, kb.Instance, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return podReady(pod), nil
	})
}

func (m *Manager) setKubeBlocksPaused(ctx context.Context, session *domain.Session, paused bool) error {
	if session.Spec.Workload().Controller.Kind == domain.KindInstanceSet {
		return m.setKubeBlocksInstanceSetPaused(ctx, session, paused)
	}
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(domain.ErrorInternal, "KubeBlocks pause", "session lacks KubeBlocks state")
	}
	apiVersion := kb.ClusterAPIVersion
	if apiVersion == "" {
		apiVersion = kubeBlocksClusterAPIVersion
	}
	gvr, err := kube.ParseGroupVersionResource(apiVersion, clusterResource)
	if err != nil {
		return err
	}
	if m.dynamic == nil {
		return domain.NewError(domain.ErrorPrecondition, "KubeBlocks pause", "dynamic client is required for Cluster pause")
	}
	if session.ID == "" {
		return domain.NewError(domain.ErrorInternal, "KubeBlocks pause", "session ID is required for Cluster pause ownership")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
		cluster, getErr := resource.Get(ctx, kb.Cluster, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "KubeBlocks pause", "read Cluster", getErr)
		}
		if kb.ClusterUID != "" && cluster.GetUID() != kb.ClusterUID {
			return domain.NewError(domain.ErrorConflict, "KubeBlocks pause", fmt.Sprintf("Cluster %s/%s UID changed", cluster.GetNamespace(), cluster.GetName()))
		}
		if kb.ClusterUID == "" {
			kb.ClusterUID = cluster.GetUID()
		}
		components, ok, nestedErr := unstructured.NestedSlice(cluster.Object, "spec", "componentSpecs")
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "KubeBlocks pause", "read componentSpecs", nestedErr)
		}
		if !ok || len(components) == 0 {
			return domain.NewError(domain.ErrorPrecondition, "KubeBlocks pause", "Cluster has no componentSpecs")
		}
		annotations := cluster.GetAnnotations()
		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(domain.ErrorConflict, "KubeBlocks pause", fmt.Sprintf("Cluster %s/%s pause is owned by session %s", cluster.GetNamespace(), cluster.GetName(), pauseOwner))
		}
		if kb.OriginalStops == nil {
			if !paused {
				return domain.NewError(domain.ErrorInternal, "KubeBlocks pause", "session lacks original component stop state")
			}
			kb.OriginalStops = map[string]bool{}
		}
		changed := false
		componentFound := false
		for index := range components {
			component, componentOK := components[index].(map[string]any)
			if !componentOK {
				return domain.NewError(domain.ErrorPrecondition, "KubeBlocks pause", fmt.Sprintf("componentSpecs[%d] is malformed", index))
			}
			name, nameOK, nameErr := unstructured.NestedString(component, "name")
			if nameErr != nil || !nameOK || name == "" {
				return domain.NewError(domain.ErrorPrecondition, "KubeBlocks pause", fmt.Sprintf("componentSpecs[%d] has no name", index))
			}
			if name != kb.Component {
				continue
			}
			componentFound = true
			current, _, stopErr := unstructured.NestedBool(component, "stop")
			if stopErr != nil {
				return domain.WrapError(domain.ErrorPrecondition, "KubeBlocks pause", fmt.Sprintf("read component %s stop state", name), stopErr)
			}
			original, known := kb.OriginalStops[name]
			if !known && paused && pauseOwner == "" {
				kb.OriginalStops[name] = current
				original = current
				known = true
			}
			if !known {
				return domain.NewError(domain.ErrorConflict, "KubeBlocks pause", fmt.Sprintf("Cluster component %s lacks original stop state", name))
			}
			expectedCurrent := original
			want := true
			if pauseOwner == session.ID {
				expectedCurrent = true
			}
			if !paused {
				want = original
			}
			if current != expectedCurrent {
				return domain.NewError(domain.ErrorConflict, "KubeBlocks pause", fmt.Sprintf("Cluster component %s stop changed from expected %t to %t", name, expectedCurrent, current))
			}
			if current != want {
				if err := unstructured.SetNestedField(component, want, "stop"); err != nil {
					return err
				}
				changed = true
			}
			components[index] = component
			break
		}
		if !componentFound {
			return domain.NewError(domain.ErrorConflict, "KubeBlocks pause", fmt.Sprintf("Cluster component %s was removed after discovery", kb.Component))
		}
		if paused && pauseOwner == "" {
			if annotations == nil {
				annotations = map[string]string{}
			}
			annotations[pauseSessionAnnotation] = session.ID
			cluster.SetAnnotations(annotations)
			changed = true
		}
		if !paused && pauseOwner == session.ID {
			delete(annotations, pauseSessionAnnotation)
			cluster.SetAnnotations(annotations)
			changed = true
		}
		if !changed {
			return nil
		}
		if err := unstructured.SetNestedField(cluster.Object, components, "spec", "componentSpecs"); err != nil {
			return err
		}
		_, updateErr := resource.Update(ctx, cluster, metav1.UpdateOptions{})
		if apierrors.IsConflict(updateErr) {
			return updateErr
		}
		if updateErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "KubeBlocks pause", "update Cluster component stop state", updateErr)
		}
		return nil
	})
}

func (m *Manager) setKubeBlocksInstanceSetPaused(ctx context.Context, session *domain.Session, paused bool) error {
	workload := session.Spec.Workload()
	kb := workload.KubeBlocks
	if kb == nil {
		return domain.NewError(domain.ErrorInternal, "InstanceSet pause", "session lacks KubeBlocks state")
	}
	if m.dynamic == nil {
		return domain.NewError(domain.ErrorPrecondition, "InstanceSet pause", "dynamic client is required for InstanceSet reconciliation control")
	}
	if session.ID == "" {
		return domain.NewError(domain.ErrorInternal, "InstanceSet pause", "session ID is required for InstanceSet pause ownership")
	}
	ref := workload.Controller
	gvr, err := kube.ParseGroupVersionResource(ref.APIVersion, instanceSetResource)
	if err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(ref.Namespace)
		object, getErr := resource.Get(ctx, ref.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(domain.ErrorKubernetes, "InstanceSet pause", "read InstanceSet", getErr)
		}
		if ref.UID != "" && object.GetUID() != "" && object.GetUID() != ref.UID {
			return domain.NewError(domain.ErrorConflict, "InstanceSet pause", fmt.Sprintf("InstanceSet %s/%s UID changed", ref.Namespace, ref.Name))
		}
		current, found, nestedErr := unstructured.NestedBool(object.Object, "spec", "paused")
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "InstanceSet pause", "read InstanceSet paused state", nestedErr)
		}
		if !found {
			current = false
		}
		annotations := object.GetAnnotations()
		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(domain.ErrorConflict, "InstanceSet pause", fmt.Sprintf("InstanceSet %s/%s pause is owned by session %s", ref.Namespace, ref.Name, pauseOwner))
		}
		switch {
		case pauseOwner == "":
			if current != kb.OriginalPaused {
				return domain.NewError(domain.ErrorConflict, "InstanceSet pause", fmt.Sprintf("InstanceSet %s/%s paused changed from expected %t to %t", ref.Namespace, ref.Name, kb.OriginalPaused, current))
			}
			if !paused {
				return nil
			}
		case paused && current:
			return nil
		case !paused && !current:
			return domain.NewError(domain.ErrorConflict, "InstanceSet resume", fmt.Sprintf("InstanceSet %s/%s paused state changed while session was active", ref.Namespace, ref.Name))
		case paused && !current:
			return domain.NewError(domain.ErrorConflict, "InstanceSet pause", fmt.Sprintf("InstanceSet %s/%s paused state changed while session was active", ref.Namespace, ref.Name))
		}
		want := paused
		if !paused {
			want = kb.OriginalPaused
		}
		changed := current != want || (!paused && found != kb.OriginalPausedConfigured)
		if changed {
			if !paused && !kb.OriginalPausedConfigured {
				unstructured.RemoveNestedField(object.Object, "spec", "paused")
			} else {
				if err := unstructured.SetNestedField(object.Object, want, "spec", "paused"); err != nil {
					return err
				}
			}
		}
		if paused {
			if annotations == nil {
				annotations = map[string]string{}
			}
			if annotations[pauseSessionAnnotation] != session.ID {
				annotations[pauseSessionAnnotation] = session.ID
				changed = true
			}
		} else if annotations[pauseSessionAnnotation] == session.ID {
			delete(annotations, pauseSessionAnnotation)
			changed = true
		}
		if !changed {
			return nil
		}
		object.SetAnnotations(annotations)
		updated, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{})
		if updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}
			return domain.WrapError(domain.ErrorKubernetes, "InstanceSet pause", "update InstanceSet paused state", updateErr)
		}
		actual, configured, nestedErr := unstructured.NestedBool(updated.Object, "spec", "paused")
		if nestedErr != nil {
			return domain.WrapError(domain.ErrorPrecondition, "InstanceSet pause", "verify updated InstanceSet paused state", nestedErr)
		}
		if paused && (!configured || !actual) {
			return domain.NewError(domain.ErrorPrecondition, "InstanceSet pause", fmt.Sprintf("InstanceSet %s/%s API did not preserve spec.paused", ref.Namespace, ref.Name))
		}
		return nil
	})
}

func (m *Manager) verifyKubeBlocksInstanceSetPaused(ctx context.Context, session *domain.Session) error {
	ref := session.Spec.Workload().Controller
	gvr, err := kube.ParseGroupVersionResource(ref.APIVersion, instanceSetResource)
	if err != nil {
		return err
	}
	object, err := m.dynamic.Resource(gvr).Namespace(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read InstanceSet", err)
	}
	if ref.UID != "" && object.GetUID() != "" && object.GetUID() != ref.UID {
		return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("InstanceSet %s/%s UID changed", ref.Namespace, ref.Name))
	}
	if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		return domain.NewError(domain.ErrorConflict, "verify paused", fmt.Sprintf("InstanceSet %s/%s pause ownership changed", ref.Namespace, ref.Name))
	}
	paused, found, nestedErr := unstructured.NestedBool(object.Object, "spec", "paused")
	if nestedErr != nil {
		return domain.WrapError(domain.ErrorPrecondition, "verify paused", "read InstanceSet paused state", nestedErr)
	}
	if !found || !paused {
		return domain.NewError(domain.ErrorPrecondition, "verify paused", fmt.Sprintf("InstanceSet %s/%s reconciliation is not paused", ref.Namespace, ref.Name))
	}
	return nil
}

func (m *Manager) deleteKubeBlocksInstancePod(ctx context.Context, session *domain.Session) error {
	ref := session.Spec.Workload().Pod
	pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "pause KubeBlocks", "read instance Pod", err)
	}
	if ref.UID != "" && pod.UID != ref.UID {
		return domain.NewError(domain.ErrorConflict, "pause KubeBlocks", fmt.Sprintf("Pod %s/%s UID changed", ref.Namespace, ref.Name))
	}
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}
	if err := m.typed.CoreV1().Pods(ref.Namespace).Delete(ctx, ref.Name, options); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "pause KubeBlocks", "delete instance Pod", err)
	}
	return kube.WaitFor(ctx, m.poll, fmt.Sprintf("KubeBlocks Pod %s/%s deletion", ref.Namespace, ref.Name), func(waitCtx context.Context) (bool, error) {
		_, getErr := m.typed.CoreV1().Pods(ref.Namespace).Get(waitCtx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return true, nil
		}
		return false, getErr
	})
}

func ignoreNotFound(err error) error {
	if err == nil || apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
