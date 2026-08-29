package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
)

func (m *Manager) kubeBlocksWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	state, err := m.prepareKubeBlocksDiscovery(ctx, pod, owner, options)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	instanceSet, err := m.discoverKubeBlocksInstanceSet(ctx, pod.Namespace, owner, pod.Name)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	role, roleIsLeader, err := resolveKubeBlocksRole(pod, owner, options, state.role, instanceSet)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	if state.switchoverCandidate != nil && !roleIsLeader {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"--kubeblocks-candidate applies only when the selected InstanceSet Pod has a leader role; Pod %s/%s has role %s, so omit the candidate",
				pod.Namespace,
				pod.Name,
				role,
			),
		)
	}

	if owner.Kind == domain.KindInstanceSet && roleIsLeader && state.switchoverCandidate == nil &&
		!options.AllowLeaderDowntime {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			m.kubeBlocksLeaderGuidance(
				ctx,
				pod,
				state.cluster,
				state.component,
				role,
				state.opsAPIVersion,
			),
		)
	}

	var switchoverStrategy domain.KubeBlocksSwitchoverStrategy

	switchoverContainer := ""
	if state.switchoverCandidate != nil {
		switchoverStrategy, switchoverContainer, err = m.resolveKubeBlocksSwitchover(
			ctx,
			pod,
			owner,
			state,
			instanceSet,
			roleIsLeader,
		)
		if err != nil {
			return domain.WorkloadSpec{}, err
		}
	}

	if owner.Kind == domain.KindInstanceSet && instanceSet.Paused {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"InstanceSet %s/%s is already paused; set spec.paused=false and wait for Pod %s/%s to become Ready before retrying",
				pod.Namespace,
				owner.Name,
				pod.Namespace,
				pod.Name,
			),
		)
	}

	controllerUID := owner.UID
	if owner.Kind == domain.KindInstanceSet {
		controllerUID = instanceSet.UID
	}

	return domain.WorkloadSpec{
		Adapter: domain.WorkloadKubeBlocks,
		Pod:     podReference(pod),
		Controller: objectReference(
			owner.APIVersion,
			owner.Kind,
			pod.Namespace,
			owner.Name,
			controllerUID,
			"",
		),
		KubeBlocks: &domain.KubeBlocksSpec{
			Cluster:                  state.cluster,
			Component:                state.component,
			Instance:                 pod.Name,
			Role:                     role,
			SwitchoverCandidate:      options.SwitchoverCandidate,
			SwitchoverStrategy:       switchoverStrategy,
			SwitchoverContainer:      switchoverContainer,
			OpsAPIVersion:            state.opsAPIVersion,
			ClusterUID:               state.clusterObject.GetUID(),
			LegacyOriginalReplicas:   state.originalReplicas,
			OriginalPaused:           instanceSet.Paused,
			OriginalPausedConfigured: instanceSet.PausedConfigured,
		},
	}, nil
}

type kubeBlocksDiscoveryState struct {
	cluster             string
	component           string
	role                string
	opsAPIVersion       string
	switchoverCandidate *corev1.Pod
	clusterObject       *unstructured.Unstructured
	originalReplicas    int32
}

func (m *Manager) prepareKubeBlocksDiscovery(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
) (kubeBlocksDiscoveryState, error) {
	state := kubeBlocksDiscoveryState{
		cluster:   pod.Labels[kube.AppInstanceLabel],
		component: kubeBlocksComponent(pod),
		role:      podRole(pod),
	}
	if state.cluster == "" || state.component == "" {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"Pod lacks cluster or component identity labels",
		)
	}

	if reason := unsupportedKubeBlocksReason(pod, nil, state.component); reason != "" {
		return state, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", reason)
	}

	if err := validateKubeBlocksSwitchoverCandidate(
		owner,
		options.SwitchoverCandidate,
	); err != nil {
		return state, err
	}

	state.opsAPIVersion = servedKubeBlocksOpsAPIVersion(m.discovery)
	if state.opsAPIVersion == "" {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"no served OpsRequest API was found",
		)
	}

	candidate, err := m.discoverKubeBlocksCandidate(ctx, pod, owner, options, state)
	if err != nil {
		return state, err
	}

	state.switchoverCandidate = candidate
	if m.dynamic == nil {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"dynamic client is required for Cluster pause control",
		)
	}

	if owner.Kind == domain.KindInstanceSet && isLeaderRole(state.role) &&
		!options.AllowLeaderDowntime && candidate == nil {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			m.kubeBlocksLeaderGuidance(
				ctx,
				pod,
				state.cluster,
				state.component,
				state.role,
				state.opsAPIVersion,
			),
		)
	}

	clusterObject, components, err := m.loadKubeBlocksCluster(ctx, pod, state.cluster)
	if err != nil {
		return state, err
	}

	state.clusterObject = clusterObject

	replicas, err := parseKubeBlocksReplicas(components, state.component)
	if err != nil {
		return state, err
	}

	state.originalReplicas = replicas

	return state, nil
}

func validateKubeBlocksSwitchoverCandidate(
	owner *metav1.OwnerReference,
	candidate string,
) error {
	if owner.Kind == domain.KindInstanceSet || candidate == "" {
		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"discover KubeBlocks",
		"--kubeblocks-candidate is supported only for InstanceSet-backed KubeBlocks components; Stop OpsRequests pause the complete legacy Cluster or component",
	)
}

func servedKubeBlocksOpsAPIVersion(discovery discovery.DiscoveryInterface) string {
	for _, candidate := range []string{kubeBlocksClusterAPIVersion, kubeBlocksOpsAPIVersion} {
		if kube.HasAPIResource(discovery, candidate, "opsrequests") {
			return candidate
		}
	}

	return ""
}

func (m *Manager) discoverKubeBlocksCandidate(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
	state kubeBlocksDiscoveryState,
) (*corev1.Pod, error) {
	if options.SwitchoverCandidate == "" {
		return nil, nil
	}

	if options.SwitchoverCandidate == pod.Name {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"--kubeblocks-candidate %s refers to the selected source Pod; choose a different Ready non-leader Pod in cluster %s component %s%s",
				pod.Name,
				state.cluster,
				state.component,
				m.kubeBlocksCandidateSuggestion(ctx, pod, state.cluster, state.component),
			),
		)
	}

	candidate, err := m.typed.CoreV1().Pods(pod.Namespace).Get(
		ctx,
		options.SwitchoverCandidate,
		metav1.GetOptions{},
	)
	if err != nil {
		message := fmt.Sprintf(
			"read switchover candidate Pod %s/%s: %v",
			pod.Namespace,
			options.SwitchoverCandidate,
			err,
		)
		if apierrors.IsNotFound(err) {
			message = fmt.Sprintf(
				"switchover candidate Pod %s/%s does not exist; verify --kubeblocks-candidate%s",
				pod.Namespace,
				options.SwitchoverCandidate,
				m.kubeBlocksCandidateSuggestion(ctx, pod, state.cluster, state.component),
			)
		}

		return nil, domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", message, err)
	}

	candidateCluster := candidate.Labels[kube.AppInstanceLabel]

	candidateComponent := kubeBlocksComponent(candidate)
	if candidateCluster != state.cluster || candidateComponent != state.component {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"switchover candidate Pod %s/%s belongs to cluster %s component %s; expected cluster %s component %s%s",
				pod.Namespace,
				candidate.Name,
				candidateCluster,
				candidateComponent,
				state.cluster,
				state.component,
				m.kubeBlocksCandidateSuggestion(ctx, pod, state.cluster, state.component),
			),
		)
	}

	if err := validatePodController(
		candidate,
		objectReference(owner.APIVersion, owner.Kind, pod.Namespace, owner.Name, owner.UID, ""),
		"discover KubeBlocks",
	); err != nil {
		return nil, err
	}

	if !kube.PodReady(candidate) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"switchover candidate Pod %s/%s must be Running and Ready%s",
				pod.Namespace,
				candidate.Name,
				m.kubeBlocksCandidateSuggestion(ctx, pod, state.cluster, state.component),
			),
		)
	}

	return candidate, nil
}

func (m *Manager) loadKubeBlocksCluster(
	ctx context.Context,
	pod *corev1.Pod,
	cluster string,
) (*unstructured.Unstructured, []any, error) {
	clusterGVR, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return nil, nil, err
	}

	clusterObject, err := m.dynamic.Resource(clusterGVR).Namespace(pod.Namespace).Get(
		ctx,
		cluster,
		metav1.GetOptions{},
	)
	if err != nil {
		return nil, nil, domain.WrapError(
			domain.ErrorKubernetes,
			"discover KubeBlocks",
			"read Cluster",
			err,
		)
	}

	if clusterObject.GetUID() == "" {
		return nil, nil, domain.NewError(
			domain.ErrorKubernetes,
			"discover KubeBlocks",
			fmt.Sprintf("Cluster %s/%s has an incomplete identity", pod.Namespace, cluster),
		)
	}

	components, ok, err := unstructured.NestedSlice(clusterObject.Object, "spec", "componentSpecs")
	if err != nil || !ok || len(components) == 0 {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"Cluster has no componentSpecs",
		)
	}

	if reason := unsupportedKubeBlocksReason(
		pod,
		components,
		kubeBlocksComponent(pod),
	); reason != "" {
		return nil, nil, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", reason)
	}

	return clusterObject, components, nil
}

func parseKubeBlocksReplicas(components []any, selected string) (int32, error) {
	found := false

	var replicas int32 = 1
	for index := range components {
		componentSpec, ok := components[index].(map[string]any)
		if !ok {
			return 0, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("componentSpecs[%d] is malformed", index),
			)
		}

		name, nameOK, err := unstructured.NestedString(componentSpec, "name")
		if err != nil || !nameOK || name == "" {
			return 0, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("componentSpecs[%d] has no name", index),
			)
		}

		if _, _, err := unstructured.NestedString(componentSpec, "componentDefRef"); err != nil {
			return 0, domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("read component %s definition", name),
				err,
			)
		}

		value, replicasFound, err := unstructured.NestedInt64(componentSpec, "replicas")
		if err != nil {
			return 0, domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("read component %s replicas", name),
				err,
			)
		}

		if name == selected {
			if replicasFound {
				if value <= 0 {
					return 0, domain.NewError(
						domain.ErrorPrecondition,
						"discover KubeBlocks",
						fmt.Sprintf("component %s must have positive replicas", name),
					)
				}

				if value > int64(1<<31-1) {
					return 0, domain.NewError(
						domain.ErrorPrecondition,
						"discover KubeBlocks",
						fmt.Sprintf("component %s replicas exceed int32 range", name),
					)
				}

				replicas = int32(value)
			}

			found = true
		}
	}

	if !found {
		return 0, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"Cluster componentSpecs has no component "+selected,
		)
	}

	return replicas, nil
}

func resolveKubeBlocksRole(
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
	role string,
	instanceSet kubeBlocksInstanceSetState,
) (string, bool, error) {
	if role != "" && instanceSet.Role != "" && !strings.EqualFold(role, instanceSet.Role) {
		return "", false, domain.NewError(
			domain.ErrorConflict,
			"discover KubeBlocks",
			fmt.Sprintf(
				"selected instance role changed from %s to %s during discovery; rerun the plan",
				role,
				instanceSet.Role,
			),
		)
	}

	if role == "" {
		role = instanceSet.Role
	}

	roleIsLeader := isLeaderRole(role)
	if !instanceSet.HasLeaderRole {
		return role, roleIsLeader, nil
	}

	if role == "" {
		if !options.AllowLeaderDowntime {
			return "", false, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf(
					"selected instance %s role is unavailable while InstanceSet %s declares leader roles; wait for the KubeBlocks role probe to recover, or use --allow-leader-downtime to acknowledge a possible leader outage",
					pod.Name,
					owner.Name,
				),
			)
		}

		return "unknown", false, nil
	}

	if knownLeader, knownRole := instanceSet.LeaderRoles[strings.ToLower(role)]; knownRole {
		return role, knownLeader, nil
	}

	if !options.AllowLeaderDowntime {
		return "", false, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"selected instance %s reports role %s, which is absent from InstanceSet %s role definitions; wait for role status to converge, or use --allow-leader-downtime",
				pod.Name,
				role,
				owner.Name,
			),
		)
	}

	return role, roleIsLeader, nil
}

func (m *Manager) resolveKubeBlocksSwitchover(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	state kubeBlocksDiscoveryState,
	instanceSet kubeBlocksInstanceSetState,
	roleIsLeader bool,
) (domain.KubeBlocksSwitchoverStrategy, string, error) {
	if roleIsLeader && instanceSet.HasLeaderRole {
		candidateRole := podRole(state.switchoverCandidate)

		candidateIsLeader, knownRole := instanceSet.LeaderRoles[strings.ToLower(candidateRole)]
		if !knownRole || candidateIsLeader {
			return "", "", domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf(
					"switchover candidate %s must have a known non-leader role in InstanceSet %s",
					state.switchoverCandidate.Name,
					owner.Name,
				),
			)
		}
	}

	strategy, container, err := m.kubeBlocksSwitchoverStrategy(
		ctx,
		pod,
		state.cluster,
		state.component,
		state.switchoverCandidate.Name,
		state.opsAPIVersion,
	)
	if err != nil {
		return "", "", domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"automatic switchover for selected instance %s is unavailable: %v; use --allow-leader-downtime to acknowledge the leader outage",
				pod.Name,
				err,
			),
		)
	}

	return strategy, container, nil
}

func (m *Manager) discoverKubeBlocksInstanceSet(
	ctx context.Context,
	namespace string,
	owner *metav1.OwnerReference,
	podName string,
) (kubeBlocksInstanceSetState, error) {
	state := kubeBlocksInstanceSetState{}
	if owner.Kind != domain.KindInstanceSet {
		return state, nil
	}

	if m.dynamic == nil {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"dynamic client is required for InstanceSet reconciliation control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(owner.APIVersion, instanceSetResource)
	if err != nil {
		return state, err
	}

	resource := m.dynamic.Resource(gvr).Namespace(namespace)

	object, err := resource.Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return state, domain.WrapError(
			domain.ErrorKubernetes,
			"discover KubeBlocks",
			"read InstanceSet",
			err,
		)
	}

	if object.GetUID() == "" || object.GetUID() != owner.UID {
		return state, domain.NewError(
			domain.ErrorConflict,
			"discover KubeBlocks",
			fmt.Sprintf("InstanceSet %s/%s UID changed", namespace, owner.Name),
		)
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
		return state, domain.WrapError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"read InstanceSet paused state",
			err,
		)
	}

	state.Paused = paused

	state.PausedConfigured = found
	if !found {
		probe := object.DeepCopy()
		if err := unstructured.SetNestedField(probe.Object, true, "spec", "paused"); err != nil {
			return state, err
		}

		result, updateErr := resource.Update(
			ctx,
			probe,
			metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}},
		)
		if updateErr != nil {
			return state, domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				"probe InstanceSet spec.paused support",
				updateErr,
			)
		}

		if _, supported, nestedErr := unstructured.NestedBool(
			result.Object,
			"spec",
			"paused",
		); nestedErr != nil ||
			!supported {
			return state, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf(
					"InstanceSet %s/%s does not support spec.paused",
					namespace,
					owner.Name,
				),
			)
		}
	}

	return state, nil
}

func kubeBlocksLeaderRoles(instanceSet *unstructured.Unstructured) (map[string]bool, bool, error) {
	roles, found, err := unstructured.NestedSlice(instanceSet.Object, "spec", "roles")
	if err != nil {
		return nil, false, domain.WrapError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"read InstanceSet role definitions",
			err,
		)
	}

	if !found || len(roles) == 0 {
		return nil, false, nil
	}

	result := make(map[string]bool, len(roles))

	hasLeader := false
	for index, value := range roles {
		role, ok := value.(map[string]any)
		if !ok {
			return nil, false, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("InstanceSet spec.roles[%d] is malformed", index),
			)
		}

		name, _, nameErr := unstructured.NestedString(role, "name")

		leader, _, leaderErr := unstructured.NestedBool(role, "isLeader")
		if nameErr != nil || leaderErr != nil || name == "" {
			return nil, false, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("InstanceSet spec.roles[%d] has invalid role identity", index),
			)
		}

		result[strings.ToLower(name)] = leader
		hasLeader = hasLeader || leader
	}

	return result, hasLeader, nil
}

func kubeBlocksMemberRole(instanceSet *unstructured.Unstructured, podName string) (string, error) {
	members, found, err := unstructured.NestedSlice(instanceSet.Object, "status", "membersStatus")
	if err != nil {
		return "", domain.WrapError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"read InstanceSet member roles",
			err,
		)
	}

	if !found {
		return "", nil
	}

	for index, value := range members {
		member, ok := value.(map[string]any)
		if !ok {
			return "", domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("InstanceSet status.membersStatus[%d] is malformed", index),
			)
		}

		name, _, nameErr := unstructured.NestedString(member, "podName")
		if nameErr != nil {
			return "", domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("read InstanceSet member %d Pod name", index),
				nameErr,
			)
		}

		if name != podName {
			continue
		}

		role, _, roleErr := unstructured.NestedString(member, "role", "name")
		if roleErr != nil {
			return "", domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("read InstanceSet member %s role", podName),
				roleErr,
			)
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

func (m *Manager) kubeBlocksLeaderGuidance(
	ctx context.Context,
	selected *corev1.Pod,
	cluster, component, role, opsAPIVersion string,
) string {
	if isKubeBlocksRedis(selected) {
		return fmt.Sprintf(
			"selected instance %s has role %s; the KubeBlocks Redis addon does not provide a Switchover action. Rerun without --kubeblocks-candidate and use --allow-leader-downtime to acknowledge the leader outage",
			selected.Name,
			role,
		)
	}

	candidate := m.readyKubeBlocksCandidate(ctx, selected, cluster, component)
	if candidate == "" {
		candidate = "REPLACE_WITH_READY_SECONDARY_POD"
	}

	if candidate != "REPLACE_WITH_READY_SECONDARY_POD" {
		if isKubeBlocksMongoDB(selected) {
			if _, err := m.preflightMongoDBNativeSwitchover(ctx, selected); err != nil {
				return fmt.Sprintf(
					"selected instance %s has role %s; MongoDB has no KubeBlocks Switchover OpsRequest handler and native switchover preflight failed: %v. Use --allow-leader-downtime to acknowledge the leader outage. Manual MongoDB switchover: %s",
					selected.Name,
					role,
					err,
					kubeBlocksMongoDBNativeSwitchoverCommand(
						selected.Namespace,
						cluster,
						component,
						selected.Name,
						candidate,
					),
				)
			}

			return fmt.Sprintf(
				"selected instance %s has role %s; MongoDB does not support KubeBlocks Switchover OpsRequests. Use --kubeblocks-candidate %s for the validated native replica-set switchover, or use --allow-leader-downtime to acknowledge the leader outage. Manual MongoDB switchover: %s",
				selected.Name,
				role,
				candidate,
				kubeBlocksMongoDBNativeSwitchoverCommand(
					selected.Namespace,
					cluster,
					component,
					selected.Name,
					candidate,
				),
			)
		}

		if err := m.validateKubeBlocksSwitchover(
			ctx,
			selected.Namespace,
			cluster,
			component,
			selected.Name,
			candidate,
			opsAPIVersion,
		); err != nil {
			return fmt.Sprintf(
				"selected instance %s has role %s; the served OpsRequest API rejected automatic switchover to %s: %v. Use the component's native switchover procedure, or use --allow-leader-downtime to acknowledge the leader outage",
				selected.Name,
				role,
				candidate,
				err,
			)
		}
	}

	return fmt.Sprintf(
		"selected instance %s has role %s; use --kubeblocks-candidate %s for an automatic switchover, or complete a native switchover first and rerun the plan. Use --allow-leader-downtime to acknowledge the leader outage. KubeBlocks commands: kbcli cluster promote %s --namespace %s --instance %s --candidate %s; or %s",
		selected.Name,
		role,
		candidate,
		cluster,
		selected.Namespace,
		selected.Name,
		candidate,
		kubeBlocksSwitchoverCommand(
			selected.Namespace,
			cluster,
			component,
			selected.Name,
			candidate,
			opsAPIVersion,
		),
	)
}

func (m *Manager) readyKubeBlocksCandidate(
	ctx context.Context,
	selected *corev1.Pod,
	cluster, component string,
) string {
	pods, err := m.typed.CoreV1().Pods(selected.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil || pods == nil {
		return ""
	}

	candidates := make([]string, 0, len(pods.Items))

	selectedOwner := controllerOwner(selected.OwnerReferences)
	for index := range pods.Items {
		candidate := &pods.Items[index]
		if candidate.Name == selected.Name || candidate.Labels[kube.AppInstanceLabel] != cluster ||
			kubeBlocksComponent(candidate) != component ||
			!kube.PodReady(candidate) ||
			isLeaderRole(podRole(candidate)) ||
			!sameControllerOwner(controllerOwner(candidate.OwnerReferences), selectedOwner) {
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

func (m *Manager) kubeBlocksCandidateSuggestion(
	ctx context.Context,
	selected *corev1.Pod,
	cluster, component string,
) string {
	candidate := m.readyKubeBlocksCandidate(ctx, selected, cluster, component)
	if candidate == "" {
		return ""
	}

	return "; available Ready non-leader candidate: --kubeblocks-candidate " + candidate
}

func kubeBlocksSwitchoverCommand(
	namespace, cluster, component, selected, candidate, opsAPIVersion string,
) string {
	var builder strings.Builder

	clusterField := "clusterRef"
	if strings.HasPrefix(opsAPIVersion, "operations.kubeblocks.io/") {
		clusterField = "clusterName"
	}

	fmt.Fprintf(
		&builder,
		"kubectl create -f - <<'YAML'\napiVersion: %s\nkind: OpsRequest\nmetadata:\n  generateName: %s-switchover-\n  namespace: %s\nspec:\n  %s: %s\n  type: Switchover\n  switchover:\n  - componentName: %s\n",
		opsAPIVersion,
		cluster,
		namespace,
		clusterField,
		cluster,
		component,
	)

	if strings.HasPrefix(opsAPIVersion, "operations.kubeblocks.io/") {
		fmt.Fprintf(&builder, "    instanceName: %s\n    candidateName: %s\n", selected, candidate)
	} else {
		fmt.Fprintf(&builder, "    instanceName: %s\n", candidate)
	}

	builder.WriteString("YAML")

	return builder.String()
}

func (m *Manager) validateKubeBlocksSwitchover(
	ctx context.Context,
	namespace, cluster, component, selected, candidate, opsAPIVersion string,
) error {
	if m.logger != nil {
		m.logger.Info(
			"checking KubeBlocks automatic switchover",
			"namespace",
			namespace,
			"cluster",
			cluster,
			"workload_component",
			component,
			"instance",
			selected,
			"candidate",
			candidate,
		)
	}

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
	_, err = m.dynamic.Resource(gvr).
		Namespace(namespace).
		Create(ctx, object, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})

	return err
}

func (m *Manager) kubeBlocksSwitchoverStrategy(
	ctx context.Context,
	selected *corev1.Pod,
	cluster, component, candidate, opsAPIVersion string,
) (domain.KubeBlocksSwitchoverStrategy, string, error) {
	if isKubeBlocksRedis(selected) {
		return "", "", errors.New(
			"the KubeBlocks Redis addon does not provide a Switchover action; omit --kubeblocks-candidate",
		)
	}

	if isKubeBlocksMongoDB(selected) {
		container, err := m.preflightMongoDBNativeSwitchover(ctx, selected)
		if err != nil {
			return "", "", fmt.Errorf(
				"MongoDB has no KubeBlocks Switchover OpsRequest handler; native switchover preflight failed: %w",
				err,
			)
		}

		return domain.KubeBlocksSwitchoverMongoDBNative, container, nil
	}

	err := m.validateKubeBlocksSwitchover(
		ctx,
		selected.Namespace,
		cluster,
		component,
		selected.Name,
		candidate,
		opsAPIVersion,
	)
	if err == nil {
		return domain.KubeBlocksSwitchoverOpsRequest, "", nil
	}

	return "", "", fmt.Errorf(
		"the served OpsRequest API rejected the request: %w; use the component's native switchover procedure",
		err,
	)
}

func isKubeBlocksMongoDB(pod *corev1.Pod) bool {
	return isKubeBlocksApplication(pod, "mongodb")
}

func isKubeBlocksRedis(pod *corev1.Pod) bool {
	return isKubeBlocksApplication(pod, "redis")
}

func isKubeBlocksApplication(pod *corev1.Pod, name string) bool {
	if pod == nil {
		return false
	}

	for _, value := range []string{pod.Labels[kube.AppNameLabel], pod.Labels[kube.AppComponentLabel], pod.Labels[kubeBlocksComponentLabel]} {
		if strings.EqualFold(value, name) {
			return true
		}
	}

	return false
}

func mongoDBContainer(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}

	for _, container := range pod.Spec.Containers {
		if container.Name == "mongodb" {
			return container.Name
		}
	}

	return ""
}

func (m *Manager) preflightMongoDBNativeSwitchover(
	ctx context.Context,
	pod *corev1.Pod,
) (string, error) {
	container := mongoDBContainer(pod)
	if container == "" {
		return "", fmt.Errorf("MongoDB Pod %s has no mongodb container", pod.Name)
	}

	if m.commandExecutor == nil {
		return "", errors.New(
			"pod exec is unavailable; configure Kubernetes REST access for the MongoDB native switchover",
		)
	}

	if m.logger != nil {
		m.logger.Info(
			"checking MongoDB native switchover prerequisites",
			"namespace",
			pod.Namespace,
			"pod",
			pod.Name,
			"container",
			container,
		)
	}

	result, err := m.commandExecutor.Execute(ctx, podCommandRequest{
		Namespace: pod.Namespace,
		Pod:       pod.Name,
		Container: container,
		Command:   []string{"sh", "-ceu", mongoDBNativeSwitchoverPreflight},
	})
	if err != nil {
		return "", podCommandError("check MongoDB native switchover prerequisites", result, err)
	}

	return container, nil
}

func podCommandError(action string, result podCommandResult, err error) error {
	output := strings.TrimSpace(result.Stderr)
	if output == "" {
		output = strings.TrimSpace(result.Stdout)
	}

	if output == "" {
		return fmt.Errorf("%s: %w", action, err)
	}

	output = strings.Join(strings.Fields(output), " ")
	if len(output) > 512 {
		output = output[:512] + "..."
	}

	return fmt.Errorf("%s: %w (output: %s)", action, err, output)
}

func kubeBlocksSwitchoverSpec(
	opsAPIVersion, cluster, component, selected, candidate string,
) map[string]any {
	switchover := map[string]any{"componentName": component, "instanceName": candidate}
	if strings.HasPrefix(opsAPIVersion, "operations.kubeblocks.io/") {
		switchover["instanceName"] = selected
		switchover["candidateName"] = candidate
	}

	clusterField := "clusterRef"
	if strings.HasPrefix(opsAPIVersion, "operations.kubeblocks.io/") {
		clusterField = "clusterName"
	}

	return map[string]any{
		clusterField: cluster,
		"type":       "Switchover",
		"switchover": []any{switchover},
	}
}

func unsupportedKubeBlocksReason(
	pod *corev1.Pod,
	components []any,
	selectedComponent string,
) string {
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
			return fmt.Sprintf(
				"KubeBlocks MinIO component %s requires MinIO's native drive or pool maintenance",
				selectedComponent,
			)
		case strings.Contains(lower, "cockroach"), strings.Contains(lower, "crdb"):
			return fmt.Sprintf(
				"KubeBlocks CockroachDB component %s requires CockroachDB drain and decommission",
				selectedComponent,
			)
		case strings.Contains(lower, "archive-wal"),
			lower == "wal",
			strings.Contains(lower, "wal-tool"):
			return fmt.Sprintf(
				"KubeBlocks archive-WAL component %s is a backup workload and cannot be migrated",
				selectedComponent,
			)
		}
	}

	return ""
}
