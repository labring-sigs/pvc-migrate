package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

func (m *Manager) verifyKubeBlocksPaused(
	ctx context.Context,
	session *domain.Session,
	workload domain.WorkloadSpec,
) error {
	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"dynamic client is required for KubeBlocks pause verification",
		)
	}

	kb := workload.KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"session lacks KubeBlocks state",
		)
	}

	if workload.Controller.Kind == domain.KindInstanceSet {
		return m.verifyKubeBlocksInstanceSetPaused(ctx, session)
	}

	if kb.ClusterUID == "" || kb.Component == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"session lacks KubeBlocks Cluster identity",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(workload.Pod.Namespace).
		Get(ctx, kb.Cluster, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify paused",
			"read KubeBlocks Cluster",
			err,
		)
	}

	if object.GetUID() != kb.ClusterUID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"KubeBlocks Cluster %s/%s UID changed",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"KubeBlocks Cluster %s/%s pause ownership changed",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	if err := m.verifyKubeBlocksPauseOperation(ctx, session, object); err != nil {
		return err
	}

	components, ok, nestedErr := unstructured.NestedSlice(
		object.Object,
		"spec",
		kubeBlocksFieldComponentSpecs,
	)
	if nestedErr != nil || !ok {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"KubeBlocks Cluster has no componentSpecs",
		)
	}

	for index := range components {
		component, ok := components[index].(map[string]any)
		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"verify paused",
				fmt.Sprintf("KubeBlocks componentSpecs[%d] is malformed", index),
			)
		}

		name, _, _ := unstructured.NestedString(component, "name")
		if name != kb.Component {
			continue
		}

		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"verify paused",
		"KubeBlocks Cluster has no component "+kb.Component,
	)
}

func (m *Manager) verifyKubeBlocksPauseOperation(
	ctx context.Context,
	session *domain.Session,
	cluster *unstructured.Unstructured,
) error {
	kb := session.Spec.Workload().KubeBlocks

	gvr, err := opsGVR(kb.OpsAPIVersion)
	if err != nil {
		return err
	}

	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
	currentName := kubeBlocksOperationName(session, "pause")
	names := []string{currentName}

	initialName := operationName(session.ID, "pause")
	if initialName != currentName {
		names = append(names, initialName)
	}

	states := make([]string, 0, len(names))
	for _, name := range names {
		request, readErr := resource.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(readErr) {
			states = append(states, name+"=missing")
			continue
		}

		if readErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"verify paused",
				"read KubeBlocks pause OpsRequest "+name,
				readErr,
			)
		}

		if _, err := validateKubeBlocksOpsRequest(
			request,
			name,
			session.ID,
			kubeBlocksPauseSpec(kb, true),
		); err != nil {
			return err
		}

		phase, phaseFound, phaseErr := unstructured.NestedString(
			request.Object,
			"status",
			"phase",
		)
		if phaseErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"verify paused",
				"read KubeBlocks pause OpsRequest phase",
				phaseErr,
			)
		}

		if phaseFound && kubeBlocksPhase(phase) == kubeBlocksPhaseSucceeded {
			return requireKubeBlocksStopped(cluster, kb)
		}

		states = append(states, fmt.Sprintf("%s=%s", name, phase))
		if !kubeBlocksOpsFailed(phase) {
			break
		}
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"verify paused",
		"no successful KubeBlocks pause OpsRequest ("+strings.Join(states, ", ")+")",
	)
}

func requireKubeBlocksStopped(
	cluster *unstructured.Unstructured,
	kb *domain.KubeBlocksSpec,
) error {
	stopped, phase, err := kubeBlocksStopped(cluster, kb)
	if err != nil {
		return err
	}

	if stopped {
		return nil
	}

	if usesComponentScopedKubeBlocksOps(kb.OpsAPIVersion) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf(
				"KubeBlocks component %s phase is %s, expected Stopped",
				kb.Component,
				phase,
			),
		)
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"verify paused",
		fmt.Sprintf("KubeBlocks Cluster phase is %s, expected Stopped", phase),
	)
}

func kubeBlocksStopped(
	cluster *unstructured.Unstructured,
	kb *domain.KubeBlocksSpec,
) (bool, string, error) {
	path := []string{"status", "phase"}

	stateName := "Cluster"
	if usesComponentScopedKubeBlocksOps(kb.OpsAPIVersion) {
		path = []string{"status", kubeBlocksFieldComponents, kb.Component, "phase"}
		stateName = "component"
	}

	phase, found, err := unstructured.NestedString(cluster.Object, path...)
	if err != nil {
		return false, "", domain.WrapError(
			domain.ErrorPrecondition,
			"verify paused",
			"read KubeBlocks "+stateName+" phase",
			err,
		)
	}

	return found && kubeBlocksPhase(phase) == kubeBlocksPhaseStopped, phase, nil
}

func kubeBlocksOpsSpecEqual(actual, expected any) bool {
	return apiequality.Semantic.DeepEqual(
		normalizeKubeBlocksOpsSpec(actual),
		normalizeKubeBlocksOpsSpec(expected),
	)
}

func normalizeKubeBlocksOpsSpec(value any) any {
	spec, ok := value.(map[string]any)
	if !ok {
		return value
	}

	normalized := maps.Clone(spec)
	if clusterRef, ok := normalized[kubeBlocksFieldClusterRef].(string); ok {
		if clusterName, ok := normalized[kubeBlocksFieldClusterName].(string); ok &&
			clusterName == clusterRef {
			delete(normalized, kubeBlocksFieldClusterName)
		}
	}

	if clusterName, ok := normalized[kubeBlocksFieldClusterName].(string); ok {
		if clusterRef, ok := normalized[kubeBlocksFieldClusterRef].(string); ok &&
			clusterRef == clusterName {
			delete(normalized, kubeBlocksFieldClusterRef)
		}
	}

	if enqueue, ok := normalized["enqueueOnForce"].(bool); ok && !enqueue {
		delete(normalized, "enqueueOnForce")
	}

	if deadline, ok := normalized["preConditionDeadlineSeconds"].(int64); ok && deadline == 0 {
		delete(normalized, "preConditionDeadlineSeconds")
	}

	// KubeBlocks 0.8.x defaults this field on every OpsRequest. It is an
	// admission default, not part of the operation identity we submit.
	if ttl, ok := normalized["ttlSecondsBeforeAbort"].(int64); ok && ttl == 0 {
		delete(normalized, "ttlSecondsBeforeAbort")
	}

	return normalized
}

func opsGVR(apiVersion string) (schema.GroupVersionResource, error) {
	return kube.ParseGroupVersionResource(apiVersion, "opsrequests")
}

func (m *Manager) createAndWaitOps(
	ctx context.Context,
	session *domain.Session,
	action string,
	spec map[string]any,
) error {
	kb := session.Spec.Workload().KubeBlocks

	gvr, err := opsGVR(kb.OpsAPIVersion)
	if err != nil {
		return err
	}

	name := kubeBlocksOperationName(session, action)
	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
	existing, getErr := resource.Get(ctx, name, metav1.GetOptions{})
	create := apierrors.IsNotFound(getErr)

	var expectedUID types.UID
	if getErr == nil {
		expectedUID, err = validateKubeBlocksOpsRequest(existing, name, session.ID, spec)
		if err != nil {
			return err
		}

		phase, _, _ := unstructured.NestedString(existing.Object, "status", "phase")
		if kubeBlocksOpsFailed(phase) {
			uid := existing.GetUID()
			if err := resource.Delete(
				ctx,
				name,
				metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
			); err != nil &&
				!apierrors.IsNotFound(err) {
				return domain.WrapError(
					domain.ErrorKubernetes,
					"KubeBlocks operation",
					"delete failed OpsRequest "+name,
					err,
				)
			}

			if err := m.waitFor(
				ctx,
				fmt.Sprintf("failed OpsRequest %s deletion", name),
				func(waitCtx context.Context) (bool, error) {
					current, err := resource.Get(waitCtx, name, metav1.GetOptions{})
					if apierrors.IsNotFound(err) {
						return true, nil
					}

					if err == nil && current.GetUID() != uid {
						return false, domain.NewError(
							domain.ErrorConflict,
							"KubeBlocks operation",
							fmt.Sprintf(
								"OpsRequest %s was replaced while waiting for deletion",
								name,
							),
						)
					}

					return false, err
				},
			); err != nil {
				return err
			}

			create = true
			expectedUID = ""
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
		created, createErr := resource.Create(ctx, object, metav1.CreateOptions{})

		if apierrors.IsAlreadyExists(createErr) {
			existing, err = resource.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return domain.WrapError(
					domain.ErrorKubernetes,
					"KubeBlocks operation",
					"read concurrently created OpsRequest "+name,
					err,
				)
			}

			expectedUID, err = validateKubeBlocksOpsRequest(existing, name, session.ID, spec)
			if err != nil {
				return err
			}
		} else {
			if createErr != nil {
				return domain.WrapError(
					domain.ErrorKubernetes,
					"KubeBlocks operation",
					"create OpsRequest "+name,
					createErr,
				)
			}

			if created == nil || created.GetName() == "" || created.GetUID() == "" {
				return domain.NewError(
					domain.ErrorKubernetes,
					"KubeBlocks operation",
					fmt.Sprintf("create OpsRequest %s returned an empty object", name),
				)
			}

			expectedUID = created.GetUID()
		}
	} else if getErr != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"KubeBlocks operation",
			"read OpsRequest "+name,
			getErr,
		)
	}

	return m.waitFor(
		ctx,
		"KubeBlocks OpsRequest "+name,
		func(waitCtx context.Context) (bool, error) {
			current, readErr := resource.Get(waitCtx, name, metav1.GetOptions{})
			if readErr != nil {
				return false, domain.WrapError(
					domain.ErrorKubernetes,
					"KubeBlocks operation",
					"read OpsRequest status",
					readErr,
				)
			}

			labels := current.GetLabels()
			if labels[kube.ManagedByLabel] != kube.ManagedByValue ||
				labels[kube.SessionKey] != session.ID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"KubeBlocks operation",
					fmt.Sprintf("OpsRequest %s ownership changed while waiting", name),
				)
			}

			if current.GetUID() != expectedUID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"KubeBlocks operation",
					fmt.Sprintf("OpsRequest %s was replaced while waiting", name),
				)
			}

			phase, _, _ := unstructured.NestedString(current.Object, "status", "phase")
			switch kubeBlocksPhase(phase) {
			case kubeBlocksPhaseSucceeded:
				return true, nil
			case kubeBlocksPhaseFailed, kubeBlocksPhaseCancelled, kubeBlocksPhaseAborted:
				return false, domain.NewError(
					domain.ErrorPrecondition,
					"KubeBlocks operation",
					fmt.Sprintf("OpsRequest %s ended in phase %s", name, phase),
				)
			default:
				return false, nil
			}
		},
	)
}

func kubeBlocksOperationName(session *domain.Session, action string) string {
	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhaseRollingBack:
		action = "rollback-" + action
	case domain.PhaseAborting:
		action = "abort-" + action
	}

	return operationName(session.ID, action)
}

func operationName(sessionID, action string) string {
	return kube.BoundedName("pvc-migrate", sessionID, action)
}

func validateKubeBlocksOpsRequest(
	existing *unstructured.Unstructured,
	name string,
	sessionID string,
	spec map[string]any,
) (types.UID, error) {
	labels := existing.GetLabels()
	if labels[kube.ManagedByLabel] != kube.ManagedByValue || labels[kube.SessionKey] != sessionID {
		return "", domain.NewError(
			domain.ErrorConflict,
			"KubeBlocks operation",
			fmt.Sprintf("OpsRequest %s belongs to another operation", name),
		)
	}

	uid := existing.GetUID()
	if uid == "" {
		return "", domain.NewError(
			domain.ErrorKubernetes,
			"KubeBlocks operation",
			fmt.Sprintf("OpsRequest %s has an incomplete identity", name),
		)
	}

	currentSpec, found, specErr := unstructured.NestedFieldCopy(existing.Object, "spec")
	if specErr != nil || !found || !kubeBlocksOpsSpecEqual(currentSpec, spec) {
		return "", domain.NewError(
			domain.ErrorConflict,
			"KubeBlocks operation",
			fmt.Sprintf("OpsRequest %s has a different spec", name),
		)
	}

	return uid, nil
}

func (m *Manager) pauseKubeBlocks(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause KubeBlocks",
			"session lacks KubeBlocks state",
		)
	}

	pod, err := m.typed.CoreV1().
		Pods(session.Spec.Workload().Pod.Namespace).
		Get(ctx, kb.Instance, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pause KubeBlocks",
			"read instance Pod",
			err,
		)
	}

	if err == nil {
		if pod.UID != session.Spec.Workload().Pod.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"pause KubeBlocks",
				fmt.Sprintf("Pod %s/%s UID changed", pod.Namespace, pod.Name),
			)
		}

		if err := validatePodController(
			pod,
			session.Spec.Workload().Controller,
			"pause KubeBlocks",
		); err != nil {
			return err
		}
	}

	if err == nil && session.Spec.Workload().Controller.Kind != domain.KindInstanceSet {
		if err := m.recoverLegacyKubeBlocksStoppedWithPod(ctx, session); err != nil {
			return err
		}
	}

	if session.Status.Phase == domain.PhasePausing &&
		session.Spec.Workload().Controller.Kind == domain.KindInstanceSet &&
		err == nil && isLeaderRole(podRole(pod)) &&
		kb.SwitchoverCandidate != "" {
		switch kb.SwitchoverStrategy {
		case domain.KubeBlocksSwitchoverOpsRequest:
			spec := kubeBlocksSwitchoverSpec(
				kb.OpsAPIVersion,
				kb.Cluster,
				kb.Component,
				kb.Instance,
				kb.SwitchoverCandidate,
			)
			if err := m.createAndWaitOps(ctx, session, "switchover", spec); err != nil {
				return err
			}
		case domain.KubeBlocksSwitchoverMongoDBNative:
			if err := m.runMongoDBNativeSwitchover(ctx, session); err != nil {
				return err
			}
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"pause KubeBlocks",
				fmt.Sprintf("unsupported persisted switchover strategy %q", kb.SwitchoverStrategy),
			)
		}

		if kb.SwitchoverStrategy != domain.KubeBlocksSwitchoverMongoDBNative {
			current, getErr := m.typed.CoreV1().
				Pods(session.Spec.Workload().Pod.Namespace).
				Get(ctx, kb.Instance, metav1.GetOptions{})
			if getErr != nil {
				return domain.WrapError(
					domain.ErrorKubernetes,
					"pause KubeBlocks",
					"verify switchover role",
					getErr,
				)
			}

			if err := validatePodController(
				current,
				session.Spec.Workload().Controller,
				"pause KubeBlocks",
			); err != nil {
				return err
			}

			if isLeaderRole(podRole(current)) {
				return domain.NewError(
					domain.ErrorPrecondition,
					"pause KubeBlocks",
					fmt.Sprintf(
						"instance %s retained role %s after switchover",
						kb.Instance,
						podRole(current),
					),
				)
			}
		}
	}

	if err := m.setKubeBlocksPaused(ctx, session, true); err != nil {
		return err
	}

	if err := m.deleteKubeBlocksPod(ctx, session); err != nil {
		return err
	}

	return m.VerifyPaused(ctx, session)
}

func (m *Manager) resumeKubeBlocks(ctx context.Context, session *domain.Session) error {
	pauseNotStarted, err := m.legacyKubeBlocksPauseNotStarted(ctx, session)
	if err != nil {
		return err
	}

	if pauseNotStarted {
		return nil
	}

	if err := m.validateKubeBlocksResume(ctx, session); err != nil {
		return err
	}

	if err := m.setKubeBlocksPaused(ctx, session, false); err != nil {
		return err
	}

	workload := session.Spec.Workload()

	if err := m.waitForResumedPod(
		ctx,
		session,
		workload.Pod,
		workload.Controller,
		"resume KubeBlocks",
	); err != nil {
		return err
	}

	if err := m.waitForKubeBlocksRunning(ctx, session); err != nil {
		return err
	}

	if workload.Controller.Kind == domain.KindInstanceSet {
		return nil
	}

	return m.updateKubeBlocksPauseOwner(ctx, session, false)
}

func (m *Manager) legacyKubeBlocksPauseNotStarted(
	ctx context.Context,
	session *domain.Session,
) (bool, error) {
	if !kubeBlocksAbortStartedFromPausing(session) {
		return false, nil
	}

	workload := session.Spec.Workload()

	kb := workload.KubeBlocks
	if kb == nil || workload.Controller.Kind == domain.KindInstanceSet {
		return false, nil
	}

	if m.dynamic == nil {
		return false, domain.NewError(
			domain.ErrorPrecondition,
			"resume KubeBlocks",
			"dynamic client is required for legacy KubeBlocks pause recovery",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return false, err
	}

	cluster, err := m.dynamic.Resource(gvr).Namespace(workload.Pod.Namespace).
		Get(ctx, kb.Cluster, metav1.GetOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"resume KubeBlocks",
			"read Cluster",
			err,
		)
	}

	if cluster.GetUID() != kb.ClusterUID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"resume KubeBlocks",
			fmt.Sprintf("Cluster %s/%s UID changed", cluster.GetNamespace(), cluster.GetName()),
		)
	}

	owner := cluster.GetAnnotations()[pauseSessionAnnotation]
	if owner != "" {
		return false, nil
	}

	phase, found, err := unstructured.NestedString(cluster.Object, "status", "phase")
	if err != nil || !found || kubeBlocksPhase(phase) != kubeBlocksPhaseRunning {
		return false, nil
	}

	components, found, err := unstructured.NestedSlice(
		cluster.Object,
		"spec",
		kubeBlocksFieldComponentSpecs,
	)
	if err != nil || !found {
		return false, nil
	}

	componentFound := false
	for index := range components {
		component, ok := components[index].(map[string]any)
		if !ok {
			return false, nil
		}

		name, _, _ := unstructured.NestedString(component, "name")
		if name != kb.Component {
			continue
		}

		componentFound = true

		break
	}

	if !componentFound {
		return false, nil
	}

	pod, err := m.typed.CoreV1().Pods(workload.Pod.Namespace).
		Get(ctx, workload.Pod.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}

		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"resume KubeBlocks",
			"read original Pod",
			err,
		)
	}

	if pod.UID != workload.Pod.UID || !kube.PodReady(pod) {
		return false, nil
	}

	if err := validatePodController(pod, workload.Controller, "resume KubeBlocks"); err != nil {
		return false, err
	}

	return true, nil
}

func kubeBlocksAbortStartedFromPausing(session *domain.Session) bool {
	if session == nil || (session.Status.Phase != domain.PhaseAborting &&
		(session.Status.Phase != domain.PhaseFailed ||
			session.Status.ResumeFrom != domain.PhaseAborting)) {
		return false
	}

	for _, history := range slices.Backward(session.Status.History) {
		switch history.Phase {
		case domain.PhaseFailed, domain.PhaseAborting:
			continue
		case domain.PhasePausing:
			return true
		default:
			return false
		}
	}

	return session.Status.ResumeFrom == domain.PhasePausing
}

func (m *Manager) waitForKubeBlocksRunning(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()

	kb := workload.KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume KubeBlocks",
			"session lacks KubeBlocks state",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return err
	}

	resource := m.dynamic.Resource(gvr).Namespace(workload.Pod.Namespace)

	return m.waitFor(
		ctx,
		fmt.Sprintf("KubeBlocks Cluster %s/%s convergence", workload.Pod.Namespace, kb.Cluster),
		func(waitCtx context.Context) (bool, error) {
			cluster, getErr := resource.Get(waitCtx, kb.Cluster, metav1.GetOptions{})
			if getErr != nil {
				return false, domain.WrapError(
					domain.ErrorKubernetes,
					"resume KubeBlocks",
					"read Cluster convergence state",
					getErr,
				)
			}

			if cluster.GetUID() != kb.ClusterUID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"resume KubeBlocks",
					fmt.Sprintf(
						"Cluster %s/%s UID changed",
						cluster.GetNamespace(),
						cluster.GetName(),
					),
				)
			}

			clusterPhase, clusterFound, phaseErr := unstructured.NestedString(
				cluster.Object,
				"status",
				"phase",
			)
			if phaseErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"resume KubeBlocks",
					"read Cluster phase",
					phaseErr,
				)
			}

			if !clusterFound || kubeBlocksPhase(clusterPhase) != kubeBlocksPhaseRunning {
				return false, nil
			}

			componentPhase, componentFound, componentErr := unstructured.NestedString(
				cluster.Object,
				"status",
				kubeBlocksFieldComponents,
				kb.Component,
				"phase",
			)
			if componentErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"resume KubeBlocks",
					"read KubeBlocks component phase",
					componentErr,
				)
			}

			return componentFound &&
				kubeBlocksPhase(componentPhase) == kubeBlocksPhaseRunning, nil
		},
	)
}

func (m *Manager) validateKubeBlocksResume(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume KubeBlocks",
			"session lacks KubeBlocks state",
		)
	}

	if session.Spec.Workload().Controller.Kind == domain.KindInstanceSet && kb.OriginalPaused {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume KubeBlocks",
			"an initially paused InstanceSet cannot safely recreate the migrated Pod; set spec.paused=false and verify the Pod is Ready before recovery",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume KubeBlocks",
			"dynamic client is required for KubeBlocks resume validation",
		)
	}

	workload := session.Spec.Workload()
	if workload.Controller.Kind == domain.KindInstanceSet {
		gvr, err := kube.ParseGroupVersionResource(
			workload.Controller.APIVersion,
			instanceSetResource,
		)
		if err != nil {
			return err
		}

		object, err := m.dynamic.Resource(gvr).Namespace(workload.Controller.Namespace).
			Get(ctx, workload.Controller.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"resume KubeBlocks",
				"read InstanceSet",
				err,
			)
		}

		if object.GetUID() != workload.Controller.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"resume KubeBlocks",
				fmt.Sprintf(
					"InstanceSet %s/%s UID changed",
					object.GetNamespace(),
					object.GetName(),
				),
			)
		}

		current, found, nestedErr := unstructured.NestedBool(
			object.Object,
			"spec",
			kubeBlocksFieldPaused,
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"resume KubeBlocks",
				"read InstanceSet paused state",
				nestedErr,
			)
		}

		if !found {
			current = false
		}

		owner := object.GetAnnotations()[pauseSessionAnnotation]
		if owner != "" && owner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"resume KubeBlocks",
				fmt.Sprintf(
					"InstanceSet %s/%s pause is owned by session %s",
					object.GetNamespace(),
					object.GetName(),
					owner,
				),
			)
		}

		return validateInstanceSetPauseState(workload.Controller, kb, false, current, found, owner)
	}

	return m.validateKubeBlocksLegacyResume(ctx, session, workload, kb)
}

func (m *Manager) validateKubeBlocksLegacyResume(
	ctx context.Context,
	session *domain.Session,
	workload domain.WorkloadSpec,
	kb *domain.KubeBlocksSpec,
) error {
	if kb.ClusterUID == "" || kb.Component == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"resume KubeBlocks",
			"session lacks Cluster identity",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return err
	}

	cluster, err := m.dynamic.Resource(gvr).Namespace(workload.Pod.Namespace).
		Get(ctx, kb.Cluster, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "resume KubeBlocks", "read Cluster", err)
	}

	if cluster.GetUID() != kb.ClusterUID {
		return domain.NewError(
			domain.ErrorConflict,
			"resume KubeBlocks",
			fmt.Sprintf("Cluster %s/%s UID changed", cluster.GetNamespace(), cluster.GetName()),
		)
	}

	owner := cluster.GetAnnotations()[pauseSessionAnnotation]
	if owner != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"resume KubeBlocks",
			fmt.Sprintf(
				"Cluster %s/%s pause ownership changed",
				cluster.GetNamespace(),
				cluster.GetName(),
			),
		)
	}

	components, ok, nestedErr := unstructured.NestedSlice(
		cluster.Object,
		"spec",
		kubeBlocksFieldComponentSpecs,
	)
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"resume KubeBlocks",
			"read componentSpecs",
			nestedErr,
		)
	}

	if !ok || len(components) == 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume KubeBlocks",
			"Cluster has no componentSpecs",
		)
	}

	resumed, err := kubeBlocksLegacyResumeConverged(cluster, kb)
	if err != nil {
		return err
	}

	if resumed {
		return nil
	}

	// A previous process may already have submitted Start. Its owned request
	// remains authoritative while the Cluster and Pods are still converging.
	starting, err := m.kubeBlocksResumeOperationStarted(ctx, session)
	if err != nil {
		return err
	}

	if !starting {
		if err := m.verifyKubeBlocksPauseOperation(ctx, session, cluster); err != nil {
			return err
		}
	}

	for index := range components {
		component, ok := components[index].(map[string]any)
		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume KubeBlocks",
				fmt.Sprintf("componentSpecs[%d] is malformed", index),
			)
		}

		name, found, nameErr := unstructured.NestedString(component, "name")
		if nameErr != nil || !found || name == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume KubeBlocks",
				fmt.Sprintf("componentSpecs[%d] has no name", index),
			)
		}

		if name != kb.Component {
			continue
		}

		return nil
	}

	return domain.NewError(
		domain.ErrorConflict,
		"resume KubeBlocks",
		"Cluster component "+kb.Component+" was removed after discovery",
	)
}

func (m *Manager) kubeBlocksResumeOperationStarted(
	ctx context.Context,
	session *domain.Session,
) (bool, error) {
	kb := session.Spec.Workload().KubeBlocks

	gvr, err := opsGVR(kb.OpsAPIVersion)
	if err != nil {
		return false, err
	}

	name := kubeBlocksOperationName(session, "resume")

	request, err := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace).
		Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"resume KubeBlocks",
			"read Start OpsRequest "+name,
			err,
		)
	}

	if _, err := validateKubeBlocksOpsRequest(
		request,
		name,
		session.ID,
		kubeBlocksPauseSpec(kb, false),
	); err != nil {
		return false, err
	}

	phase, _, err := unstructured.NestedString(request.Object, "status", "phase")
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorPrecondition,
			"resume KubeBlocks",
			"read Start OpsRequest phase",
			err,
		)
	}

	return !kubeBlocksOpsFailed(phase), nil
}

func (m *Manager) currentKubeBlocksRollbackPods(
	ctx context.Context,
	session *domain.Session,
) ([]domain.ObjectReference, error) {
	const operation = validateRollbackConsumers

	workload := session.Spec.Workload()
	if workload.KubeBlocks == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			operation,
			"session lacks KubeBlocks state",
		)
	}

	pod, err := m.typed.CoreV1().Pods(workload.Pod.Namespace).
		Get(ctx, workload.Pod.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, operation, "read KubeBlocks Pod", err)
	}

	if err := validatePodController(pod, workload.Controller, operation); err != nil {
		return nil, err
	}

	return []domain.ObjectReference{podReference(pod)}, nil
}

func (m *Manager) setKubeBlocksPaused(
	ctx context.Context,
	session *domain.Session,
	paused bool,
) error {
	workload := session.Spec.Workload()

	kb := workload.KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"KubeBlocks pause",
			"session lacks KubeBlocks state",
		)
	}

	if workload.Controller.Kind == domain.KindInstanceSet {
		return m.setKubeBlocksInstanceSetPaused(ctx, session, paused)
	}

	if kb.ClusterUID == "" || kb.Component == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"KubeBlocks pause",
			"session lacks Cluster identity",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"KubeBlocks pause",
			"dynamic client is required for Cluster pause",
		)
	}

	if session.ID == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"KubeBlocks pause",
			"session ID is required for OpsRequest ownership",
		)
	}

	cluster, err := m.validateKubeBlocksClusterForPause(ctx, session)
	if err != nil {
		return err
	}

	if !paused {
		resumed, stateErr := kubeBlocksLegacyResumeConverged(cluster, kb)
		if stateErr != nil {
			return stateErr
		}

		if resumed {
			return nil
		}
	}

	action := "resume"
	if paused {
		action = "pause"

		stopped, _, stateErr := kubeBlocksStopped(cluster, kb)
		if stateErr != nil {
			return stateErr
		}

		if stopped {
			if cluster.GetAnnotations()[pauseSessionAnnotation] != session.ID {
				return domain.NewError(
					domain.ErrorConflict,
					"KubeBlocks pause",
					fmt.Sprintf(
						"Cluster %s/%s is already stopped without this session's ownership",
						cluster.GetNamespace(),
						cluster.GetName(),
					),
				)
			}

			return nil
		}

		if err := m.updateKubeBlocksPauseOwner(ctx, session, true); err != nil {
			return err
		}
	}

	if err := m.createAndWaitOps(
		ctx,
		session,
		action,
		kubeBlocksPauseSpec(kb, paused),
	); err != nil {
		return err
	}

	return nil
}

func kubeBlocksLegacyResumeConverged(
	cluster *unstructured.Unstructured,
	kb *domain.KubeBlocksSpec,
) (bool, error) {
	if cluster == nil || kb == nil {
		return false, domain.NewError(
			domain.ErrorValidation,
			"resume KubeBlocks",
			"Cluster and KubeBlocks state are required",
		)
	}

	phase, found, err := unstructured.NestedString(cluster.Object, "status", "phase")
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorPrecondition,
			"resume KubeBlocks",
			"read Cluster phase",
			err,
		)
	}

	if !found || kubeBlocksPhase(phase) != kubeBlocksPhaseRunning {
		return false, nil
	}

	componentPhase, componentFound, componentErr := unstructured.NestedString(
		cluster.Object,
		"status",
		kubeBlocksFieldComponents,
		kb.Component,
		"phase",
	)
	if componentErr != nil {
		return false, domain.WrapError(
			domain.ErrorPrecondition,
			"resume KubeBlocks",
			"read KubeBlocks component phase",
			componentErr,
		)
	}

	if !componentFound || kubeBlocksPhase(componentPhase) != kubeBlocksPhaseRunning {
		return false, nil
	}

	return true, nil
}

func (m *Manager) recoverLegacyKubeBlocksStoppedWithPod(
	ctx context.Context,
	session *domain.Session,
) error {
	kb := session.Spec.Workload().KubeBlocks

	cluster, err := m.validateKubeBlocksClusterForPause(ctx, session)
	if err != nil {
		return err
	}

	stopped, phase, err := kubeBlocksStopped(cluster, kb)
	if err != nil || kubeBlocksPhase(phase) == kubeBlocksPhaseRunning {
		return err
	}

	if !stopped && kubeBlocksPhase(phase) != kubeBlocksPhaseFailed {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			fmt.Sprintf(
				"Cluster %s/%s phase is %s while its instance Pod is still present",
				cluster.GetNamespace(),
				cluster.GetName(),
				phase,
			),
		)
	}

	if cluster.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"pause KubeBlocks",
			fmt.Sprintf(
				"Cluster %s/%s is stopped while its instance Pod is still present without this session's ownership",
				cluster.GetNamespace(),
				cluster.GetName(),
			),
		)
	}

	if err := m.createAndWaitOps(
		ctx,
		session,
		"reconcile",
		kubeBlocksPauseSpec(kb, false),
	); err != nil {
		return err
	}

	if err := m.replaceLegacyKubeBlocksPod(ctx, session); err != nil {
		return err
	}

	return m.waitForKubeBlocksRunning(ctx, session)
}

func (m *Manager) replaceLegacyKubeBlocksPod(
	ctx context.Context,
	session *domain.Session,
) error {
	workload := session.Spec.WorkloadPtr()
	if workload == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"reconcile KubeBlocks",
			"session workload is required",
		)
	}

	previous := workload.Pod
	if previous.Namespace == "" || previous.Name == "" || previous.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"reconcile KubeBlocks",
			"persisted KubeBlocks Pod identity is incomplete",
		)
	}

	pods := m.typed.CoreV1().Pods(previous.Namespace)

	current, err := pods.Get(ctx, previous.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reconcile KubeBlocks",
			"read stale KubeBlocks Pod",
			err,
		)
	}

	if current.UID != previous.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reconcile KubeBlocks",
			fmt.Sprintf("Pod %s/%s UID changed", previous.Namespace, previous.Name),
		)
	}

	if err := validatePodController(
		current,
		workload.Controller,
		"reconcile KubeBlocks",
	); err != nil {
		return err
	}

	uid := current.UID
	if err := pods.Delete(
		ctx,
		previous.Name,
		metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
	); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reconcile KubeBlocks",
			"delete stale KubeBlocks Pod",
			err,
		)
	}

	var replacement *corev1.Pod
	if err := m.waitFor(
		ctx,
		fmt.Sprintf("replacement Pod %s/%s readiness", previous.Namespace, previous.Name),
		func(waitCtx context.Context) (bool, error) {
			pod, readErr := pods.Get(waitCtx, previous.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(readErr) {
				return false, nil
			}

			if readErr != nil {
				return false, readErr
			}

			if pod.UID == previous.UID {
				return false, nil
			}

			if err := validatePodController(
				pod,
				workload.Controller,
				"reconcile KubeBlocks",
			); err != nil {
				return false, err
			}

			if !kube.PodReady(pod) {
				return false, nil
			}

			replacement = pod.DeepCopy()

			return true, nil
		},
	); err != nil {
		return err
	}

	if replacement == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"reconcile KubeBlocks",
			fmt.Sprintf(
				"replacement Pod %s/%s readiness returned no Pod",
				previous.Namespace,
				previous.Name,
			),
		)
	}

	refreshResumedPodReference(workload, previous, replacement)

	return nil
}

func (m *Manager) updateKubeBlocksPauseOwner(
	ctx context.Context,
	session *domain.Session,
	paused bool,
) error {
	kb := session.Spec.Workload().KubeBlocks

	gvr, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return err
	}

	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cluster, err := resource.Get(ctx, kb.Cluster, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"KubeBlocks pause",
				"read Cluster pause owner",
				err,
			)
		}

		if cluster.GetUID() != kb.ClusterUID {
			return domain.NewError(
				domain.ErrorConflict,
				"KubeBlocks pause",
				fmt.Sprintf("Cluster %s/%s UID changed", cluster.GetNamespace(), cluster.GetName()),
			)
		}

		annotations := cluster.GetAnnotations()

		owner := annotations[pauseSessionAnnotation]
		if owner != "" && owner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"KubeBlocks pause",
				fmt.Sprintf(
					"Cluster %s/%s pause is owned by session %s",
					cluster.GetNamespace(),
					cluster.GetName(),
					owner,
				),
			)
		}

		if paused {
			if owner == session.ID {
				return nil
			}

			annotations = maps.Clone(annotations)
			if annotations == nil {
				annotations = map[string]string{}
			}

			annotations[pauseSessionAnnotation] = session.ID
			cluster.SetAnnotations(annotations)
		} else {
			if owner != session.ID {
				return nil
			}

			annotations = maps.Clone(annotations)
			delete(annotations, pauseSessionAnnotation)
			cluster.SetAnnotations(annotations)
		}

		if _, err := resource.Update(
			ctx,
			cluster,
			metav1.UpdateOptions{},
		); apierrors.IsConflict(
			err,
		) {
			return err
		} else if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"KubeBlocks pause",
				"update Cluster pause owner",
				err,
			)
		}

		return nil
	})
}

func (m *Manager) validateKubeBlocksClusterForPause(
	ctx context.Context,
	session *domain.Session,
) (*unstructured.Unstructured, error) {
	kb := session.Spec.Workload().KubeBlocks

	gvr, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return nil, err
	}

	cluster, err := m.dynamic.Resource(gvr).
		Namespace(session.Spec.Workload().Pod.Namespace).
		Get(ctx, kb.Cluster, metav1.GetOptions{})
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"KubeBlocks pause",
			"read Cluster",
			err,
		)
	}

	if cluster.GetUID() != kb.ClusterUID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"KubeBlocks pause",
			fmt.Sprintf("Cluster %s/%s UID changed", cluster.GetNamespace(), cluster.GetName()),
		)
	}

	components, found, nestedErr := unstructured.NestedSlice(
		cluster.Object,
		"spec",
		kubeBlocksFieldComponentSpecs,
	)
	if nestedErr != nil || !found {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"KubeBlocks pause",
			"Cluster has no componentSpecs",
		)
	}

	for index := range components {
		component, ok := components[index].(map[string]any)
		if !ok {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"KubeBlocks pause",
				fmt.Sprintf("componentSpecs[%d] is malformed", index),
			)
		}

		name, nameFound, nameErr := unstructured.NestedString(component, "name")
		if nameErr != nil || !nameFound || name == "" {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"KubeBlocks pause",
				fmt.Sprintf("componentSpecs[%d] has no name", index),
			)
		}

		if name != kb.Component {
			continue
		}

		return cluster, nil
	}

	return nil, domain.NewError(
		domain.ErrorConflict,
		"KubeBlocks pause",
		fmt.Sprintf("Cluster component %s was removed after discovery", kb.Component),
	)
}

func (m *Manager) setKubeBlocksInstanceSetPaused(
	ctx context.Context,
	session *domain.Session,
	paused bool,
) error {
	workload := session.Spec.Workload()

	kb := workload.KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"InstanceSet pause",
			"session lacks KubeBlocks state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"InstanceSet pause",
			"dynamic client is required for InstanceSet reconciliation control",
		)
	}

	if session.ID == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"InstanceSet pause",
			"session ID is required for InstanceSet pause ownership",
		)
	}

	ref := workload.Controller

	gvr, err := kube.ParseGroupVersionResource(ref.APIVersion, instanceSetResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return m.updateKubeBlocksInstanceSet(ctx, session, kb, ref, paused, gvr)
	})
}

func kubeBlocksPauseSpec(kb *domain.KubeBlocksSpec, paused bool) map[string]any {
	typeName := "Start"

	field := "start"
	if paused {
		typeName = "Stop"
		field = "stop"
	}

	clusterField := kubeBlocksClusterField(kb.OpsAPIVersion)
	if usesComponentScopedKubeBlocksOps(kb.OpsAPIVersion) {
		return map[string]any{
			clusterField: kb.Cluster,
			"type":       typeName,
			field:        []any{map[string]any{"componentName": kb.Component}},
		}
	}

	return map[string]any{
		clusterField: kb.Cluster,
		"type":       typeName,
	}
}

func (m *Manager) updateKubeBlocksInstanceSet(
	ctx context.Context,
	session *domain.Session,
	kb *domain.KubeBlocksSpec,
	ref domain.ObjectReference,
	paused bool,
	gvr schema.GroupVersionResource,
) error {
	resource := m.dynamic.Resource(gvr).Namespace(ref.Namespace)

	object, err := resource.Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"InstanceSet pause",
			"read InstanceSet",
			err,
		)
	}

	if object.GetUID() != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"InstanceSet pause",
			fmt.Sprintf("InstanceSet %s/%s UID changed", ref.Namespace, ref.Name),
		)
	}

	current, found, err := unstructured.NestedBool(object.Object, "spec", kubeBlocksFieldPaused)
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"InstanceSet pause",
			"read InstanceSet paused state",
			err,
		)
	}

	if !found {
		current = false
	}

	annotations := object.GetAnnotations()

	pauseOwner := annotations[pauseSessionAnnotation]
	if pauseOwner != "" && pauseOwner != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"InstanceSet pause",
			fmt.Sprintf(
				"InstanceSet %s/%s pause is owned by session %s",
				ref.Namespace,
				ref.Name,
				pauseOwner,
			),
		)
	}

	if err := validateInstanceSetPauseState(
		ref,
		kb,
		paused,
		current,
		found,
		pauseOwner,
	); err != nil {
		return err
	}

	want := paused
	if !paused {
		want = kb.OriginalPaused
	}

	changed := current != want || (!paused && found != kb.OriginalPausedConfigured)
	if changed {
		if !paused && !kb.OriginalPausedConfigured {
			unstructured.RemoveNestedField(object.Object, "spec", kubeBlocksFieldPaused)
		} else if err := unstructured.SetNestedField(object.Object, want, "spec", kubeBlocksFieldPaused); err != nil {
			return err
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

	updated, err := resource.Update(ctx, object, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return err
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"InstanceSet pause",
			"update InstanceSet paused state",
			err,
		)
	}

	actual, configured, err := unstructured.NestedBool(
		updated.Object,
		"spec",
		kubeBlocksFieldPaused,
	)
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"InstanceSet pause",
			"verify updated InstanceSet paused state",
			err,
		)
	}

	if paused && (!configured || !actual) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"InstanceSet pause",
			fmt.Sprintf(
				"InstanceSet %s/%s API did not preserve spec.paused",
				ref.Namespace,
				ref.Name,
			),
		)
	}

	return nil
}

func validateInstanceSetPauseState(
	ref domain.ObjectReference,
	kb *domain.KubeBlocksSpec,
	paused, current, found bool,
	pauseOwner string,
) error {
	if pauseOwner == "" {
		if current != kb.OriginalPaused {
			return domain.NewError(
				domain.ErrorConflict,
				"InstanceSet pause",
				fmt.Sprintf(
					"InstanceSet %s/%s paused changed from expected %t to %t",
					ref.Namespace,
					ref.Name,
					kb.OriginalPaused,
					current,
				),
			)
		}

		return nil
	}

	if paused && current {
		return nil
	}

	if !paused && !current {
		return domain.NewError(
			domain.ErrorConflict,
			"InstanceSet resume",
			fmt.Sprintf(
				"InstanceSet %s/%s paused state changed while session was active",
				ref.Namespace,
				ref.Name,
			),
		)
	}

	if paused && !current {
		return domain.NewError(
			domain.ErrorConflict,
			"InstanceSet pause",
			fmt.Sprintf(
				"InstanceSet %s/%s paused state changed while session was active",
				ref.Namespace,
				ref.Name,
			),
		)
	}

	_ = found

	return nil
}

func (m *Manager) verifyKubeBlocksInstanceSetPaused(
	ctx context.Context,
	session *domain.Session,
) error {
	ref := session.Spec.Workload().Controller

	gvr, err := kube.ParseGroupVersionResource(ref.APIVersion, instanceSetResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read InstanceSet", err)
	}

	if object.GetUID() != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("InstanceSet %s/%s UID changed", ref.Namespace, ref.Name),
		)
	}

	if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("InstanceSet %s/%s pause ownership changed", ref.Namespace, ref.Name),
		)
	}

	paused, found, nestedErr := unstructured.NestedBool(
		object.Object,
		"spec",
		kubeBlocksFieldPaused,
	)
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"verify paused",
			"read InstanceSet paused state",
			nestedErr,
		)
	}

	if !found || !paused {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf("InstanceSet %s/%s reconciliation is not paused", ref.Namespace, ref.Name),
		)
	}

	return nil
}

func (m *Manager) deleteKubeBlocksPod(ctx context.Context, session *domain.Session) error {
	ref := session.Spec.Workload().Pod

	pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pause KubeBlocks",
			"read instance Pod",
			err,
		)
	}

	if pod.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"pause KubeBlocks",
			fmt.Sprintf("Pod %s/%s UID changed", ref.Namespace, ref.Name),
		)
	}

	if err := validatePodController(
		pod,
		session.Spec.Workload().Controller,
		"pause KubeBlocks",
	); err != nil {
		return err
	}

	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}
	if err := m.typed.CoreV1().
		Pods(ref.Namespace).
		Delete(ctx, ref.Name, options); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pause KubeBlocks",
			"delete instance Pod",
			err,
		)
	}

	return m.waitFor(
		ctx,
		fmt.Sprintf("KubeBlocks Pod %s/%s deletion", ref.Namespace, ref.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := m.typed.CoreV1().
				Pods(ref.Namespace).
				Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}

			if getErr == nil && current.UID != pod.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"pause KubeBlocks",
					fmt.Sprintf(
						"Pod %s/%s was replaced while waiting for deletion",
						ref.Namespace,
						ref.Name,
					),
				)
			}

			return false, getErr
		},
	)
}
