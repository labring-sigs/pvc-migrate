package controller

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
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

	if kb.ClusterUID == "" {
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

	components, ok, nestedErr := unstructured.NestedSlice(object.Object, "spec", "componentSpecs")
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

		stopped, _, _ := unstructured.NestedBool(component, "stop")
		if !stopped {
			return domain.NewError(
				domain.ErrorPrecondition,
				"verify paused",
				fmt.Sprintf("KubeBlocks component %s is not stopped", name),
			)
		}

		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"verify paused",
		"KubeBlocks Cluster has no component "+kb.Component,
	)
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

	name := operationName(session.ID, action)
	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
	existing, getErr := resource.Get(ctx, name, metav1.GetOptions{})
	create := apierrors.IsNotFound(getErr)

	var expectedUID types.UID
	if getErr == nil {
		labels := existing.GetLabels()
		if labels[kube.ManagedByLabel] != kube.ManagedByValue ||
			labels[kube.SessionKey] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"KubeBlocks operation",
				fmt.Sprintf("OpsRequest %s belongs to another operation", name),
			)
		}

		expectedUID = existing.GetUID()
		if expectedUID == "" {
			return domain.NewError(
				domain.ErrorKubernetes,
				"KubeBlocks operation",
				fmt.Sprintf("OpsRequest %s has an incomplete identity", name),
			)
		}

		phase, _, _ := unstructured.NestedString(existing.Object, "status", "phase")
		if phase == "Failed" || phase == "Cancelled" || phase == "Aborted" {
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

		err = createErr
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"KubeBlocks operation",
				"create OpsRequest "+name,
				err,
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
			switch phase {
			case "Succeed":
				return true, nil
			case "Failed", "Cancelled", "Aborted":
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

func operationName(sessionID, action string) string {
	return kube.BoundedName("pvc-migrate", sessionID, action)
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

	if session.Status.Phase == domain.PhasePausing && err == nil && isLeaderRole(podRole(pod)) &&
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

	if session.Spec.Workload().Controller.Kind == domain.KindInstanceSet {
		if err := m.deleteKubeBlocksInstancePod(ctx, session); err != nil {
			return err
		}
		return m.VerifyPaused(ctx, session)
	}

	if err := m.waitForPodDeletion(
		ctx,
		session.Spec.Workload().Pod,
		"pause KubeBlocks",
	); err != nil {
		return err
	}

	return m.VerifyPaused(ctx, session)
}

func (m *Manager) runMongoDBNativeSwitchover(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause KubeBlocks",
			"session lacks KubeBlocks state",
		)
	}

	if kb.SwitchoverContainer == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			"MongoDB native switchover session lacks the validated container",
		)
	}

	if m.commandExecutor == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			"Pod exec is unavailable for the MongoDB native switchover; manual MongoDB switchover: "+kubeBlocksMongoDBNativeSwitchoverCommand(
				session.Spec.Workload().Pod.Namespace,
				kb.Cluster,
				kb.Component,
				kb.Instance,
				kb.SwitchoverCandidate,
			),
		)
	}

	namespace := session.Spec.Workload().Pod.Namespace

	selected, err := m.typed.CoreV1().Pods(namespace).Get(ctx, kb.Instance, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pause KubeBlocks",
			"read MongoDB switchover source Pod",
			err,
		)
	}

	if selected.UID != session.Spec.Workload().Pod.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"pause KubeBlocks",
			fmt.Sprintf("Pod %s/%s UID changed", selected.Namespace, selected.Name),
		)
	}

	if err := validatePodController(
		selected,
		session.Spec.Workload().Controller,
		"pause KubeBlocks",
	); err != nil {
		return err
	}

	headlessService := fmt.Sprintf("%s-%s-headless", kb.Cluster, kb.Component)
	leaderFQDN := fmt.Sprintf("%s.%s", kb.Instance, headlessService)

	candidateFQDN := fmt.Sprintf("%s.%s", kb.SwitchoverCandidate, headlessService)
	if m.logger != nil {
		m.logger.Info(
			"starting KubeBlocks MongoDB native switchover",
			"namespace",
			namespace,
			"cluster",
			kb.Cluster,
			"workload_component",
			kb.Component,
			"instance",
			kb.Instance,
			"candidate",
			kb.SwitchoverCandidate,
		)
	}

	result, err := m.commandExecutor.Execute(ctx, podCommandRequest{
		Namespace: namespace,
		Pod:       kb.Instance,
		Container: kb.SwitchoverContainer,
		Command: []string{
			"env",
			"KB_CONSENSUS_LEADER_POD_FQDN=" + leaderFQDN,
			"KB_SWITCHOVER_CANDIDATE_FQDN=" + candidateFQDN,
			"/scripts/switchover-with-candidate.sh",
		},
	})
	if err != nil {
		executionErr := podCommandError("run MongoDB native candidate switchover", result, err)

		return domain.WrapError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			fmt.Sprintf(
				"%v; manual MongoDB switchover: %s",
				executionErr,
				kubeBlocksMongoDBNativeSwitchoverCommand(
					namespace,
					kb.Cluster,
					kb.Component,
					kb.Instance,
					kb.SwitchoverCandidate,
				),
			),
			executionErr,
		)
	}

	return m.waitFor(
		ctx,
		fmt.Sprintf(
			"KubeBlocks MongoDB switchover from %s to %s",
			kb.Instance,
			kb.SwitchoverCandidate,
		),
		func(waitCtx context.Context) (bool, error) {
			leader, leaderErr := m.typed.CoreV1().
				Pods(namespace).
				Get(waitCtx, kb.Instance, metav1.GetOptions{})
			if leaderErr != nil {
				return false, leaderErr
			}

			if err := validatePodController(
				leader,
				session.Spec.Workload().Controller,
				"pause KubeBlocks",
			); err != nil {
				return false, err
			}

			candidate, candidateErr := m.typed.CoreV1().
				Pods(namespace).
				Get(waitCtx, kb.SwitchoverCandidate, metav1.GetOptions{})
			if candidateErr != nil {
				return false, candidateErr
			}

			if err := validatePodController(
				candidate,
				session.Spec.Workload().Controller,
				"pause KubeBlocks",
			); err != nil {
				return false, err
			}

			return !isLeaderRole(podRole(leader)) && isLeaderRole(podRole(candidate)), nil
		},
	)
}

func (m *Manager) resumeKubeBlocks(ctx context.Context, session *domain.Session) error {
	if err := m.validateKubeBlocksResume(ctx, session); err != nil {
		return err
	}

	if err := m.setKubeBlocksPaused(ctx, session, false); err != nil {
		return err
	}

	workload := session.Spec.Workload()

	return m.waitForResumedPod(ctx, session, workload.Pod, workload.Controller, "resume KubeBlocks")
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

		current, found, nestedErr := unstructured.NestedBool(object.Object, "spec", "paused")
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

	if kb.ClusterUID == "" || kb.OriginalStops == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume KubeBlocks",
			"session lacks Cluster identity or original component stop state",
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

	components, ok, nestedErr := unstructured.NestedSlice(cluster.Object, "spec", "componentSpecs")
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

	owner := cluster.GetAnnotations()[pauseSessionAnnotation]
	if owner != "" && owner != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"resume KubeBlocks",
			fmt.Sprintf(
				"Cluster %s/%s pause is owned by session %s",
				cluster.GetNamespace(),
				cluster.GetName(),
				owner,
			),
		)
	}

	_, err = updateKubeBlocksComponent(components, kb, session.ID, false, owner)

	return err
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
	if session.Spec.Workload().Controller.Kind == domain.KindInstanceSet {
		return m.setKubeBlocksInstanceSetPaused(ctx, session, paused)
	}

	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"KubeBlocks pause",
			"session lacks KubeBlocks state",
		)
	}

	if kb.ClusterUID == "" || kb.OriginalStops == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"KubeBlocks pause",
			"session lacks Cluster identity or original component stop state",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return err
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
			"session ID is required for Cluster pause ownership",
		)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return m.updateKubeBlocksCluster(ctx, session, kb, paused, gvr)
	})
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

func (m *Manager) updateKubeBlocksCluster(
	ctx context.Context,
	session *domain.Session,
	kb *domain.KubeBlocksSpec,
	paused bool,
	gvr schema.GroupVersionResource,
) error {
	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

	cluster, err := resource.Get(ctx, kb.Cluster, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "KubeBlocks pause", "read Cluster", err)
	}

	if cluster.GetUID() != kb.ClusterUID {
		return domain.NewError(
			domain.ErrorConflict,
			"KubeBlocks pause",
			fmt.Sprintf("Cluster %s/%s UID changed", cluster.GetNamespace(), cluster.GetName()),
		)
	}

	components, ok, nestedErr := unstructured.NestedSlice(cluster.Object, "spec", "componentSpecs")
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"KubeBlocks pause",
			"read componentSpecs",
			nestedErr,
		)
	}

	if !ok || len(components) == 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"KubeBlocks pause",
			"Cluster has no componentSpecs",
		)
	}

	annotations := cluster.GetAnnotations()

	pauseOwner := annotations[pauseSessionAnnotation]
	if pauseOwner != "" && pauseOwner != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"KubeBlocks pause",
			fmt.Sprintf(
				"Cluster %s/%s pause is owned by session %s",
				cluster.GetNamespace(),
				cluster.GetName(),
				pauseOwner,
			),
		)
	}

	changed, err := updateKubeBlocksComponent(components, kb, session.ID, paused, pauseOwner)
	if err != nil {
		return err
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

	if err := unstructured.SetNestedField(
		cluster.Object,
		components,
		"spec",
		"componentSpecs",
	); err != nil {
		return err
	}

	_, err = resource.Update(ctx, cluster, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return err
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"KubeBlocks pause",
			"update Cluster component stop state",
			err,
		)
	}

	return nil
}

func updateKubeBlocksComponent(
	components []any,
	kb *domain.KubeBlocksSpec,
	sessionID string,
	paused bool,
	pauseOwner string,
) (bool, error) {
	for index := range components {
		component, ok := components[index].(map[string]any)
		if !ok {
			return false, domain.NewError(
				domain.ErrorPrecondition,
				"KubeBlocks pause",
				fmt.Sprintf("componentSpecs[%d] is malformed", index),
			)
		}

		name, found, err := unstructured.NestedString(component, "name")
		if err != nil || !found || name == "" {
			return false, domain.NewError(
				domain.ErrorPrecondition,
				"KubeBlocks pause",
				fmt.Sprintf("componentSpecs[%d] has no name", index),
			)
		}

		if name != kb.Component {
			continue
		}

		current, _, err := unstructured.NestedBool(component, "stop")
		if err != nil {
			return false, domain.WrapError(
				domain.ErrorPrecondition,
				"KubeBlocks pause",
				fmt.Sprintf("read component %s stop state", name),
				err,
			)
		}

		original, known := kb.OriginalStops[name]
		if !known {
			return false, domain.NewError(
				domain.ErrorConflict,
				"KubeBlocks pause",
				fmt.Sprintf("Cluster component %s lacks original stop state", name),
			)
		}

		expected := original
		if pauseOwner == sessionID {
			expected = true
		}

		want := true
		if !paused {
			want = original
		}

		if current != expected {
			return false, domain.NewError(
				domain.ErrorConflict,
				"KubeBlocks pause",
				fmt.Sprintf(
					"Cluster component %s stop changed from expected %t to %t",
					name,
					expected,
					current,
				),
			)
		}

		changed := current != want
		if changed {
			if err := unstructured.SetNestedField(component, want, "stop"); err != nil {
				return false, err
			}
		}

		components[index] = component

		return changed, nil
	}

	return false, domain.NewError(
		domain.ErrorConflict,
		"KubeBlocks pause",
		fmt.Sprintf("Cluster component %s was removed after discovery", kb.Component),
	)
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

	current, found, err := unstructured.NestedBool(object.Object, "spec", "paused")
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
			unstructured.RemoveNestedField(object.Object, "spec", "paused")
		} else if err := unstructured.SetNestedField(object.Object, want, "spec", "paused"); err != nil {
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

	actual, configured, err := unstructured.NestedBool(updated.Object, "spec", "paused")
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

	paused, found, nestedErr := unstructured.NestedBool(object.Object, "spec", "paused")
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

func (m *Manager) deleteKubeBlocksInstancePod(ctx context.Context, session *domain.Session) error {
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
