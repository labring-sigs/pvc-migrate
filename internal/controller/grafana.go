package controller

import (
	"context"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/retry"
)

func (m *Manager) verifyGrafanaPaused(
	ctx context.Context,
	session *domain.Session,
	workload domain.WorkloadSpec,
) error {
	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"dynamic client is required for Grafana pause verification",
		)
	}

	grafana := workload.Grafana
	if grafana == nil {
		return domain.NewError(domain.ErrorInternal, "verify paused", "session lacks Grafana state")
	}

	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(workload.Pod.Namespace).
		Get(ctx, grafana.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read Grafana", err)
	}

	if object.GetUID() != grafana.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()),
		)
	}

	if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"Grafana %s/%s suspend ownership changed",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	suspended, _, _ := unstructured.NestedBool(object.Object, "spec", "suspend")
	if !suspended {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"Grafana reconciliation is not suspended",
		)
	}

	if workload.Controller.Kind != domain.KindDeployment {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"Grafana session lacks Deployment controller state",
		)
	}

	deployment, deploymentErr := m.typed.AppsV1().
		Deployments(workload.Controller.Namespace).
		Get(ctx, workload.Controller.Name, metav1.GetOptions{})
	if deploymentErr != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify paused",
			"read Grafana Deployment",
			deploymentErr,
		)
	}

	if deployment.UID != workload.Controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"Grafana Deployment %s/%s UID changed",
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	if replicas := deploymentReplicas(deployment); replicas != 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf(
				"Grafana Deployment %s/%s replicas=%d while reconciliation is suspended",
				deployment.Namespace,
				deployment.Name,
				replicas,
			),
		)
	}

	return nil
}

func (m *Manager) grafanaWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	deployment *appsv1.Deployment,
	owner *metav1.OwnerReference,
) (domain.WorkloadSpec, error) {
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas <= 0 {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Grafana",
			fmt.Sprintf(
				"Deployment %s/%s has no positive replica count",
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	if m.dynamic == nil {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Grafana",
			"dynamic client is required for Grafana pause control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(grafanaAPIVersion, grafanaResource)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	grafana, err := m.dynamic.Resource(gvr).
		Namespace(pod.Namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover Grafana",
			"read Grafana",
			err,
		)
	}

	if grafana.GetUID() == "" || grafana.GetUID() != owner.UID {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover Grafana",
			fmt.Sprintf(
				"Deployment %s/%s Grafana owner UID changed",
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	suspended, suspendConfigured, nestedErr := unstructured.NestedBool(
		grafana.Object,
		"spec",
		"suspend",
	)
	if nestedErr != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorPrecondition,
			"discover Grafana",
			"read reconciliation suspend state",
			nestedErr,
		)
	}

	return domain.WorkloadSpec{
		Adapter: domain.WorkloadGrafana,
		Pod:     podReference(pod),
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindDeployment,
			deployment.Namespace,
			deployment.Name,
			deployment.UID,
			deployment.ResourceVersion,
		),
		OriginalReplicas: deployment.Spec.Replicas,
		AffectedPods:     []domain.ObjectReference{podReference(pod)},
		Grafana: &domain.GrafanaSpec{
			APIVersion:                grafanaAPIVersion,
			Name:                      owner.Name,
			UID:                       grafana.GetUID(),
			OriginalSuspend:           suspended,
			OriginalSuspendConfigured: suspendConfigured,
			OriginalReplicas:          *deployment.Spec.Replicas,
		},
	}, nil
}

func (m *Manager) pauseGrafana(ctx context.Context, session *domain.Session) error {
	grafana := session.Spec.Workload().Grafana
	if grafana == nil || session.Spec.Workload().OriginalReplicas == nil {
		return domain.NewError(domain.ErrorInternal, "pause Grafana", "session lacks Grafana state")
	}

	if err := m.setGrafanaPaused(ctx, session); err != nil {
		return err
	}

	if err := m.patchDeploymentReplicas(
		ctx,
		session.Spec.Workload().Controller,
		0,
		*session.Spec.Workload().OriginalReplicas,
	); err != nil {
		if restoreErr := m.restoreGrafanaPause(ctx, session); restoreErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"pause Grafana",
				fmt.Sprintf("scale Deployment: %v; restore Grafana suspend state", err),
				restoreErr,
			)
		}

		return workloadScaleError("pause Grafana", "scale Deployment", err)
	}

	for _, ref := range session.Spec.Workload().AffectedPods {
		if err := m.waitForPodDeletion(ctx, ref, "pause Grafana"); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) resumeGrafana(ctx context.Context, session *domain.Session) error {
	grafana := session.Spec.Workload().Grafana
	if grafana == nil || session.Spec.Workload().OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume Grafana",
			"session lacks Grafana state",
		)
	}

	if err := m.patchDeploymentReplicas(
		ctx,
		session.Spec.Workload().Controller,
		*session.Spec.Workload().OriginalReplicas,
		0,
	); err != nil {
		return workloadScaleError("resume Grafana", "restore Deployment replicas", err)
	}

	if err := m.restoreGrafanaPause(ctx, session); err != nil {
		return err
	}

	var ready *corev1.Pod
	if err := m.waitFor(
		ctx,
		fmt.Sprintf(
			"Grafana Deployment %s/%s readiness",
			session.Spec.Workload().Controller.Namespace,
			session.Spec.Workload().Controller.Name,
		),
		func(waitCtx context.Context) (bool, error) {
			deployment, err := m.typed.AppsV1().
				Deployments(session.Spec.Workload().Controller.Namespace).
				Get(waitCtx, session.Spec.Workload().Controller.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}

			if expectedUID := session.Spec.Workload().Controller.UID; deployment.UID != expectedUID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"resume Grafana",
					fmt.Sprintf(
						"Deployment %s/%s UID changed",
						deployment.Namespace,
						deployment.Name,
					),
				)
			}

			selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
			if err != nil {
				return false, err
			}

			pods, err := m.typed.CoreV1().
				Pods(deployment.Namespace).
				List(waitCtx, metav1.ListOptions{LabelSelector: selector.String()})
			if err != nil {
				return false, err
			}

			for index := range pods.Items {
				owned, ownerErr := m.podControlledByDeployment(
					waitCtx,
					&pods.Items[index],
					deployment,
				)
				if ownerErr != nil {
					return false, ownerErr
				}

				if owned && kube.PodReady(&pods.Items[index]) {
					ready = &pods.Items[index]
					return true, nil
				}
			}

			return false, nil
		},
	); err != nil {
		return err
	}

	if ready != nil {
		workload := session.Spec.WorkloadPtr()
		previous := workload.Pod
		refreshResumedPodReference(workload, previous, ready)
		// Grafana discovery records one representative Deployment Pod. A new
		// ReplicaSet can change its generated name, so refresh that single
		// affected reference even when the name no longer matches.
		if len(workload.AffectedPods) == 1 {
			workload.AffectedPods[0] = podReference(ready)
		}
	}

	return nil
}

func (m *Manager) podControlledByDeployment(
	ctx context.Context,
	pod *corev1.Pod,
	deployment *appsv1.Deployment,
) (bool, error) {
	owner := controllerOwner(pod.OwnerReferences)
	if owner == nil || owner.APIVersion != domain.AppsAPIVersion ||
		owner.Kind != domain.KindReplicaSet ||
		owner.Name == "" {
		return false, nil
	}

	replicaSet, err := m.typed.AppsV1().
		ReplicaSets(pod.Namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	if owner.UID == "" || replicaSet.UID == "" || replicaSet.UID != owner.UID {
		return false, nil
	}

	return sameControllerOwner(controllerOwner(replicaSet.OwnerReferences), &metav1.OwnerReference{
		APIVersion: domain.AppsAPIVersion,
		Kind:       domain.KindDeployment,
		Name:       deployment.Name,
		UID:        deployment.UID,
	}), nil
}

func (m *Manager) restoreGrafanaPause(ctx context.Context, session *domain.Session) error {
	grafana := session.Spec.Workload().Grafana
	if grafana == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore Grafana pause",
			"session lacks Grafana state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"restore Grafana pause",
			"dynamic client is required for deployment pause control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

		object, getErr := resource.Get(ctx, grafana.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"restore Grafana pause",
				"read Grafana",
				getErr,
			)
		}

		if object.GetUID() != grafana.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"restore Grafana pause",
				fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		current, _, nestedErr := unstructured.NestedBool(object.Object, "spec", "suspend")
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"restore Grafana suspend",
				"read reconciliation suspend state",
				nestedErr,
			)
		}

		annotations := object.GetAnnotations()

		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"restore Grafana suspend",
				fmt.Sprintf(
					"Grafana %s/%s suspend is owned by session %s",
					object.GetNamespace(),
					object.GetName(),
					pauseOwner,
				),
			)
		}

		if pauseOwner == "" {
			if current != grafana.OriginalSuspend {
				return domain.NewError(
					domain.ErrorConflict,
					"restore Grafana suspend",
					fmt.Sprintf(
						"Grafana suspend changed from expected %t to %t",
						grafana.OriginalSuspend,
						current,
					),
				)
			}

			return nil
		}

		if !current {
			return domain.NewError(
				domain.ErrorConflict,
				"restore Grafana suspend",
				"Grafana suspend state changed while session was active",
			)
		}

		if current != grafana.OriginalSuspend {
			if grafana.OriginalSuspendConfigured {
				if err := unstructured.SetNestedField(
					object.Object,
					grafana.OriginalSuspend,
					"spec",
					"suspend",
				); err != nil {
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

			return domain.WrapError(
				domain.ErrorKubernetes,
				"restore Grafana suspend",
				"clear reconciliation suspend owner",
				updateErr,
			)
		}

		return nil
	})
}

func (m *Manager) setGrafanaPaused(
	ctx context.Context,
	session *domain.Session,
) error {
	grafana := session.Spec.Workload().Grafana

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"Grafana suspend",
			"dynamic client is required for reconciliation suspend control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

		object, getErr := resource.Get(ctx, grafana.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"Grafana suspend",
				"read Grafana",
				getErr,
			)
		}

		if object.GetUID() != grafana.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"Grafana suspend",
				fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		current, _, nestedErr := unstructured.NestedBool(object.Object, "spec", "suspend")
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"Grafana suspend",
				"read reconciliation suspend state",
				nestedErr,
			)
		}

		annotations := object.GetAnnotations()

		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"Grafana suspend",
				fmt.Sprintf(
					"Grafana %s/%s suspend is owned by session %s",
					object.GetNamespace(),
					object.GetName(),
					pauseOwner,
				),
			)
		}

		if pauseOwner == "" && current != grafana.OriginalSuspend {
			return domain.NewError(
				domain.ErrorConflict,
				"Grafana suspend",
				fmt.Sprintf(
					"Grafana suspend changed from expected %t to %t",
					grafana.OriginalSuspend,
					current,
				),
			)
		}

		if pauseOwner == session.ID && current {
			return nil
		}

		if pauseOwner == session.ID && !current {
			return domain.NewError(
				domain.ErrorConflict,
				"Grafana suspend",
				"Grafana suspend state changed while session was active",
			)
		}

		if err := unstructured.SetNestedField(
			object.Object,
			true,
			"spec",
			"suspend",
		); err != nil {
			return err
		}

		if annotations == nil {
			annotations = map[string]string{}
		}

		annotations[pauseSessionAnnotation] = session.ID

		object.SetAnnotations(annotations)

		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}

			return domain.WrapError(
				domain.ErrorKubernetes,
				"Grafana suspend",
				"update reconciliation suspend state",
				updateErr,
			)
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

func (m *Manager) patchDeploymentReplicas(
	ctx context.Context,
	ref domain.ObjectReference,
	replicas int32,
	allowedCurrent ...int32,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := m.typed.AppsV1().
			Deployments(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if deployment.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"scale Deployment",
				fmt.Sprintf("Deployment %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		current := int32(1)
		if deployment.Spec.Replicas != nil {
			current = *deployment.Spec.Replicas
		}

		if current == replicas {
			return nil
		}

		allowed := slices.Contains(allowedCurrent, current)

		if !allowed {
			return domain.NewError(
				domain.ErrorConflict,
				"scale Deployment",
				fmt.Sprintf(
					"Deployment %s/%s replicas changed to %d",
					ref.Namespace,
					ref.Name,
					current,
				),
			)
		}

		deployment.Spec.Replicas = &replicas
		_, err = m.typed.AppsV1().
			Deployments(ref.Namespace).
			Update(ctx, deployment, metav1.UpdateOptions{})

		return err
	})
}
