package controller

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/retry"
)

const grafanaFieldSuspend = "suspend"

func (m *Manager) verifyGrafanaPaused(
	ctx context.Context,
	workflowID string,
	controller v1alpha1.ObjectReference,
	pod v1alpha1.ObjectReference,
	grafana *v1alpha1.GrafanaSpec,
) error {
	if grafana == nil {
		return domain.NewError(domain.ErrorInternal, "verify paused", "session lacks Grafana state")
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"dynamic client is required for Grafana pause verification",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(pod.Namespace).
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

	if object.GetAnnotations()[pauseSessionAnnotation] != workflowID {
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

	suspended, _, _ := unstructured.NestedBool(object.Object, "spec", grafanaFieldSuspend)
	if !suspended {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"Grafana reconciliation is not suspended",
		)
	}

	if controller.Kind != domain.KindDeployment {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"Grafana session lacks Deployment controller state",
		)
	}

	deployment, err := m.readGrafanaDeploymentFor(ctx, controller, grafana, "verify paused")
	if err != nil {
		return err
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

func (m *Manager) readGrafanaDeploymentFor(
	ctx context.Context,
	controller v1alpha1.ObjectReference,
	grafana *v1alpha1.GrafanaSpec,
	operation string,
) (*appsv1.Deployment, error) {
	if grafana == nil {
		return nil, domain.NewError(domain.ErrorInternal, operation, "Grafana state is required")
	}

	deployment, err := m.readDeployment(ctx, controller, operation)
	if err != nil {
		return nil, err
	}

	expectedOwner := &metav1.OwnerReference{
		APIVersion: grafana.APIVersion,
		Kind:       domain.KindGrafana,
		Name:       grafana.Name,
		UID:        grafana.UID,
	}
	if !sameControllerOwner(controllerOwner(deployment.OwnerReferences), expectedOwner) {
		return nil, domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf(
				"Deployment %s/%s Grafana controller identity changed",
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		deployment.Namespace,
		domain.KindDeployment,
		deployment.Name,
		operation,
	); err != nil {
		return nil, err
	}

	return deployment, nil
}

func (m *Manager) grafanaWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	deployment *appsv1.Deployment,
	owner *metav1.OwnerReference,
) (v1alpha1.WorkloadSpec, error) {
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas <= 0 {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Grafana",
			fmt.Sprintf(
				"Deployment %s/%s has no positive replica count",
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		deployment.Namespace,
		domain.KindDeployment,
		deployment.Name,
		"discover Grafana",
	); err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	if m.dynamic == nil {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Grafana",
			"dynamic client is required for Grafana pause control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(grafanaAPIVersion, grafanaResource)
	if err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	grafana, err := m.dynamic.Resource(gvr).
		Namespace(pod.Namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return v1alpha1.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover Grafana",
			"read Grafana",
			err,
		)
	}

	if grafana.GetUID() == "" || grafana.GetUID() != owner.UID {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
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
		grafanaFieldSuspend,
	)
	if nestedErr != nil {
		return v1alpha1.WorkloadSpec{}, domain.WrapError(
			domain.ErrorPrecondition,
			"discover Grafana",
			"read reconciliation suspend state",
			nestedErr,
		)
	}

	return v1alpha1.WorkloadSpec{
		Adapter: v1alpha1.WorkloadGrafana,
		Pod:     workloadPodReference(pod),
		Controller: workloadObjectReference(
			domain.AppsAPIVersion,
			domain.KindDeployment,
			deployment.Name,
			deployment.UID,
			deployment.ResourceVersion,
		),
		OriginalReplicas: deployment.Spec.Replicas,
		AffectedPods:     []v1alpha1.LocalResourceReference{*workloadPodReference(pod)},
		Grafana: &v1alpha1.GrafanaSpec{
			APIVersion:                grafanaAPIVersion,
			Name:                      owner.Name,
			UID:                       grafana.GetUID(),
			OriginalSuspend:           suspended,
			OriginalSuspendConfigured: suspendConfigured,
			OriginalReplicas:          *deployment.Spec.Replicas,
		},
	}, nil
}

func (m *Manager) pauseGrafana(
	ctx context.Context,
	workflowID, namespace string,
	controller v1alpha1.ObjectReference,
	originalReplicas *int32,
	affectedPods []v1alpha1.ObjectReference,
	grafana *v1alpha1.GrafanaSpec,
) error {
	if grafana == nil || originalReplicas == nil {
		return domain.NewError(domain.ErrorInternal, "pause Grafana", "session lacks Grafana state")
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		controller.Namespace,
		domain.KindDeployment,
		controller.Name,
		"pause Grafana",
	); err != nil {
		return err
	}

	if err := m.setGrafanaPaused(ctx, workflowID, namespace, grafana); err != nil {
		return err
	}

	deployment, err := m.readGrafanaDeploymentFor(ctx, controller, grafana, "pause Grafana")
	if err != nil {
		if restoreErr := m.restoreGrafanaPause(
			ctx,
			workflowID,
			namespace,
			grafana,
		); restoreErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"pause Grafana",
				fmt.Sprintf("validate Deployment: %v; restore Grafana suspend state", err),
				restoreErr,
			)
		}

		return err
	}

	if err := m.updateDeploymentReplicas(
		ctx,
		deployment,
		"pause Grafana",
		0,
		*originalReplicas,
	); err != nil {
		if restoreErr := m.restoreGrafanaPause(
			ctx,
			workflowID,
			namespace,
			grafana,
		); restoreErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"pause Grafana",
				fmt.Sprintf("scale Deployment: %v; restore Grafana suspend state", err),
				restoreErr,
			)
		}

		return workloadScaleError("pause Grafana", "scale Deployment", err)
	}

	for _, ref := range affectedPods {
		if err := m.waitForPodDeletion(ctx, ref, "pause Grafana"); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) resumeGrafana(
	ctx context.Context,
	workflowID, namespace string,
	controller v1alpha1.ObjectReference,
	originalReplicas *int32,
	grafana *v1alpha1.GrafanaSpec,
) (v1alpha1.ObjectReference, error) {
	if grafana == nil || originalReplicas == nil {
		return v1alpha1.ObjectReference{}, domain.NewError(
			domain.ErrorInternal,
			"resume Grafana",
			"session lacks Grafana state",
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		controller.Namespace,
		domain.KindDeployment,
		controller.Name,
		"resume Grafana",
	); err != nil {
		return v1alpha1.ObjectReference{}, err
	}

	deployment, err := m.readGrafanaDeploymentFor(ctx, controller, grafana, "resume Grafana")
	if err != nil {
		return v1alpha1.ObjectReference{}, err
	}

	if err := m.updateDeploymentReplicas(
		ctx,
		deployment,
		"resume Grafana",
		*originalReplicas,
		0,
	); err != nil {
		return v1alpha1.ObjectReference{}, workloadScaleError(
			"resume Grafana",
			"restore Deployment replicas",
			err,
		)
	}

	if err := m.restoreGrafanaPause(ctx, workflowID, namespace, grafana); err != nil {
		return v1alpha1.ObjectReference{}, err
	}

	var ready v1alpha1.ObjectReference
	if err := m.waitFor(
		ctx,
		fmt.Sprintf(
			"Grafana Deployment %s/%s readiness",
			controller.Namespace,
			controller.Name,
		),
		func(waitCtx context.Context) (bool, error) {
			deployment, err := m.readGrafanaDeploymentFor(
				waitCtx,
				controller,
				grafana,
				"resume Grafana",
			)
			if err != nil {
				return false, err
			}

			expectedReplicas := *originalReplicas
			if replicas := deploymentReplicas(deployment); replicas != expectedReplicas {
				return false, domain.NewError(
					domain.ErrorConflict,
					"resume Grafana",
					fmt.Sprintf(
						"Deployment %s/%s replicas changed to %d while restoring %d replicas",
						deployment.Namespace,
						deployment.Name,
						replicas,
						expectedReplicas,
					),
				)
			}

			current, allReady, err := m.observeDeploymentPods(waitCtx, deployment)
			if err != nil {
				return false, err
			}

			if !allReady || len(current) != int(expectedReplicas) ||
				deployment.Status.ObservedGeneration < deployment.Generation ||
				deployment.Status.Replicas != expectedReplicas ||
				deployment.Status.ReadyReplicas != expectedReplicas ||
				deployment.Status.AvailableReplicas != expectedReplicas ||
				deployment.Status.UpdatedReplicas != expectedReplicas ||
				deployment.Status.UnavailableReplicas != 0 {
				return false, nil
			}

			ready = current[0]

			return true, nil
		},
	); err != nil {
		return v1alpha1.ObjectReference{}, err
	}

	return ready, nil
}

func (m *Manager) validateGrafanaResume(
	ctx context.Context,
	workflowID, namespace string,
	controller v1alpha1.ObjectReference,
	originalReplicas *int32,
	grafana *v1alpha1.GrafanaSpec,
) error {
	if grafana == nil || originalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume Grafana",
			"session lacks Grafana state",
		)
	}

	if err := m.validateGrafanaSuspendState(
		ctx,
		workflowID,
		namespace,
		grafana,
		"resume Grafana",
	); err != nil {
		return err
	}

	deployment, err := m.readGrafanaDeploymentFor(ctx, controller, grafana, "resume Grafana")
	if err != nil {
		return err
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		deployment.Namespace,
		domain.KindDeployment,
		deployment.Name,
		"resume Grafana",
	); err != nil {
		return err
	}

	return validateResumeReplicas(
		deployment.Namespace,
		deployment.Name,
		deploymentReplicas(deployment),
		*originalReplicas,
		0,
		"resume Grafana",
		domain.KindDeployment,
	)
}

// validateGrafanaSuspendState mirrors restoreGrafanaPause without mutating the
// object. Resume dry-runs must reject the same CRD drift that execution would
// reject before touching the Deployment.
func (m *Manager) validateGrafanaSuspendState(
	ctx context.Context,
	workflowID, namespace string,
	grafana *v1alpha1.GrafanaSpec,
	operation string,
) error {
	if grafana == nil {
		return domain.NewError(domain.ErrorInternal, operation, "session lacks Grafana state")
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			operation,
			"dynamic client is required for Grafana suspend validation",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(namespace).
		Get(ctx, grafana.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, operation, "read Grafana", err)
	}

	if object.GetUID() != grafana.UID {
		return domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()),
		)
	}

	annotations := object.GetAnnotations()

	owner := annotations[pauseSessionAnnotation]
	if owner != "" && owner != workflowID {
		return domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf(
				"Grafana %s/%s suspend is owned by session %s",
				object.GetNamespace(),
				object.GetName(),
				owner,
			),
		)
	}

	suspended, _, nestedErr := unstructured.NestedBool(object.Object, "spec", grafanaFieldSuspend)
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			operation,
			"read reconciliation suspend state",
			nestedErr,
		)
	}

	if owner == "" {
		if suspended != grafana.OriginalSuspend {
			return domain.NewError(
				domain.ErrorConflict,
				operation,
				fmt.Sprintf(
					"Grafana suspend changed from expected %t to %t",
					grafana.OriginalSuspend,
					suspended,
				),
			)
		}

		return nil
	}

	if !suspended {
		return domain.NewError(
			domain.ErrorConflict,
			operation,
			"Grafana suspend state changed while session was active",
		)
	}

	return nil
}

func (m *Manager) currentGrafanaRollbackPods(
	ctx context.Context,
	controller v1alpha1.ObjectReference,
	originalReplicas *int32,
	grafana *v1alpha1.GrafanaSpec,
) ([]v1alpha1.ObjectReference, error) {
	const operation = validateRollbackConsumers

	if originalReplicas == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			operation,
			"session lacks Grafana replica state",
		)
	}

	deployment, err := m.readGrafanaDeploymentFor(ctx, controller, grafana, operation)
	if err != nil {
		return nil, err
	}

	if err := validateResumeReplicas(
		deployment.Namespace,
		deployment.Name,
		deploymentReplicas(deployment),
		*originalReplicas,
		0,
		operation,
		domain.KindDeployment,
	); err != nil {
		return nil, err
	}

	current, _, err := m.observeDeploymentPods(ctx, deployment)

	return current, err
}

func (m *Manager) restoreGrafanaPause(
	ctx context.Context,
	workflowID, namespace string,
	grafana *v1alpha1.GrafanaSpec,
) error {
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
		resource := m.dynamic.Resource(gvr).Namespace(namespace)

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

		current, _, nestedErr := unstructured.NestedBool(object.Object, "spec", grafanaFieldSuspend)
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
		if pauseOwner != "" && pauseOwner != workflowID {
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
					grafanaFieldSuspend,
				); err != nil {
					return err
				}
			} else {
				unstructured.RemoveNestedField(object.Object, "spec", grafanaFieldSuspend)
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

		if err := kube.LeaseFenceError(ctx); err != nil {
			return err
		}

		return nil
	})
}

func (m *Manager) setGrafanaPaused(
	ctx context.Context,
	workflowID, namespace string,
	grafana *v1alpha1.GrafanaSpec,
) error {
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
		resource := m.dynamic.Resource(gvr).Namespace(namespace)

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

		current, _, nestedErr := unstructured.NestedBool(object.Object, "spec", grafanaFieldSuspend)
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
		if pauseOwner != "" && pauseOwner != workflowID {
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

		if pauseOwner == workflowID && current {
			return nil
		}

		if pauseOwner == workflowID && !current {
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
			grafanaFieldSuspend,
		); err != nil {
			return err
		}

		if annotations == nil {
			annotations = map[string]string{}
		}

		annotations[pauseSessionAnnotation] = workflowID

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

		if err := kube.LeaseFenceError(ctx); err != nil {
			return err
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
