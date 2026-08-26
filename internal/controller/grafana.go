package controller

import (
	"context"
	"fmt"

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
	grafana := workload.Grafana
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

	deployment, err := m.readGrafanaDeployment(ctx, session, "verify paused")
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

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		deployment.Namespace,
		domain.KindDeployment,
		deployment.Name,
		"discover Grafana",
	); err != nil {
		return domain.WorkloadSpec{}, err
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

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		session.Spec.Workload().Controller.Namespace,
		domain.KindDeployment,
		session.Spec.Workload().Controller.Name,
		"pause Grafana",
	); err != nil {
		return err
	}

	if err := m.setGrafanaPaused(ctx, session); err != nil {
		return err
	}

	deployment, err := m.readGrafanaDeployment(ctx, session, "pause Grafana")
	if err != nil {
		if restoreErr := m.restoreGrafanaPause(ctx, session); restoreErr != nil {
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

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		session.Spec.Workload().Controller.Namespace,
		domain.KindDeployment,
		session.Spec.Workload().Controller.Name,
		"resume Grafana",
	); err != nil {
		return err
	}

	deployment, err := m.readGrafanaDeployment(ctx, session, "resume Grafana")
	if err != nil {
		return err
	}

	if err := m.updateDeploymentReplicas(
		ctx,
		deployment,
		"resume Grafana",
		*session.Spec.Workload().OriginalReplicas,
		0,
	); err != nil {
		return workloadScaleError("resume Grafana", "restore Deployment replicas", err)
	}

	if err := m.restoreGrafanaPause(ctx, session); err != nil {
		return err
	}

	var ready domain.ObjectReference
	if err := m.waitFor(
		ctx,
		fmt.Sprintf(
			"Grafana Deployment %s/%s readiness",
			session.Spec.Workload().Controller.Namespace,
			session.Spec.Workload().Controller.Name,
		),
		func(waitCtx context.Context) (bool, error) {
			deployment, err := m.readGrafanaDeployment(waitCtx, session, "resume Grafana")
			if err != nil {
				return false, err
			}

			expectedReplicas := *session.Spec.Workload().OriginalReplicas
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
		return err
	}

	if ready.UID != "" {
		workload := session.Spec.WorkloadPtr()
		workload.Pod = ready
		// Grafana discovery records one representative Deployment Pod. A new
		// ReplicaSet can change its generated name, so refresh that single
		// affected reference even when the name no longer matches.
		if len(workload.AffectedPods) == 1 {
			workload.AffectedPods[0] = ready
		}
	}

	return nil
}

func (m *Manager) validateGrafanaResume(
	ctx context.Context,
	session *domain.Session,
) error {
	workload := session.Spec.Workload()
	if workload.Grafana == nil || workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume Grafana",
			"session lacks Grafana state",
		)
	}

	if err := m.validateGrafanaSuspendState(ctx, session, "resume Grafana"); err != nil {
		return err
	}

	deployment, err := m.readGrafanaDeployment(ctx, session, "resume Grafana")
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
		*workload.OriginalReplicas,
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
	session *domain.Session,
	operation string,
) error {
	grafana := session.Spec.Workload().Grafana
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
		Namespace(session.Spec.Workload().Pod.Namespace).
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
	if owner != "" && owner != session.ID {
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

	suspended, _, nestedErr := unstructured.NestedBool(object.Object, "spec", "suspend")
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
	session *domain.Session,
) ([]domain.ObjectReference, error) {
	const operation = validateRollbackConsumers

	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			operation,
			"session lacks Grafana replica state",
		)
	}

	deployment, err := m.readGrafanaDeployment(ctx, session, operation)
	if err != nil {
		return nil, err
	}

	if err := validateResumeReplicas(
		deployment.Namespace,
		deployment.Name,
		deploymentReplicas(deployment),
		*workload.OriginalReplicas,
		0,
		operation,
		domain.KindDeployment,
	); err != nil {
		return nil, err
	}

	current, _, err := m.observeDeploymentPods(ctx, deployment)

	return current, err
}

func (m *Manager) readGrafanaDeployment(
	ctx context.Context,
	session *domain.Session,
	operation string,
) (*appsv1.Deployment, error) {
	workload := session.Spec.Workload()

	grafana := workload.Grafana
	if grafana == nil {
		return nil, domain.NewError(domain.ErrorInternal, operation, "session lacks Grafana state")
	}

	deployment, err := m.readDeployment(ctx, workload.Controller, operation)
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
