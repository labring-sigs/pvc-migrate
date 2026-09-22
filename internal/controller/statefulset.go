package controller

import (
	"context"
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

func (m *Manager) verifyStatefulSetPaused(
	ctx context.Context,
	controller v1alpha1.ObjectReference,
	originalReplicas, ordinal *int32,
) error {
	if controller.Kind != domain.KindStatefulSet || originalReplicas == nil || ordinal == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"StatefulSet session lacks controller and replica state",
		)
	}

	sts, err := m.typed.AppsV1().
		StatefulSets(controller.Namespace).
		Get(ctx, controller.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read StatefulSet", err)
	}

	if sts.UID != controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("StatefulSet %s/%s UID changed", sts.Namespace, sts.Name),
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		sts.Namespace,
		domain.KindStatefulSet,
		sts.Name,
		"verify paused",
	); err != nil {
		return err
	}

	if replicas := statefulSetReplicas(sts); replicas != *ordinal {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf(
				"StatefulSet %s/%s replicas=%d, expected %d while paused",
				sts.Namespace,
				sts.Name,
				replicas,
				*ordinal,
			),
		)
	}

	return nil
}

func (m *Manager) verifyVictoriaLogsPaused(
	ctx context.Context,
	workflowID string,
	controller v1alpha1.ObjectReference,
) error {
	if controller.Kind != domain.KindStatefulSet {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"Victoria Logs session lacks StatefulSet controller state",
		)
	}

	sts, err := m.typed.AppsV1().
		StatefulSets(controller.Namespace).
		Get(ctx, controller.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify paused",
			"read Victoria Logs StatefulSet",
			err,
		)
	}

	if sts.UID != controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("Victoria Logs StatefulSet %s/%s UID changed", sts.Namespace, sts.Name),
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		sts.Namespace,
		domain.KindStatefulSet,
		sts.Name,
		"verify paused",
	); err != nil {
		return err
	}

	if sts.Annotations[pauseSessionAnnotation] != workflowID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"Victoria Logs StatefulSet %s/%s pause ownership changed",
				sts.Namespace,
				sts.Name,
			),
		)
	}

	if replicas := statefulSetReplicas(sts); replicas != 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf(
				"Victoria Logs StatefulSet %s/%s replicas=%d",
				sts.Namespace,
				sts.Name,
				replicas,
			),
		)
	}

	return nil
}

func (m *Manager) statefulSetWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	sts *appsv1.StatefulSet,
	allowLeaderDowntime bool,
) (v1alpha1.WorkloadSpec, error) {
	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		sts.Namespace,
		domain.KindStatefulSet,
		sts.Name,
		"discover StatefulSet",
	); err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	ordinal, err := podOrdinal(pod, sts.Name)
	if err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	if ordinal >= replicas {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover StatefulSet",
			fmt.Sprintf("Pod ordinal %d is outside replicas %d", ordinal, replicas),
		)
	}

	if policy := sts.Spec.PersistentVolumeClaimRetentionPolicy; policy != nil &&
		policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover StatefulSet",
			fmt.Sprintf("PVC retention whenScaled is %s", policy.WhenScaled),
		)
	}

	affected := make([]v1alpha1.ObjectReference, 0, replicas-ordinal)

	names := make([]string, 0, replicas-ordinal)
	for current := ordinal; current < replicas; current++ {
		names = append(names, fmt.Sprintf("%s-%d", sts.Name, current))
	}

	candidates, getErrors := m.readPods(ctx, pod.Namespace, names)
	for index, name := range names {
		candidate, getErr := candidates[index], getErrors[index]
		if getErr != nil {
			return v1alpha1.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover StatefulSet",
				fmt.Sprintf("affected Pod %s/%s is unavailable", pod.Namespace, name),
				getErr,
			)
		}

		if candidate.Status.Phase != corev1.PodRunning || !kube.PodReady(candidate) {
			return v1alpha1.WorkloadSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"discover StatefulSet",
				fmt.Sprintf("affected Pod %s/%s must be Running and Ready", pod.Namespace, name),
			)
		}

		if err := validatePodController(
			candidate,
			objectReference(
				domain.AppsAPIVersion,
				domain.KindStatefulSet,
				sts.Namespace,
				sts.Name,
				sts.UID,
				sts.ResourceVersion,
			),
			"discover StatefulSet",
		); err != nil {
			return v1alpha1.WorkloadSpec{}, err
		}

		if isLeaderRole(podRole(candidate)) && !allowLeaderDowntime {
			return v1alpha1.WorkloadSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"discover StatefulSet",
				fmt.Sprintf(
					"scale-down affects %s with role %s; complete an application switchover and pass --allow-leader-downtime",
					name,
					podRole(candidate),
				),
			)
		}

		affected = append(affected, podReference(candidate))
	}

	return v1alpha1.WorkloadSpec{
		Adapter: v1alpha1.WorkloadStatefulSet,
		Pod:     workloadPodReference(pod),
		Controller: workloadObjectReference(
			domain.AppsAPIVersion,
			domain.KindStatefulSet,
			sts.Name,
			sts.UID,
			sts.ResourceVersion,
		),
		OriginalReplicas: &replicas,
		Ordinal:          &ordinal,
		AffectedPods:     localWorkloadReferences(affected),
	}, nil
}

func (m *Manager) victoriaLogsWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	sts *appsv1.StatefulSet,
) (v1alpha1.WorkloadSpec, error) {
	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		sts.Namespace,
		domain.KindStatefulSet,
		sts.Name,
		"discover Victoria Logs",
	); err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	replicas := statefulSetReplicas(sts)
	if policy := sts.Spec.PersistentVolumeClaimRetentionPolicy; policy != nil &&
		policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Victoria Logs",
			fmt.Sprintf("PVC retention whenScaled is %s", policy.WhenScaled),
		)
	}

	affected := make([]v1alpha1.ObjectReference, 0, replicas)

	names := make([]string, 0, replicas)
	for ordinal := range replicas {
		names = append(names, fmt.Sprintf("%s-%d", sts.Name, ordinal))
	}

	candidates, getErrors := m.readPods(ctx, pod.Namespace, names)
	for index, name := range names {
		candidate, err := candidates[index], getErrors[index]
		if err != nil {
			return v1alpha1.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover Victoria Logs",
				fmt.Sprintf("affected Pod %s/%s is unavailable", pod.Namespace, name),
				err,
			)
		}

		if candidate.Status.Phase != corev1.PodRunning || !kube.PodReady(candidate) {
			return v1alpha1.WorkloadSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"discover Victoria Logs",
				fmt.Sprintf("affected Pod %s/%s must be Running and Ready", pod.Namespace, name),
			)
		}

		if err := validatePodController(
			candidate,
			objectReference(
				domain.AppsAPIVersion,
				domain.KindStatefulSet,
				sts.Namespace,
				sts.Name,
				sts.UID,
				sts.ResourceVersion,
			),
			"discover Victoria Logs",
		); err != nil {
			return v1alpha1.WorkloadSpec{}, err
		}

		affected = append(affected, podReference(candidate))
	}

	zero := int32(0)

	return v1alpha1.WorkloadSpec{
		Adapter: v1alpha1.WorkloadVictoriaLogs,
		Pod:     workloadPodReference(pod),
		Controller: workloadObjectReference(
			domain.AppsAPIVersion,
			domain.KindStatefulSet,
			sts.Name,
			sts.UID,
			sts.ResourceVersion,
		),
		OriginalReplicas: &replicas,
		Ordinal:          &zero,
		AffectedPods:     localWorkloadReferences(affected),
	}, nil
}

func (m *Manager) patchStatefulSetReplicas(
	ctx context.Context,
	ref v1alpha1.ObjectReference,
	replicas int32,
	allowedCurrent ...int32,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if sts.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"scale StatefulSet",
				fmt.Sprintf("StatefulSet %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		current := int32(1)
		if sts.Spec.Replicas != nil {
			current = *sts.Spec.Replicas
		}

		if current == replicas {
			return nil
		}

		allowed := slices.Contains(allowedCurrent, current)

		if !allowed {
			return domain.NewError(
				domain.ErrorConflict,
				"scale StatefulSet",
				fmt.Sprintf(
					"StatefulSet %s/%s replicas changed to %d",
					ref.Namespace,
					ref.Name,
					current,
				),
			)
		}

		sts.Spec.Replicas = &replicas
		_, err = m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Update(ctx, sts, metav1.UpdateOptions{})

		return err
	})
}

func (m *Manager) validateStatefulSetResumed(
	ctx context.Context,
	controller v1alpha1.ObjectReference,
	originalReplicas *int32,
	operation string,
) error {
	if originalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			operation,
			"session lacks StatefulSet replica state",
		)
	}

	sts, err := m.typed.AppsV1().
		StatefulSets(controller.Namespace).
		Get(ctx, controller.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, operation, "read StatefulSet", err)
	}

	if sts.UID != controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf("StatefulSet %s/%s UID changed", sts.Namespace, sts.Name),
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		sts.Namespace,
		domain.KindStatefulSet,
		sts.Name,
		operation,
	); err != nil {
		return err
	}

	if replicas := statefulSetReplicas(sts); replicas != *originalReplicas {
		return domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf(
				"StatefulSet %s/%s replicas changed to %d while restoring %d replicas",
				sts.Namespace,
				sts.Name,
				replicas,
				*originalReplicas,
			),
		)
	}

	return nil
}

func (m *Manager) pauseStatefulSet(
	ctx context.Context,
	controller v1alpha1.ObjectReference,
	originalReplicas, ordinal *int32,
	affectedPods []v1alpha1.ObjectReference,
) error {
	if ordinal == nil || originalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause StatefulSet",
			"session lacks replica state",
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		controller.Namespace,
		domain.KindStatefulSet,
		controller.Name,
		"pause StatefulSet",
	); err != nil {
		return err
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		controller,
		*ordinal,
		*originalReplicas,
	); err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}
		return domain.WrapError(domain.ErrorKubernetes, "pause StatefulSet", "scale down", err)
	}

	for _, pod := range affectedPods {
		if err := m.waitForPodDeletion(ctx, pod, "pause StatefulSet"); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) pauseVictoriaLogs(
	ctx context.Context,
	workflowID string,
	controller v1alpha1.ObjectReference,
	originalReplicas *int32,
	affectedPods []v1alpha1.ObjectReference,
) error {
	if controller.Kind != domain.KindStatefulSet || originalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause Victoria Logs",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		controller.Namespace,
		domain.KindStatefulSet,
		controller.Name,
		"pause Victoria Logs",
	); err != nil {
		return err
	}

	if err := m.patchVictoriaLogsReplicas(ctx, workflowID, controller,
		*originalReplicas, 0, false); err != nil {
		return err
	}

	for _, pod := range affectedPods {
		if err := m.waitForPodDeletion(ctx, pod, "pause Victoria Logs"); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) resumeStatefulSet(
	ctx context.Context,
	controller v1alpha1.ObjectReference,
	originalReplicas, ordinal *int32,
	affectedPods []v1alpha1.ObjectReference,
) ([]v1alpha1.ObjectReference, error) {
	if originalReplicas == nil || ordinal == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"resume StatefulSet",
			"session lacks replica state",
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		controller.Namespace,
		domain.KindStatefulSet,
		controller.Name,
		"resume StatefulSet",
	); err != nil {
		return nil, err
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		controller,
		*originalReplicas,
		*ordinal,
	); err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return nil, err
		}

		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"resume StatefulSet",
			"restore replicas",
			err,
		)
	}

	observed, err := m.waitForResumedPods(ctx, affectedPods, controller, "resume StatefulSet")
	if err != nil {
		return observed, err
	}

	return observed, m.validateStatefulSetResumed(
		ctx,
		controller,
		originalReplicas,
		"resume StatefulSet",
	)
}

func (m *Manager) validateVictoriaLogsResume(
	ctx context.Context,
	workflowID string,
	controller v1alpha1.ObjectReference,
	originalReplicas *int32,
) error {
	if controller.Kind != domain.KindStatefulSet || originalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume Victoria Logs",
			"session lacks StatefulSet replica state",
		)
	}

	sts, err := m.typed.AppsV1().StatefulSets(controller.Namespace).
		Get(ctx, controller.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"resume Victoria Logs",
			"read StatefulSet",
			err,
		)
	}

	if sts.UID != controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"resume Victoria Logs",
			fmt.Sprintf("StatefulSet %s/%s UID changed", sts.Namespace, sts.Name),
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		sts.Namespace,
		domain.KindStatefulSet,
		sts.Name,
		"resume Victoria Logs",
	); err != nil {
		return err
	}

	if sts.Annotations[pauseSessionAnnotation] != workflowID {
		return domain.NewError(
			domain.ErrorConflict,
			"resume Victoria Logs",
			fmt.Sprintf("StatefulSet %s/%s pause ownership changed", sts.Namespace, sts.Name),
		)
	}

	return validateResumeReplicas(
		sts.Namespace,
		sts.Name,
		statefulSetReplicas(sts),
		*originalReplicas,
		0,
		"resume Victoria Logs",
		domain.KindStatefulSet,
	)
}

func (m *Manager) validateStatefulSetTransitionReplicas(
	ctx context.Context,
	controller v1alpha1.ObjectReference,
	originalReplicas, ordinal *int32,
	operation string,
) error {
	if originalReplicas == nil || ordinal == nil {
		return domain.NewError(
			domain.ErrorInternal,
			operation,
			"session lacks replica state",
		)
	}

	sts, err := m.typed.AppsV1().
		StatefulSets(controller.Namespace).
		Get(ctx, controller.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			operation,
			"read StatefulSet",
			err,
		)
	}

	if sts.UID != controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf("StatefulSet %s/%s UID changed", sts.Namespace, sts.Name),
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		sts.Namespace,
		domain.KindStatefulSet,
		sts.Name,
		operation,
	); err != nil {
		return err
	}

	return validateResumeReplicas(
		sts.Namespace,
		sts.Name,
		statefulSetReplicas(sts),
		*originalReplicas,
		*ordinal,
		operation,
		domain.KindStatefulSet,
	)
}

func (m *Manager) resumeVictoriaLogs(
	ctx context.Context,
	workflowID string,
	controller v1alpha1.ObjectReference,
	originalReplicas *int32,
	affectedPods []v1alpha1.ObjectReference,
) ([]v1alpha1.ObjectReference, error) {
	if controller.Kind != domain.KindStatefulSet || originalReplicas == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"resume Victoria Logs",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		controller.Namespace,
		domain.KindStatefulSet,
		controller.Name,
		"resume Victoria Logs",
	); err != nil {
		return nil, err
	}

	if err := m.patchVictoriaLogsReplicas(
		ctx,
		workflowID,
		controller,
		*originalReplicas,
		*originalReplicas,
		true,
	); err != nil {
		return nil, err
	}

	observed, err := m.waitForResumedPods(ctx, affectedPods, controller, "resume Victoria Logs")
	if err != nil {
		return observed, err
	}

	if err := m.validateStatefulSetResumed(
		ctx,
		controller,
		originalReplicas,
		"resume Victoria Logs",
	); err != nil {
		return observed, err
	}

	return observed, m.clearVictoriaLogsPauseOwner(ctx, workflowID, controller, originalReplicas)
}

func (m *Manager) patchVictoriaLogsReplicas(
	ctx context.Context,
	workflowID string,
	ref v1alpha1.ObjectReference,
	originalReplicas, replicas int32,
	resuming bool,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"Victoria Logs pause",
				"read StatefulSet",
				err,
			)
		}

		if sts.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs pause",
				fmt.Sprintf("StatefulSet %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		annotations := sts.GetAnnotations()

		owner := annotations[pauseSessionAnnotation]
		if owner != "" && owner != workflowID {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs pause",
				fmt.Sprintf(
					"StatefulSet %s/%s pause is owned by session %s",
					ref.Namespace,
					ref.Name,
					owner,
				),
			)
		}

		current := statefulSetReplicas(sts)
		if resuming {
			if owner != workflowID {
				return domain.NewError(
					domain.ErrorConflict,
					"Victoria Logs resume",
					fmt.Sprintf(
						"StatefulSet %s/%s is not owned by session %s",
						ref.Namespace,
						ref.Name,
						workflowID,
					),
				)
			}

			if current != 0 && current != replicas {
				return domain.NewError(
					domain.ErrorConflict,
					"Victoria Logs resume",
					fmt.Sprintf(
						"StatefulSet %s/%s replicas changed to %d",
						ref.Namespace,
						ref.Name,
						current,
					),
				)
			}
		} else {
			if owner == workflowID && current == replicas {
				return nil
			}

			if owner == "" && current != originalReplicas {
				return domain.NewError(
					domain.ErrorConflict,
					"Victoria Logs pause",
					fmt.Sprintf(
						"StatefulSet %s/%s replicas changed to %d",
						ref.Namespace,
						ref.Name,
						current,
					),
				)
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

			if annotations[pauseSessionAnnotation] != workflowID {
				annotations[pauseSessionAnnotation] = workflowID
				changed = true
			}
		}

		if !changed {
			return nil
		}

		sts.SetAnnotations(annotations)
		_, err = m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Update(ctx, sts, metav1.UpdateOptions{})

		return err
	})
}

func (m *Manager) clearVictoriaLogsPauseOwner(
	ctx context.Context,
	workflowID string,
	ref v1alpha1.ObjectReference,
	originalReplicas *int32,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"Victoria Logs resume",
				"read StatefulSet",
				err,
			)
		}

		if sts.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs resume",
				fmt.Sprintf("StatefulSet %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		annotations := sts.GetAnnotations()
		if annotations[pauseSessionAnnotation] != workflowID {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs resume",
				fmt.Sprintf("StatefulSet %s/%s pause ownership changed", ref.Namespace, ref.Name),
			)
		}

		if originalReplicas == nil || statefulSetReplicas(sts) != *originalReplicas {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs resume",
				fmt.Sprintf(
					"StatefulSet %s/%s replicas changed while pause ownership was active",
					ref.Namespace,
					ref.Name,
				),
			)
		}

		delete(annotations, pauseSessionAnnotation)
		sts.SetAnnotations(annotations)
		_, err = m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Update(ctx, sts, metav1.UpdateOptions{})

		return err
	})
}
