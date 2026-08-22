package controller

import (
	"context"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

func (m *Manager) verifyStatefulSetPaused(
	ctx context.Context,
	workload domain.WorkloadSpec,
) error {
	if workload.Controller.Kind != domain.KindStatefulSet || workload.OriginalReplicas == nil ||
		workload.Ordinal == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"StatefulSet session lacks controller and replica state",
		)
	}

	sts, err := m.typed.AppsV1().
		StatefulSets(workload.Controller.Namespace).
		Get(ctx, workload.Controller.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read StatefulSet", err)
	}

	if sts.UID != workload.Controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("StatefulSet %s/%s UID changed", sts.Namespace, sts.Name),
		)
	}

	if replicas := statefulSetReplicas(sts); replicas != *workload.Ordinal {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf(
				"StatefulSet %s/%s replicas=%d, expected %d while paused",
				sts.Namespace,
				sts.Name,
				replicas,
				*workload.Ordinal,
			),
		)
	}

	return nil
}

func (m *Manager) verifyVictoriaLogsPaused(
	ctx context.Context,
	session *domain.Session,
	workload domain.WorkloadSpec,
) error {
	if workload.Controller.Kind != domain.KindStatefulSet {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"Victoria Logs session lacks StatefulSet controller state",
		)
	}

	sts, err := m.typed.AppsV1().
		StatefulSets(workload.Controller.Namespace).
		Get(ctx, workload.Controller.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify paused",
			"read Victoria Logs StatefulSet",
			err,
		)
	}

	if sts.UID != workload.Controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("Victoria Logs StatefulSet %s/%s UID changed", sts.Namespace, sts.Name),
		)
	}

	if sts.Annotations[pauseSessionAnnotation] != session.ID {
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
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	ordinal, err := podOrdinal(pod, sts.Name)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	if ordinal >= replicas {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover StatefulSet",
			fmt.Sprintf("Pod ordinal %d is outside replicas %d", ordinal, replicas),
		)
	}

	if policy := sts.Spec.PersistentVolumeClaimRetentionPolicy; policy != nil &&
		policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover StatefulSet",
			fmt.Sprintf("PVC retention whenScaled is %s", policy.WhenScaled),
		)
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
			return domain.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover StatefulSet",
				fmt.Sprintf("affected Pod %s/%s is unavailable", pod.Namespace, name),
				getErr,
			)
		}

		if candidate.Status.Phase != corev1.PodRunning || !podReady(candidate) {
			return domain.WorkloadSpec{}, domain.NewError(
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
			return domain.WorkloadSpec{}, err
		}

		if isLeaderRole(podRole(candidate)) && !options.AllowLeaderDowntime {
			return domain.WorkloadSpec{}, domain.NewError(
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

	return domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod:     podReference(pod),
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindStatefulSet,
			sts.Namespace,
			sts.Name,
			sts.UID,
			sts.ResourceVersion,
		),
		OriginalReplicas: &replicas,
		Ordinal:          &ordinal,
		AffectedPods:     affected,
	}, nil
}

func (m *Manager) victoriaLogsWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	sts *appsv1.StatefulSet,
) (domain.WorkloadSpec, error) {
	replicas := statefulSetReplicas(sts)
	if policy := sts.Spec.PersistentVolumeClaimRetentionPolicy; policy != nil &&
		policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Victoria Logs",
			fmt.Sprintf("PVC retention whenScaled is %s", policy.WhenScaled),
		)
	}

	affected := make([]domain.ObjectReference, 0, replicas)

	names := make([]string, 0, replicas)
	for ordinal := range replicas {
		names = append(names, fmt.Sprintf("%s-%d", sts.Name, ordinal))
	}

	candidates, getErrors := m.readPods(ctx, pod.Namespace, names)
	for index, name := range names {
		candidate, err := candidates[index], getErrors[index]
		if err != nil {
			return domain.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover Victoria Logs",
				fmt.Sprintf("affected Pod %s/%s is unavailable", pod.Namespace, name),
				err,
			)
		}

		if candidate.Status.Phase != corev1.PodRunning || !podReady(candidate) {
			return domain.WorkloadSpec{}, domain.NewError(
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
			return domain.WorkloadSpec{}, err
		}

		affected = append(affected, podReference(candidate))
	}

	zero := int32(0)

	return domain.WorkloadSpec{
		Adapter: domain.WorkloadVictoriaLogs,
		Pod:     podReference(pod),
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindStatefulSet,
			sts.Namespace,
			sts.Name,
			sts.UID,
			sts.ResourceVersion,
		),
		OriginalReplicas: &replicas,
		Ordinal:          &zero,
		AffectedPods:     affected,
	}, nil
}

func (m *Manager) patchStatefulSetReplicas(
	ctx context.Context,
	ref domain.ObjectReference,
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

func (m *Manager) pauseStatefulSet(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Ordinal == nil || workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause StatefulSet",
			"session lacks replica state",
		)
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		workload.Controller,
		*workload.Ordinal,
		*workload.OriginalReplicas,
	); err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}
		return domain.WrapError(domain.ErrorKubernetes, "pause StatefulSet", "scale down", err)
	}

	for _, pod := range workload.AffectedPods {
		if err := m.waitForPodDeletion(ctx, pod, "pause StatefulSet"); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) pauseVictoriaLogs(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Controller.Kind != domain.KindStatefulSet || workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause Victoria Logs",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.patchVictoriaLogsReplicas(ctx, session, 0, false); err != nil {
		return err
	}

	for _, pod := range workload.AffectedPods {
		if err := m.waitForPodDeletion(ctx, pod, "pause Victoria Logs"); err != nil {
			return err
		}
	}

	return m.VerifyPaused(ctx, session)
}

func (m *Manager) resumeStatefulSet(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil || workload.Ordinal == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume StatefulSet",
			"session lacks replica state",
		)
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		workload.Controller,
		*workload.OriginalReplicas,
		*workload.Ordinal,
	); err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"resume StatefulSet",
			"restore replicas",
			err,
		)
	}

	for _, ref := range workload.AffectedPods {
		if err := m.waitForResumedPod(
			ctx,
			session,
			ref,
			workload.Controller,
			"resume StatefulSet",
		); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) resumeVictoriaLogs(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Controller.Kind != domain.KindStatefulSet || workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume Victoria Logs",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.patchVictoriaLogsReplicas(
		ctx,
		session,
		*workload.OriginalReplicas,
		true,
	); err != nil {
		return err
	}

	for _, ref := range workload.AffectedPods {
		if err := m.waitForResumedPod(
			ctx,
			session,
			ref,
			workload.Controller,
			"resume Victoria Logs",
		); err != nil {
			return err
		}
	}

	return m.clearVictoriaLogsPauseOwner(ctx, session)
}

func (m *Manager) patchVictoriaLogsReplicas(
	ctx context.Context,
	session *domain.Session,
	replicas int32,
	resuming bool,
) error {
	ref := session.Spec.Workload().Controller

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
		if owner != "" && owner != session.ID {
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
			if owner != session.ID {
				return domain.NewError(
					domain.ErrorConflict,
					"Victoria Logs resume",
					fmt.Sprintf(
						"StatefulSet %s/%s is not owned by session %s",
						ref.Namespace,
						ref.Name,
						session.ID,
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
			if owner == session.ID && current == replicas {
				return nil
			}

			if owner == "" && current != *session.Spec.Workload().OriginalReplicas {
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

			if annotations[pauseSessionAnnotation] != session.ID {
				annotations[pauseSessionAnnotation] = session.ID
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

func (m *Manager) clearVictoriaLogsPauseOwner(ctx context.Context, session *domain.Session) error {
	ref := session.Spec.Workload().Controller

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
		if annotations[pauseSessionAnnotation] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs resume",
				fmt.Sprintf("StatefulSet %s/%s pause ownership changed", ref.Namespace, ref.Name),
			)
		}

		if session.Spec.Workload().OriginalReplicas == nil ||
			statefulSetReplicas(sts) != *session.Spec.Workload().OriginalReplicas {
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
