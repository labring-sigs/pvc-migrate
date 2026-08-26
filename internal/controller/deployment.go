package controller

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (m *Manager) deploymentWorkload(
	ctx context.Context,
	selected *corev1.Pod,
	deployment *appsv1.Deployment,
) (domain.WorkloadSpec, error) {
	if err := rejectDeploymentControllerOwner(deployment, "discover Deployment"); err != nil {
		return domain.WorkloadSpec{}, err
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		deployment.Namespace,
		domain.KindDeployment,
		deployment.Name,
		"discover Deployment",
	); err != nil {
		return domain.WorkloadSpec{}, err
	}

	replicas := deploymentReplicas(deployment)
	if replicas <= 0 {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Deployment",
			fmt.Sprintf(
				"Deployment %s/%s has no positive replica count",
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	affected, err := m.readyDeploymentPods(ctx, deployment, replicas, "discover Deployment")
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	selectedFound := false

	for _, ref := range affected {
		if ref.UID == selected.UID {
			selectedFound = true
			break
		}
	}

	if !selectedFound {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover Deployment",
			fmt.Sprintf(
				"selected Pod %s/%s is no longer controlled by Deployment %s/%s",
				selected.Namespace,
				selected.Name,
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	return domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Pod:     podReference(selected),
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindDeployment,
			deployment.Namespace,
			deployment.Name,
			deployment.UID,
			deployment.ResourceVersion,
		),
		OriginalReplicas: &replicas,
		AffectedPods:     affected,
	}, nil
}

func (m *Manager) readyDeploymentPods(
	ctx context.Context,
	deployment *appsv1.Deployment,
	replicas int32,
	operation string,
) ([]domain.ObjectReference, error) {
	if deployment.Status.ObservedGeneration < deployment.Generation ||
		deployment.Status.Replicas != replicas ||
		deployment.Status.ReadyReplicas != replicas ||
		deployment.Status.AvailableReplicas != replicas ||
		deployment.Status.UpdatedReplicas != replicas ||
		deployment.Status.UnavailableReplicas != 0 {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			operation,
			fmt.Sprintf(
				"Deployment %s/%s must be fully rolled out with %d Ready replica(s)",
				deployment.Namespace,
				deployment.Name,
				replicas,
			),
		)
	}

	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorPrecondition,
			operation,
			"convert Deployment selector",
			err,
		)
	}

	pods, err := m.typed.CoreV1().Pods(deployment.Namespace).List(
		ctx,
		metav1.ListOptions{LabelSelector: selector.String()},
	)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			operation,
			"list Deployment Pods",
			err,
		)
	}

	ready := make([]domain.ObjectReference, 0, replicas)
	replicaSets := make(map[string]*appsv1.ReplicaSet)

	for index := range pods.Items {
		candidate := &pods.Items[index]

		owned, ownerErr := m.podControlledByDeployment(ctx, candidate, deployment, replicaSets)
		if ownerErr != nil {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				operation,
				"verify Deployment Pod ownership",
				ownerErr,
			)
		}

		if !owned {
			continue
		}

		if candidate.Status.Phase != corev1.PodRunning || !kube.PodReady(candidate) ||
			candidate.DeletionTimestamp != nil {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				operation,
				fmt.Sprintf(
					"Deployment Pod %s/%s must be Running and Ready",
					candidate.Namespace,
					candidate.Name,
				),
			)
		}

		ready = append(ready, podReference(candidate))
	}

	if len(ready) != int(replicas) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			operation,
			fmt.Sprintf(
				"Deployment %s/%s has %d owned Ready Pod(s), expected %d",
				deployment.Namespace,
				deployment.Name,
				len(ready),
				replicas,
			),
		)
	}

	sort.Slice(ready, func(i, j int) bool { return ready[i].Name < ready[j].Name })

	return ready, nil
}

func (m *Manager) podControlledByDeployment(
	ctx context.Context,
	pod *corev1.Pod,
	deployment *appsv1.Deployment,
	cache map[string]*appsv1.ReplicaSet,
) (bool, error) {
	owner := controllerOwner(pod.OwnerReferences)
	if owner == nil || owner.APIVersion != domain.AppsAPIVersion ||
		owner.Kind != domain.KindReplicaSet || owner.Name == "" {
		return false, nil
	}

	replicaSet, found := cache[owner.Name]
	if !found {
		var err error

		replicaSet, err = m.typed.AppsV1().ReplicaSets(pod.Namespace).
			Get(ctx, owner.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			replicaSet = nil
		} else if err != nil {
			return false, err
		}

		cache[owner.Name] = replicaSet
	}

	if replicaSet == nil || owner.UID == "" || replicaSet.UID == "" || replicaSet.UID != owner.UID {
		return false, nil
	}

	return sameControllerOwner(controllerOwner(replicaSet.OwnerReferences), &metav1.OwnerReference{
		APIVersion: domain.AppsAPIVersion,
		Kind:       domain.KindDeployment,
		Name:       deployment.Name,
		UID:        deployment.UID,
	}), nil
}

func rejectDeploymentControllerOwner(deployment *appsv1.Deployment, operation string) error {
	owner := controllerOwner(deployment.OwnerReferences)
	if owner == nil {
		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		operation,
		fmt.Sprintf(
			"Deployment %s/%s is controlled by %s %s and cannot be safely scaled directly",
			deployment.Namespace,
			deployment.Name,
			owner.Kind,
			owner.Name,
		),
	)
}

func (m *Manager) readUnmanagedDeployment(
	ctx context.Context,
	ref domain.ObjectReference,
	operation string,
) (*appsv1.Deployment, error) {
	deployment, err := m.readDeployment(ctx, ref, operation)
	if err != nil {
		return nil, err
	}

	if err := rejectDeploymentControllerOwner(deployment, operation); err != nil {
		return nil, err
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

func (m *Manager) readDeployment(
	ctx context.Context,
	ref domain.ObjectReference,
	operation string,
) (*appsv1.Deployment, error) {
	deployment, err := m.typed.AppsV1().Deployments(ref.Namespace).Get(
		ctx,
		ref.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			operation,
			"read Deployment",
			err,
		)
	}

	if deployment.UID != ref.UID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf("Deployment %s/%s UID changed", ref.Namespace, ref.Name),
		)
	}

	return deployment, nil
}

func (m *Manager) verifyDeploymentPaused(ctx context.Context, workload domain.WorkloadSpec) error {
	if workload.Controller.Kind != domain.KindDeployment || workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"Deployment session lacks controller and replica state",
		)
	}

	deployment, err := m.readUnmanagedDeployment(
		ctx,
		workload.Controller,
		"verify paused",
	)
	if err != nil {
		return err
	}

	if replicas := deploymentReplicas(deployment); replicas != 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf(
				"Deployment %s/%s replicas=%d, expected 0 while paused",
				deployment.Namespace,
				deployment.Name,
				replicas,
			),
		)
	}

	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"verify paused",
			"convert Deployment selector",
			err,
		)
	}

	pods, err := m.typed.CoreV1().
		Pods(deployment.Namespace).
		List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify paused",
			"list Deployment Pods",
			err,
		)
	}

	replicaSets := make(map[string]*appsv1.ReplicaSet)
	for index := range pods.Items {
		owned, ownerErr := m.podControlledByDeployment(
			ctx,
			&pods.Items[index],
			deployment,
			replicaSets,
		)
		if ownerErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"verify paused",
				"verify Deployment Pod ownership",
				ownerErr,
			)
		}

		if owned {
			return domain.NewError(
				domain.ErrorPrecondition,
				"verify paused",
				fmt.Sprintf(
					"Deployment Pod %s/%s is still present",
					deployment.Namespace,
					pods.Items[index].Name,
				),
			)
		}
	}

	return nil
}

func (m *Manager) pauseDeployment(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause Deployment",
			"session lacks replica state",
		)
	}

	deployment, err := m.readUnmanagedDeployment(
		ctx,
		workload.Controller,
		"pause Deployment",
	)
	if err != nil {
		return err
	}

	if deploymentReplicas(deployment) == *workload.OriginalReplicas {
		if session.Status.Phase == domain.PhaseRollingBack {
			currentPods, _, observeErr := m.observeDeploymentPods(ctx, deployment)
			if observeErr != nil {
				return observeErr
			}

			updateDeploymentPodReferences(session.Spec.WorkloadPtr(), currentPods)
		} else {
			currentPods, readyErr := m.readyDeploymentPods(
				ctx,
				deployment,
				*workload.OriginalReplicas,
				"pause Deployment",
			)
			if readyErr != nil {
				return readyErr
			}

			if !samePodIdentitySet(workload.AffectedPods, currentPods) {
				return domain.NewError(
					domain.ErrorConflict,
					"pause Deployment",
					fmt.Sprintf(
						"Deployment %s/%s Pod set changed after planning",
						deployment.Namespace,
						deployment.Name,
					),
				)
			}
		}
	}

	if err := m.updateDeploymentReplicas(
		ctx,
		deployment,
		"pause Deployment",
		0,
		*workload.OriginalReplicas,
	); err != nil {
		return workloadScaleError("pause Deployment", "scale down", err)
	}

	for _, ref := range session.Spec.Workload().AffectedPods {
		if err := m.waitForPodDeletion(ctx, ref, "pause Deployment"); err != nil {
			return err
		}
	}

	return nil
}

func samePodIdentitySet(expected, current []domain.ObjectReference) bool {
	if len(expected) != len(current) {
		return false
	}

	identities := make(map[string]types.UID, len(expected))
	for _, ref := range expected {
		key := ref.Namespace + "/" + ref.Name
		if ref.UID == "" || identities[key] != "" {
			return false
		}

		identities[key] = ref.UID
	}

	for _, ref := range current {
		expectedUID, found := identities[ref.Namespace+"/"+ref.Name]
		if !found || ref.UID == "" || expectedUID != ref.UID {
			return false
		}
	}

	return true
}

func (m *Manager) resumeDeployment(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume Deployment",
			"session lacks replica state",
		)
	}

	deployment, err := m.readUnmanagedDeployment(
		ctx,
		workload.Controller,
		"resume Deployment",
	)
	if err != nil {
		return err
	}

	if err := m.updateDeploymentReplicas(
		ctx,
		deployment,
		"resume Deployment",
		*workload.OriginalReplicas,
		0,
	); err != nil {
		return workloadScaleError("resume Deployment", "restore replicas", err)
	}

	return m.waitForDeploymentReady(
		ctx,
		session,
		workload.Controller,
		*workload.OriginalReplicas,
	)
}

func (m *Manager) validateDeploymentResume(
	ctx context.Context,
	session *domain.Session,
) error {
	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume Deployment",
			"session lacks replica state",
		)
	}

	deployment, err := m.readUnmanagedDeployment(
		ctx,
		workload.Controller,
		"resume Deployment",
	)
	if err != nil {
		return err
	}

	if err := m.rejectHorizontalPodAutoscaler(
		ctx,
		deployment.Namespace,
		domain.KindDeployment,
		deployment.Name,
		"resume Deployment",
	); err != nil {
		return err
	}

	return validateResumeReplicas(
		deployment.Namespace,
		deployment.Name,
		deploymentReplicas(deployment),
		*workload.OriginalReplicas,
		0,
		"resume Deployment",
		domain.KindDeployment,
	)
}

func (m *Manager) currentDeploymentRollbackPods(
	ctx context.Context,
	session *domain.Session,
) ([]domain.ObjectReference, error) {
	const operation = validateRollbackConsumers

	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			operation,
			"session lacks Deployment replica state",
		)
	}

	deployment, err := m.readUnmanagedDeployment(ctx, workload.Controller, operation)
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

func validateResumeReplicas(
	namespace string,
	name string,
	current int32,
	original int32,
	paused int32,
	operation string,
	kind string,
) error {
	if current == original || current == paused {
		return nil
	}

	return domain.NewError(
		domain.ErrorConflict,
		operation,
		fmt.Sprintf(
			"%s %s/%s replicas changed to %d while restoring %d replicas",
			kind,
			namespace,
			name,
			current,
			original,
		),
	)
}

func (m *Manager) waitForDeploymentReady(
	ctx context.Context,
	session *domain.Session,
	ref domain.ObjectReference,
	replicas int32,
) error {
	return m.waitFor(
		ctx,
		fmt.Sprintf("Deployment %s/%s readiness", ref.Namespace, ref.Name),
		func(waitCtx context.Context) (bool, error) {
			deployment, err := m.readDeployment(
				waitCtx,
				ref,
				"resume Deployment",
			)
			if err != nil {
				return false, err
			}

			if err := rejectDeploymentControllerOwner(deployment, "resume Deployment"); err != nil {
				return false, err
			}

			if currentReplicas := deploymentReplicas(deployment); currentReplicas != replicas {
				return false, domain.NewError(
					domain.ErrorConflict,
					"resume Deployment",
					fmt.Sprintf(
						"Deployment %s/%s replicas changed to %d while restoring %d replicas",
						deployment.Namespace,
						deployment.Name,
						currentReplicas,
						replicas,
					),
				)
			}

			current, allReady, err := m.observeDeploymentPods(waitCtx, deployment)
			if err != nil {
				return false, err
			}

			updateDeploymentPodReferences(session.Spec.WorkloadPtr(), current)

			if err := m.rejectHorizontalPodAutoscaler(
				waitCtx,
				deployment.Namespace,
				domain.KindDeployment,
				deployment.Name,
				"resume Deployment",
			); err != nil {
				return false, err
			}

			if !allReady || len(current) != int(replicas) ||
				deployment.Status.ObservedGeneration < deployment.Generation ||
				deployment.Status.Replicas != replicas || deployment.Status.ReadyReplicas != replicas ||
				deployment.Status.AvailableReplicas != replicas || deployment.Status.UpdatedReplicas != replicas ||
				deployment.Status.UnavailableReplicas != 0 {
				return false, nil
			}

			return true, nil
		},
	)
}

func (m *Manager) observeDeploymentPods(
	ctx context.Context,
	deployment *appsv1.Deployment,
) ([]domain.ObjectReference, bool, error) {
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, false, domain.WrapError(
			domain.ErrorPrecondition,
			"observe Deployment Pods",
			"convert Deployment selector",
			err,
		)
	}

	pods, err := m.typed.CoreV1().Pods(deployment.Namespace).List(
		ctx,
		metav1.ListOptions{LabelSelector: selector.String()},
	)
	if err != nil {
		return nil, false, domain.WrapError(
			domain.ErrorKubernetes,
			"observe Deployment Pods",
			"list Deployment Pods",
			err,
		)
	}

	current := make([]domain.ObjectReference, 0, len(pods.Items))
	allReady := true
	replicaSets := make(map[string]*appsv1.ReplicaSet)

	for index := range pods.Items {
		pod := &pods.Items[index]

		owned, ownerErr := m.podControlledByDeployment(ctx, pod, deployment, replicaSets)
		if ownerErr != nil {
			return nil, false, domain.WrapError(
				domain.ErrorKubernetes,
				"observe Deployment Pods",
				"verify Deployment Pod ownership",
				ownerErr,
			)
		}

		if !owned {
			continue
		}

		current = append(current, podReference(pod))
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning ||
			!kube.PodReady(pod) {
			allReady = false
		}
	}

	sort.Slice(current, func(i, j int) bool { return current[i].Name < current[j].Name })

	return current, allReady, nil
}

func updateDeploymentPodReferences(
	workload *domain.WorkloadSpec,
	current []domain.ObjectReference,
) {
	if workload == nil || len(current) == 0 {
		return
	}

	workload.AffectedPods = slices.Clone(current)
	workload.Pod = current[0]
}

func (m *Manager) updateDeploymentReplicas(
	ctx context.Context,
	deployment *appsv1.Deployment,
	operation string,
	replicas int32,
	allowedCurrent ...int32,
) error {
	if deployment == nil || deployment.Namespace == "" ||
		deployment.Name == "" || deployment.UID == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			operation,
			"Kubernetes returned an incomplete Deployment identity",
		)
	}

	current := deploymentReplicas(deployment)
	if current == replicas {
		return nil
	}

	if !slices.Contains(allowedCurrent, current) {
		return domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf(
				"Deployment %s/%s replicas changed to %d",
				deployment.Namespace,
				deployment.Name,
				current,
			),
		)
	}

	updated := deployment.DeepCopy()
	updated.Spec.Replicas = &replicas

	_, err := m.typed.AppsV1().Deployments(updated.Namespace).Update(
		ctx,
		updated,
		metav1.UpdateOptions{},
	)
	if apierrors.IsConflict(err) {
		return domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf(
				"Deployment %s/%s changed after validation for %s",
				updated.Namespace,
				updated.Name,
				operation,
			),
		)
	}

	return err
}
