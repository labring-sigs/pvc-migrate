package controller

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func standaloneWorkload(pod *corev1.Pod) (domain.WorkloadSpec, error) {
	raw, err := json.Marshal(pod)
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorInternal,
			"discover standalone Pod",
			"encode Pod",
			err,
		)
	}

	return domain.WorkloadSpec{
		Adapter:        domain.WorkloadStandalone,
		Pod:            podReference(pod),
		OriginalObject: raw,
	}, nil
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

	if pod.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"pause standalone Pod",
			fmt.Sprintf("Pod %s/%s UID changed", ref.Namespace, ref.Name),
		)
	}

	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}
	if err := m.typed.CoreV1().
		Pods(ref.Namespace).
		Delete(ctx, ref.Name, options); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "pause standalone Pod", "delete Pod", err)
	}

	return m.waitFor(
		ctx,
		fmt.Sprintf("Pod %s/%s deletion", ref.Namespace, ref.Name),
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
					"pause standalone Pod",
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

func (m *Manager) resumeStandalone(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	existing, err := m.typed.CoreV1().
		Pods(workload.Pod.Namespace).
		Get(ctx, workload.Pod.Name, metav1.GetOptions{})

	var expectedUID types.UID
	if err == nil {
		if existing.Annotations[kube.SessionKey] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"resume standalone Pod",
				fmt.Sprintf(
					"Pod %s/%s was recreated outside this session",
					existing.Namespace,
					existing.Name,
				),
			)
		}

		expectedUID = existing.UID
		if kube.PodReady(existing) {
			session.Spec.WorkloadPtr().Pod = podReference(existing)
			return nil
		}
	} else if !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "resume standalone Pod", "read Pod", err)
	}

	var pod corev1.Pod
	if err := json.Unmarshal(workload.OriginalObject, &pod); err != nil {
		return domain.WrapError(
			domain.ErrorInternal,
			"resume standalone Pod",
			"decode saved Pod",
			err,
		)
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
	if session.Status.Phase == domain.PhaseRollingBack ||
		session.Status.Phase == domain.PhaseAborting {
		resumeNode = options.SourceNode
	}

	if resumeNode != "" {
		node, getErr := m.typed.CoreV1().Nodes().Get(ctx, resumeNode, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"resume standalone Pod",
				"read resume node",
				getErr,
			)
		}

		hostname := node.Labels[corev1.LabelHostname]
		if hostname == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume standalone Pod",
				fmt.Sprintf("node %s lacks kubernetes.io/hostname", resumeNode),
			)
		}

		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = map[string]string{}
		}

		pod.Spec.NodeSelector[corev1.LabelHostname] = hostname
	}

	created, err := m.typed.CoreV1().Pods(pod.Namespace).Create(ctx, &pod, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"resume standalone Pod",
				"create Pod",
				err,
			)
		}
		// The initial Get and Create are a TOCTOU window. Revalidate ownership
		// after AlreadyExists so an unrelated actor cannot be adopted.
		existing, getErr := m.typed.CoreV1().
			Pods(pod.Namespace).
			Get(ctx, pod.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"resume standalone Pod",
				"read concurrently created Pod",
				getErr,
			)
		}

		if existing.Annotations[kube.SessionKey] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"resume standalone Pod",
				fmt.Sprintf(
					"Pod %s/%s was created outside this session",
					existing.Namespace,
					existing.Name,
				),
			)
		}

		created = existing
	}

	if created == nil || created.Name == "" || created.UID == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"resume standalone Pod",
			fmt.Sprintf("create Pod %s/%s returned an empty object", pod.Namespace, pod.Name),
		)
	}

	expectedUID = created.UID

	var ready *corev1.Pod
	if err := m.waitFor(
		ctx,
		fmt.Sprintf("Pod %s/%s readiness", pod.Namespace, pod.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := m.typed.CoreV1().
				Pods(pod.Namespace).
				Get(waitCtx, pod.Name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}

			if current.Annotations[kube.SessionKey] != session.ID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"resume standalone Pod",
					fmt.Sprintf(
						"Pod %s/%s ownership changed while waiting for readiness",
						current.Namespace,
						current.Name,
					),
				)
			}

			if current.UID != expectedUID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"resume standalone Pod",
					fmt.Sprintf(
						"Pod %s/%s was replaced while waiting for readiness",
						current.Namespace,
						current.Name,
					),
				)
			}

			if kube.PodReady(current) {
				ready = current
				return true, nil
			}

			return false, nil
		},
	); err != nil {
		return err
	}

	session.Spec.WorkloadPtr().Pod = podReference(ready)

	return nil
}

func (m *Manager) validateStandaloneResume(
	ctx context.Context,
	session *domain.Session,
) error {
	workload := session.Spec.Workload()

	pod, err := m.typed.CoreV1().
		Pods(workload.Pod.Namespace).
		Get(ctx, workload.Pod.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"resume standalone Pod",
			"read Pod",
			err,
		)
	}

	if pod.Annotations[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"resume standalone Pod",
			fmt.Sprintf(
				"Pod %s/%s was recreated outside this session",
				pod.Namespace,
				pod.Name,
			),
		)
	}

	return nil
}

func (m *Manager) currentStandaloneRollbackPods(
	ctx context.Context,
	session *domain.Session,
) ([]domain.ObjectReference, error) {
	const operation = validateRollbackConsumers

	ref := session.Spec.Workload().Pod

	pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, operation, "read standalone Pod", err)
	}

	if pod.Annotations[kube.SessionKey] != session.ID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf("Pod %s/%s was recreated outside this session", pod.Namespace, pod.Name),
		)
	}

	return []domain.ObjectReference{podReference(pod)}, nil
}
