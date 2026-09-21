package controller

import (
	"context"
	"encoding/json"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func standaloneWorkload(pod *corev1.Pod) (v1alpha1.WorkloadSpec, error) {
	raw, err := json.Marshal(pod)
	if err != nil {
		return v1alpha1.WorkloadSpec{}, domain.WrapError(
			domain.ErrorInternal,
			"discover standalone Pod",
			"encode Pod",
			err,
		)
	}

	return v1alpha1.WorkloadSpec{
		Adapter:        v1alpha1.WorkloadStandalone,
		Pod:            workloadPodReference(pod),
		OriginalObject: &apiextensionsv1.JSON{Raw: raw},
	}, nil
}

func (m *Manager) pauseStandalone(ctx context.Context, ref v1alpha1.ObjectReference) error {
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
	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
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

func (m *Manager) resumeStandalone(
	ctx context.Context,
	workflowID string,
	ref *v1alpha1.ObjectReference,
	snapshot *apiextensionsv1.JSON,
	resumeNode string,
) error {
	existing, err := m.typed.CoreV1().
		Pods(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})

	var expectedUID types.UID
	if err == nil {
		if existing.Annotations[kube.SessionKey] != workflowID {
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
			*ref = podReference(existing)
			return nil
		}
	} else if !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "resume standalone Pod", "read Pod", err)
	}

	if snapshot == nil {
		return domain.NewError(
			domain.ErrorPrecondition, "resume standalone Pod", "saved Pod snapshot is required",
		)
	}

	var pod corev1.Pod
	if err := json.Unmarshal(snapshot.Raw, &pod); err != nil {
		return domain.WrapError(
			domain.ErrorInternal,
			"resume standalone Pod",
			"decode saved Pod",
			err,
		)
	}

	// The snapshot is controller-validated before this path runs. Reassert the
	// live workflow identity here as defense in depth so a stale or malformed
	// snapshot can never redirect Pod creation to another namespace or name.
	pod.Namespace = ref.Namespace
	pod.Name = ref.Name

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

	pod.Annotations[kube.SessionKey] = workflowID

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

		if existing.Annotations[kube.SessionKey] != workflowID {
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
	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	expectedUID = created.UID
	*ref = podReference(created)

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

			if current.Annotations[kube.SessionKey] != workflowID {
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

	*ref = podReference(ready)

	return nil
}

func (m *Manager) validateStandaloneResume(
	ctx context.Context,
	workflowID string,
	ref v1alpha1.ObjectReference,
) error {
	pod, err := m.typed.CoreV1().
		Pods(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
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

	if pod.Annotations[kube.SessionKey] != workflowID {
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
	workflowID string,
	ref v1alpha1.ObjectReference,
) ([]v1alpha1.ObjectReference, error) {
	const operation = validateRollbackConsumers

	pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, operation, "read standalone Pod", err)
	}

	if pod.Annotations[kube.SessionKey] != workflowID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			operation,
			fmt.Sprintf("Pod %s/%s was recreated outside this session", pod.Namespace, pod.Name),
		)
	}

	return []v1alpha1.ObjectReference{podReference(pod)}, nil
}
