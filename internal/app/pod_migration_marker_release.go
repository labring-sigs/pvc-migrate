package app

import (
	"context"
	"errors"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// releaseStandalonePodMarker drops the session ownership annotation from a
// migrated standalone Pod during terminal cleanup. The marker keeps in-flight
// resume validations honest; once the workflow is finalized it would only
// orphan the Pod against every future migration.
func releaseStandalonePodMarker(
	ctx context.Context,
	client kubernetes.Interface,
	workflowID string,
	pod v1alpha1.ObjectReference,
) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, getErr := client.CoreV1().
			Pods(pod.Namespace).
			Get(ctx, pod.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return nil
		}

		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"cleanup",
				fmt.Sprintf("read standalone Pod %s/%s", pod.Namespace, pod.Name),
				getErr,
			)
		}

		if current.Annotations[kube.SessionKey] != workflowID {
			return nil
		}

		delete(current.Annotations, kube.SessionKey)

		_, updateErr := client.CoreV1().
			Pods(pod.Namespace).
			Update(ctx, current, metav1.UpdateOptions{})

		return errors.Join(updateErr, checkpointFenceError(ctx))
	})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("release standalone Pod %s/%s", pod.Namespace, pod.Name),
			err,
		)
	}

	return nil
}

// releaseWorkloadMarkers clears terminal session markers from migrated
// workloads. Standalone Pods carry the session key directly; other adapters
// drop their pause annotations when the workload resumes.
func releaseWorkloadMarkers(
	ctx context.Context,
	client kubernetes.Interface,
	workflowID string,
	workload v1alpha1.WorkloadSpec,
	sourceNamespace string,
) error {
	if workload.Adapter != v1alpha1.WorkloadStandalone || workload.Pod == nil ||
		workload.Pod.Name == "" {
		return nil
	}

	return releaseStandalonePodMarker(
		ctx,
		client,
		workflowID,
		qualifiedResourceReference(*workload.Pod, sourceNamespace),
	)
}
