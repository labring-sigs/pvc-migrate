package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// rollbackDestinationConsumed reports whether any Running or Pending pod in
// the namespace mounts the destination PVC. After cutover, the destination
// PVC carries the workload's live data — a rollback that swaps the backing
// PV would silently disrupt those consumers.
func rollbackDestinationConsumed(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	pvcName string,
	sessionID string,
) error {
	pods, err := client.CoreV1().Pods(namespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes, "migration rollback",
			fmt.Sprintf("list pods in %s for destination consumer check", namespace), err,
		)
	}

	var consumers []string
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending {
			continue
		}
		if pod.Labels["app.kubernetes.io/managed-by"] == "pvc-migrate" &&
			pod.Labels["migrate.sealos.io/session"] == sessionID {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil &&
				volume.PersistentVolumeClaim.ClaimName == pvcName {
				consumers = append(consumers, pod.Name)
				break
			}
		}
	}

	if len(consumers) > 0 {
		return domain.NewError(
			domain.ErrorConflict,
			"migration rollback",
			fmt.Sprintf(
				"destination PVC %s/%s is still in use by pod(s) %v; "+
					"stop these consumers before rolling back",
				namespace, pvcName, consumers,
			),
		)
	}

	return nil
}
