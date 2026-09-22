package kube

import (
	"context"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// RequireTransferToolsStopped retains recovery records while a tool still mounts
// the claim. Attempt IDs cannot safely identify which workflow owns an orphan.
func RequireTransferToolsStopped(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, claim string,
) error {
	usesClaim := func(labels map[string]string, volumes []corev1.Volume) bool {
		if !strings.HasPrefix(labels[AppInstanceLabel], "pv-migrate-") {
			return false
		}

		switch labels[AppComponentLabel] {
		case ToolComponentSSHD, ToolComponentRsync, ToolComponentRclone:
		default:
			return false
		}

		for _, volume := range volumes {
			if volume.PersistentVolumeClaim != nil &&
				volume.PersistentVolumeClaim.ClaimName == claim {
				return true
			}
		}

		return false
	}

	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, pod := range pods.Items {
		if usesClaim(pod.Labels, pod.Spec.Volumes) {
			return transferToolStillExists("Pod", namespace, pod.Name)
		}
	}

	jobs, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, job := range jobs.Items {
		if usesClaim(job.Spec.Template.Labels, job.Spec.Template.Spec.Volumes) {
			return transferToolStillExists("Job", namespace, job.Name)
		}
	}

	return nil
}

func transferToolStillExists(kind, namespace, name string) error {
	return domain.NewError(
		domain.ErrorPrecondition,
		"finalize workflow",
		fmt.Sprintf(
			"transfer %s %s/%s still exists; wait for transfer cleanup or inspect its orphan Helm release before removing it",
			kind,
			namespace,
			name,
		),
	)
}
