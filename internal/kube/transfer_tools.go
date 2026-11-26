package kube

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ErrTransferToolStillExists reports a transfer tool resource that is still
// present. An interrupted run's detached cleanup may still be converging the
// release, so callers that fence on this condition should retry rather than
// treat the presence as durable.
var ErrTransferToolStillExists = errors.New("transfer tool still exists")

// TransferToolCheck selects which workload controllers could recreate a Pod
// mounting the claim. Pods are always checked.
type TransferToolCheck struct {
	OperationID string
	Jobs        bool
	Deployments bool
}

// RequireTransferToolsStopped checks rclone Pods and Jobs before finalization.
func RequireTransferToolsStopped(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, claim string,
) error {
	return (TransferToolCheck{Jobs: true}).RequireStopped(ctx, client, namespace, claim)
}

func (check TransferToolCheck) RequireStopped(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, claim string,
) error {
	prefix := "pv-migrate-"
	if check.OperationID != "" {
		prefix += check.OperationID + "-"
	}

	usesClaim := func(labels map[string]string, volumes []corev1.Volume) bool {
		if !strings.HasPrefix(labels[AppInstanceLabel], prefix) {
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

	if check.Deployments {
		deployments, err := client.AppsV1().
			Deployments(namespace).
			List(ctx, metav1.ListOptions{LabelSelector: AppComponentLabel + "=" + ToolComponentSSHD})
		if err != nil {
			return err
		}

		for _, deployment := range deployments.Items {
			if usesClaim(deployment.Spec.Template.Labels, deployment.Spec.Template.Spec.Volumes) {
				return transferToolStillExists("Deployment", namespace, deployment.Name)
			}
		}
	}

	if check.Jobs {
		jobs, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}

		for _, job := range jobs.Items {
			if usesClaim(job.Spec.Template.Labels, job.Spec.Template.Spec.Volumes) {
				return transferToolStillExists("Job", namespace, job.Name)
			}
		}
	}

	return nil
}

func transferToolStillExists(kind, namespace, name string) error {
	return domain.WrapError(
		domain.ErrorPrecondition,
		"finalize workflow",
		fmt.Sprintf(
			"transfer %s %s/%s still exists; wait for transfer cleanup or inspect its orphan Helm release before removing it",
			kind,
			namespace,
			name,
		),
		ErrTransferToolStillExists,
	)
}
