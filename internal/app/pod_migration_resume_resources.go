package app

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func verifyResumedStandalonePod(
	ctx context.Context,
	client kubernetes.Interface,
	workflowID string,
	ref v1alpha1.ObjectReference,
	targetNode string,
) error {
	// TargetNode pins reservation and copy tools for every workload. The
	// standalone adapter also pins the recreated Pod; controller-managed
	// workloads retain their own scheduler policy and may validly land on a
	// different node when the destination volume is topology-independent.
	if targetNode != "" {
		pod, err := client.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				verifyMigrationPhase,
				"read resumed Pod",
				err,
			)
		}

		if ref.UID == "" || pod.UID != ref.UID || pod.Annotations[kube.SessionKey] != workflowID {
			return domain.NewError(
				domain.ErrorConflict,
				verifyMigrationPhase,
				fmt.Sprintf(
					"Pod %s/%s identity or session ownership changed",
					pod.Namespace,
					pod.Name,
				),
			)
		}

		if pod.Spec.NodeName != targetNode {
			return domain.NewError(
				domain.ErrorPrecondition,
				verifyMigrationPhase,
				fmt.Sprintf(
					"Pod %s/%s runs on %s, expected %s",
					pod.Namespace,
					pod.Name,
					pod.Spec.NodeName,
					targetNode,
				),
			)
		}
	}

	return nil
}

func (m *podMigrationResources) validateResumeNode(
	ctx context.Context,
	adapter v1alpha1.WorkloadKind,
	targetNode string,
) error {
	if adapter != v1alpha1.WorkloadStandalone || targetNode == "" {
		return nil
	}

	node, err := m.client.CoreV1().Nodes().Get(ctx, targetNode, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "resume workload", "read target node", err)
	}

	if !kube.NodeReadyAndSchedulable(node) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume workload",
			fmt.Sprintf("target node %s must be Ready and schedulable", node.Name),
		)
	}

	return nil
}
