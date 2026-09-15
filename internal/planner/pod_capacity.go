package planner

import (
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func checkPodMigrationCapacity(
	result checkRecorder,
	pvc *corev1.PersistentVolumeClaim,
	sourceCapacity resource.Quantity,
	requestedCapacity string,
	kubeBlocksWorkload bool,
	kubeBlocks *v1alpha1.KubeBlocksSpec,
) bool {
	if pvc == nil || (!kubeBlocksWorkload && !isKubeBlocksPVC(pvc)) || requestedCapacity == "" {
		return true
	}

	destination, err := resource.ParseQuantity(requestedCapacity)
	if err != nil || destination.Sign() <= 0 || destination.Cmp(sourceCapacity) == 0 {
		return true
	}

	owner := "the KubeBlocks Cluster component"
	if kubeBlocks != nil && kubeBlocks.Cluster != "" && kubeBlocks.Component != "" {
		owner = fmt.Sprintf("Cluster %s component %s", kubeBlocks.Cluster, kubeBlocks.Component)
	}

	result.AddCheck(failed(domain.CheckNameDestinationCapacity, fmt.Sprintf(
		"KubeBlocks real-time migration cannot override destination capacity for PVC %s/%s; update %s volumeClaimTemplates storage request",
		pvc.Namespace,
		pvc.Name,
		owner,
	)))

	return false
}
