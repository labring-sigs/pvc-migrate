package app

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Transfer charts use a 30-second Pod termination grace period. Allow that
// grace period and asynchronous garbage collection to finish before retrying.
const copyToolCleanupTimeout = 90 * time.Second

func validateCopyConsumersFromPods(
	online bool,
	source v1alpha1.ObjectReference,
	accessModes []corev1.PersistentVolumeAccessMode,
	pods []corev1.Pod,
	listErr error,
	sourceNode string,
) (string, error) {
	if listErr != nil {
		return "", domain.WrapError(
			domain.ErrorKubernetes,
			"copy preflight",
			"list PVC consumers in "+source.Namespace,
			listErr,
		)
	}

	active := make([]*corev1.Pod, 0)
	nodes := map[string]struct{}{}

	scheduledCount := 0
	for index := range pods {
		pod := &pods[index]
		if !kube.ActivePodUsesPVC(pod, source.Name) {
			continue
		}

		active = append(active, pod)
		if pod.Spec.NodeName != "" {
			scheduledCount++
			nodes[pod.Spec.NodeName] = struct{}{}
		}
	}

	if len(active) == 0 {
		return "", nil
	}

	if !online {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"offline copy requires PVC %s/%s to have zero active Pod consumers",
				source.Namespace,
				source.Name,
			),
		)
	}

	if kube.HasAccessMode(accessModes, corev1.ReadWriteOncePod) {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"active RWOP PVC %s/%s cannot be warm-copied",
				source.Namespace,
				source.Name,
			),
		)
	}

	if kube.HasAccessMode(accessModes, corev1.ReadWriteOnce) &&
		scheduledCount != len(active) {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"every active consumer of RWO PVC %s/%s must be scheduled before online copy",
				source.Namespace,
				source.Name,
			),
		)
	}

	if len(nodes) > 1 {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"online copy consumers for PVC %s/%s moved across multiple nodes",
				source.Namespace,
				source.Name,
			),
		)
	}

	inferredSourceNode := ""
	for node := range nodes {
		inferredSourceNode = node
		if sourceNode != "" && node != sourceNode {
			return "", domain.NewError(
				domain.ErrorConflict,
				"copy preflight",
				fmt.Sprintf(
					"PVC %s/%s consumer runs on %s, session source node is %s",
					source.Namespace,
					source.Name,
					node,
					sourceNode,
				),
			)
		}
	}

	return inferredSourceNode, nil
}

func mergeToolLogError(copyErr, observedErr error) error {
	if copyErr == nil {
		return nil
	}
	return errors.Join(copyErr, observedErr)
}

func isDestinationNoSpaceError(err error) bool {
	if err == nil {
		return false
	}

	if copyengine.IsDestinationNoSpaceError(err) {
		return true
	}

	var visit func(error) bool

	visit = func(candidate error) bool {
		if candidate == nil {
			return false
		}

		if errors.Is(candidate, kube.ErrToolPodNoSpace) {
			return true
		}

		message := strings.ToLower(candidate.Error())
		if strings.Contains(message, "no space left on device") ||
			strings.Contains(message, "enospc") {
			return true
		}

		if joined, ok := candidate.(interface{ Unwrap() []error }); ok {
			return slices.ContainsFunc(joined.Unwrap(), visit)
		}

		return visit(errors.Unwrap(candidate))
	}

	return visit(err)
}

func destinationCapacityIsSmaller(sourceCapacity, destinationCapacity string) bool {
	source, sourceErr := resource.ParseQuantity(sourceCapacity)
	destination, destinationErr := resource.ParseQuantity(destinationCapacity)

	return sourceErr == nil && destinationErr == nil && destination.Cmp(source) < 0
}

func probedSourceNode(
	sourceNode string,
	source v1alpha1.ObjectReference,
	results []kube.ToolImageProbeResult,
) string {
	if sourceNode != "" {
		return sourceNode
	}

	for _, result := range results {
		if result.Target.Namespace == source.Namespace &&
			result.Target.PVCName == source.Name &&
			slices.Contains(result.Target.Components, kube.ToolComponentSSHD) {
			return result.NodeName
		}
	}

	return ""
}
