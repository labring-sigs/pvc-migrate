package kube

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// NodeReady reports whether the kubelet currently reports the node as Ready.
func NodeReady(node *corev1.Node) bool {
	if node == nil {
		return false
	}

	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

// NodeReadyAndSchedulable reports whether a node is Ready and not cordoned.
// Callers must separately account for taints when their Pod does not inject
// node-specific tolerations.
func NodeReadyAndSchedulable(node *corev1.Node) bool {
	return NodeReady(node) && !node.Spec.Unschedulable
}

// PodReady reports whether the Pod has a true Ready condition.
func PodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

// StorageClassAllowsNode reports whether a node satisfies one of the
// StorageClass allowed-topology terms.
func StorageClassAllowsNode(sc *storagev1.StorageClass, node *corev1.Node) bool {
	if sc == nil || node == nil {
		return false
	}

	if len(sc.AllowedTopologies) == 0 {
		return true
	}

	for _, term := range sc.AllowedTopologies {
		if len(term.MatchLabelExpressions) == 0 {
			continue
		}

		matches := true
		for _, expression := range term.MatchLabelExpressions {
			actual, exists := node.Labels[expression.Key]
			if !exists || !slices.Contains(expression.Values, actual) {
				matches = false
				break
			}
		}

		if matches {
			return true
		}
	}

	return false
}

// ValidateBoundVolumeCapacity checks capacity that Kubernetes has actually
// published on the PV. StorageClass allowVolumeExpansion only permits a PVC
// resize request; it does not make the requested bytes available until the PV
// capacity has caught up.
func ValidateBoundVolumeCapacity(
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
	required *resource.Quantity,
) error {
	if pv == nil {
		return errors.New("bound PV is unavailable")
	}

	provisioned, hasProvisioned := pv.Spec.Capacity[corev1.ResourceStorage]

	validProvisioned := hasProvisioned && provisioned.Sign() > 0
	if pvc != nil {
		requested, hasRequest := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

		if hasRequest && requested.Sign() > 0 {
			if !validProvisioned {
				return fmt.Errorf(
					"PVC %s/%s requests %s but bound PV %s has no positive storage capacity",
					pvc.Namespace,
					pvc.Name,
					requested.String(),
					pv.Name,
				)
			}

			if requested.Cmp(provisioned) > 0 {
				return fmt.Errorf(
					"PVC %s/%s requests %s but bound PV %s currently provides %s; volume expansion is incomplete or inconsistent, so wait for PV capacity to reach the request before continuing",
					pvc.Namespace,
					pvc.Name,
					requested.String(),
					pv.Name,
					provisioned.String(),
				)
			}
		}
	}

	if required == nil {
		return nil
	}

	if required.Sign() <= 0 {
		return errors.New("required PV storage capacity must be positive")
	}

	if !validProvisioned || provisioned.Cmp(*required) < 0 {
		actual := "missing"
		if hasProvisioned {
			actual = provisioned.String()
		}

		return fmt.Errorf(
			"bound PV %s currently provides %s, below required capacity %s",
			pv.Name,
			actual,
			required.String(),
		)
	}

	return nil
}

// ValidateStorageClassPlacement rejects topology combinations that Kubernetes
// cannot schedule before a PVC or binding Pod is created. A named target is
// valid on a tainted node because pvc-migrate injects exact tolerations for
// pinned tool Pods. Scheduler-selected Pods have no such tolerations, so they
// require at least one compatible node without a hard scheduling taint.
func ValidateStorageClassPlacement(
	ctx context.Context,
	client kubernetes.Interface,
	storageClassName, targetNode string,
) error {
	if client == nil || storageClassName == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"storage topology",
			"Kubernetes client and StorageClass name are required",
		)
	}

	storageClass, err := client.StorageV1().StorageClasses().Get(
		ctx,
		storageClassName,
		metav1.GetOptions{},
	)
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"storage topology",
			"read destination StorageClass "+storageClassName,
			err,
		)
	}

	if targetNode != "" {
		node, getErr := client.CoreV1().Nodes().Get(ctx, targetNode, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"storage topology",
				"read target node "+targetNode,
				getErr,
			)
		}

		if !NodeReadyAndSchedulable(node) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"storage topology",
				fmt.Sprintf("target node %s must be Ready and not cordoned", targetNode),
			)
		}

		if !StorageClassAllowsNode(storageClass, node) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"storage topology",
				fmt.Sprintf(
					"target node %s does not satisfy StorageClass %s allowedTopologies",
					targetNode,
					storageClassName,
				),
			)
		}

		return nil
	}

	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"storage topology",
			"list nodes for StorageClass "+storageClassName,
			err,
		)
	}

	for index := range nodes.Items {
		node := &nodes.Items[index]
		if NodeReadyAndSchedulable(node) &&
			!nodeHasHardSchedulingTaint(node) &&
			StorageClassAllowsNode(storageClass, node) {
			return nil
		}
	}

	message := fmt.Sprintf(
		"StorageClass %s has no Ready, uncordoned node available to an unpinned binding Pod",
		storageClassName,
	)
	if len(storageClass.AllowedTopologies) > 0 {
		message = fmt.Sprintf(
			"StorageClass %s allowedTopologies match no Ready, uncordoned node available to an unpinned binding Pod",
			storageClassName,
		)
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"storage topology",
		message,
	)
}

func nodeHasHardSchedulingTaint(node *corev1.Node) bool {
	if node == nil {
		return false
	}

	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule ||
			taint.Effect == corev1.TaintEffectNoExecute {
			return true
		}
	}

	return false
}
