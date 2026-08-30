package app

import (
	"context"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type sourcePodListResult struct {
	pods []corev1.Pod
	err  error
}

func (s *Service) sourcePVCIsActive(ctx context.Context, volume *domain.VolumeSpec) (bool, error) {
	if s == nil || s.client == nil || volume == nil {
		return false, nil
	}

	pods, err := s.client.CoreV1().Pods(volume.SourcePVC.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"list Pods in namespace "+volume.SourcePVC.Namespace,
			err,
		)
	}

	for index := range pods.Items {
		if kube.ActivePodUsesPVC(&pods.Items[index], volume.SourcePVC.Name) {
			return true, nil
		}
	}

	return false, nil
}

func (s *Service) resolveSourceToolProbeTargets(
	ctx context.Context,
	session *domain.Session,
	mountSourcePVC bool,
) ([]kube.ToolProbeTarget, error) {
	if session == nil {
		return nil, domain.NewError(domain.ErrorValidation, "tool image probe", "session is nil")
	}

	options := session.Spec.WorkflowOptions()
	namespaces := sourceVolumeNamespaces(session)

	podLists := make([]sourcePodListResult, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		pods, err := s.client.CoreV1().Pods(namespaces[index]).List(ctx, metav1.ListOptions{})
		if err == nil && pods == nil {
			err = fmt.Errorf("list PVC consumers in %s returned an empty object", namespaces[index])
		}

		if pods != nil {
			podLists[index].pods = pods.Items
		}

		podLists[index].err = err
	})

	resolvedNodes, needsTopology, activeCopyNodes, err := resolveConsumerNodes(
		session,
		namespaces,
		podLists,
		options.SourceNode,
	)
	if err != nil {
		return nil, err
	}

	if len(activeCopyNodes) > 1 {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			"online copy consumers span multiple source nodes",
		)
	}

	if options.SourceNode != "" {
		return nil, nil
	}

	needsAnyTopology := slices.Contains(needsTopology, true)

	var nodes []corev1.Node
	if needsAnyTopology {
		nodeList, err := s.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				"tool image probe",
				"list nodes for source PV topology",
				err,
			)
		}

		if nodeList == nil {
			return nil, domain.NewError(
				domain.ErrorKubernetes,
				"tool image probe",
				"list nodes for source PV topology returned an empty object",
			)
		}

		nodes = nodeList.Items

		type pvResult struct {
			pv  *corev1.PersistentVolume
			err error
		}

		pvs := make([]pvResult, len(session.Spec.Volumes))
		parallel.For(len(session.Spec.Volumes), func(index int) {
			if !needsTopology[index] || session.Spec.Volumes[index].SourcePV.Name == "" {
				return
			}

			pvs[index].pv, pvs[index].err = s.client.CoreV1().
				PersistentVolumes().
				Get(ctx, session.Spec.Volumes[index].SourcePV.Name, metav1.GetOptions{})
		})

		for index := range pvs {
			if pvs[index].err != nil {
				return nil, domain.WrapError(
					domain.ErrorKubernetes,
					"tool image probe",
					"read source PV "+session.Spec.Volumes[index].SourcePV.Name,
					pvs[index].err,
				)
			}

			if pvs[index].pv != nil {
				resolvedNodes[index] = kube.PVUniqueNodeName(pvs[index].pv, nodes)
			}
		}
	}

	targets := make([]kube.ToolProbeTarget, 0, len(session.Spec.Volumes))

	var sourceComponents []string
	if sessionNeedsSourceSSHD(session) {
		sourceComponents = []string{kube.ToolComponentSSHD}
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		target := kube.ToolProbeTarget{
			Namespace:    volume.SourcePVC.Namespace,
			NodeName:     resolvedNodes[index],
			PVCName:      volume.SourcePVC.Name,
			SkipPVCMount: resolvedNodes[index] != "" && !mountSourcePVC,
			Components:   slices.Clone(sourceComponents),
		}
		if mountSourcePVC &&
			domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
			target.RequiredPath = domain.SourceTransferPath(volume.TransferScope)
		}

		targets = append(targets, target)
	}

	return targets, nil
}

func resolveConsumerNodes(
	session *domain.Session,
	namespaces []string,
	podLists []sourcePodListResult,
	sourceNode string,
) ([]string, []bool, map[string]struct{}, error) {
	resolved := make([]string, len(session.Spec.Volumes))
	needsTopology := make([]bool, len(session.Spec.Volumes))

	activeCopyNodes := map[string]struct{}{}
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		namespaceIndex := slices.Index(namespaces, volume.SourcePVC.Namespace)
		if namespaceIndex < 0 {
			return nil, nil, nil, domain.NewError(
				domain.ErrorInternal,
				"tool image probe",
				fmt.Sprintf("source namespace %s was not inventoried", volume.SourcePVC.Namespace),
			)
		}

		result := podLists[namespaceIndex]
		if result.err != nil {
			return nil, nil, nil, domain.WrapError(
				domain.ErrorKubernetes,
				"tool image probe",
				"list PVC consumers in "+volume.SourcePVC.Namespace,
				result.err,
			)
		}

		node, err := resolveVolumeConsumerNode(session, volume, result.pods)
		if err != nil {
			return nil, nil, nil, err
		}

		resolved[index] = node

		needsTopology[index] = node == ""
		if node != "" && session.Spec.Operation() == domain.OperationCopy {
			activeCopyNodes[node] = struct{}{}
		}

		if sourceNode != "" && node != "" && node != sourceNode {
			return nil, nil, nil, domain.NewError(
				domain.ErrorConflict,
				"tool image probe",
				fmt.Sprintf(
					"PVC %s/%s consumer runs on %s, session source node is %s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					node,
					sourceNode,
				),
			)
		}
	}

	return resolved, needsTopology, activeCopyNodes, nil
}

func resolveVolumeConsumerNode(
	session *domain.Session,
	volume *domain.VolumeSpec,
	pods []corev1.Pod,
) (string, error) {
	activeCount, scheduledCount := 0, 0

	nodes := map[string]struct{}{}
	for index := range pods {
		pod := &pods[index]
		if !kube.ActivePodUsesPVC(pod, volume.SourcePVC.Name) {
			continue
		}

		activeCount++

		if pod.Spec.NodeName != "" {
			scheduledCount++
			nodes[pod.Spec.NodeName] = struct{}{}
		}
	}

	if err := validateCopyConsumers(session, volume, activeCount, scheduledCount); err != nil {
		return "", err
	}

	if len(nodes) > 1 {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"PVC %s/%s active consumers span multiple nodes",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	for node := range nodes {
		return node, nil
	}

	return "", nil
}

func validateCopyConsumers(
	session *domain.Session,
	volume *domain.VolumeSpec,
	active, scheduled int,
) error {
	if session.Spec.Operation() != domain.OperationCopy || active == 0 {
		return nil
	}

	if !session.Spec.Online() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"offline copy requires PVC %s/%s to have zero active Pod consumers",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOncePod) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"active RWOP PVC %s/%s cannot be warm-copied",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOnce) && scheduled != active {
		return domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"every active consumer of RWO PVC %s/%s must be scheduled before online copy",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	return nil
}
