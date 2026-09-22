package app

import (
	"context"
	"fmt"
	"slices"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (m *podMigrationResources) warmSourceTargets(
	ctx context.Context, namespace, sourceNode string, volumes []v1alpha1.VolumeSpec,
) ([]kube.ToolProbeTarget, error) {
	pods, err := m.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"pod migration source preflight",
			"list source Pods",
			err,
		)
	}

	targets := make([]kube.ToolProbeTarget, 0, len(volumes))

	var nodes *corev1.NodeList
	for _, volume := range volumes {
		source := qualifiedResourceReference(volume.SourcePVC, namespace)

		node, err := resolveVolumeConsumerNode(source, pods.Items)
		if err != nil {
			return nil, err
		}

		if sourceNode != "" && node != "" && node != sourceNode {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"pod migration source preflight",
				fmt.Sprintf(
					"PVC %s/%s consumer runs on %s, planned source node is %s",
					namespace,
					source.Name,
					node,
					sourceNode,
				),
			)
		}

		if sourceNode != "" {
			node = sourceNode
		}

		if node == "" {
			if nodes == nil {
				nodes, err = m.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				if err != nil {
					return nil, domain.WrapError(
						domain.ErrorKubernetes,
						"pod migration source preflight",
						"list nodes for source PV topology",
						err,
					)
				}
			}

			pv, err := m.client.CoreV1().
				PersistentVolumes().
				Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
			if err != nil {
				return nil, domain.WrapError(
					domain.ErrorKubernetes,
					"pod migration source preflight",
					"read source PV "+volume.SourcePV.Name,
					err,
				)
			}

			if pv.UID != volume.SourcePV.UID {
				return nil, domain.NewError(
					domain.ErrorConflict,
					"pod migration source preflight",
					"source PV UID changed",
				)
			}

			node = kube.PVUniqueNodeName(pv, nodes.Items)
		}

		target := kube.ToolProbeTarget{Namespace: namespace, NodeName: node, PVCName: source.Name}
		if path := domain.SourceTransferPath(volume.TransferScope); path != domain.VolumeRootPath {
			target.RequiredPath = path
		}

		targets = append(targets, target)
	}

	return targets, nil
}

func (m *podMigrationResources) validateWarmSourceMounts(
	ctx context.Context, namespace string, volumes []v1alpha1.VolumeSpec,
	allowShared bool, mounts []v1alpha1.SharedMountStatus,
) error {
	if m.sharedVolumes == nil {
		if allowShared {
			return domain.NewError(
				domain.ErrorInternal,
				"pod migration shared mount",
				"OpenEBS LVMVolume manager is required when shared mounts are enabled",
			)
		}

		return nil
	}

	pods, err := m.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pod migration shared mount",
			"list source Pods",
			err,
		)
	}

	for _, volume := range volumes {
		source := qualifiedResourceReference(volume.SourcePVC, namespace)
		pv := qualifiedResourceReference(volume.SourcePV, "")

		lvm, err := openEBSLVMSource(ctx, m.client, source, pv)
		if err != nil {
			return err
		}

		if !lvm || !podInventoryUsesPVC(pods.Items, source.Name) {
			continue
		}

		var needsChange bool
		if recorded, exists := podSharedMountForPV(mounts, pv); exists {
			previous := strings.TrimSpace(recorded.PreviousShared)
			if recorded.PreviousSharedSet && previous != "" && !strings.EqualFold(previous, "no") &&
				!strings.EqualFold(previous, "yes") {
				return domain.NewError(
					domain.ErrorPrecondition,
					"pod migration shared mount",
					"recorded LVMVolume spec.shared value is unsupported",
				)
			}

			needsChange = !recorded.PreviousSharedSet || previous == "" ||
				strings.EqualFold(previous, "no")
		} else {
			prepared, err := m.sharedVolumes.PrepareShared(ctx, pv)
			if err != nil {
				return err
			}

			needsChange = prepared.NeedsChange
		}

		if needsChange && !allowShared {
			return domain.NewError(
				domain.ErrorPrecondition,
				"warm-copy mount preflight",
				fmt.Sprintf(
					"source PVC %s/%s is active and its OpenEBS LVMVolume does not have spec.shared=yes; recreate the migration with precopyPasses=0 or openebsLvmEnableShared enabled",
					namespace,
					source.Name,
				),
			)
		}
	}

	return nil
}

func podInventoryUsesPVC(pods []corev1.Pod, name string) bool {
	for i := range pods {
		if kube.ActivePodUsesPVC(&pods[i], name) {
			return true
		}
	}

	return false
}

func podSharedMountForPV(
	mounts []v1alpha1.SharedMountStatus,
	pv v1alpha1.ObjectReference,
) (v1alpha1.SharedMountStatus, bool) {
	for _, mount := range mounts {
		if mount.SourcePV.Name == pv.Name && mount.SourcePV.UID == pv.UID {
			return mount, true
		}
	}

	return v1alpha1.SharedMountStatus{}, false
}

func (m *podMigrationResources) prepareWarmSharedMounts(
	ctx context.Context, owner, namespace string, volumes []v1alpha1.VolumeSpec,
	mounts *[]v1alpha1.SharedMountStatus, save func(context.Context) error,
) error {
	if m.sharedVolumes == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pod migration shared mount",
			"OpenEBS LVMVolume manager is required when shared mounts are enabled",
		)
	}

	pods, err := m.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pod migration shared mount",
			"list source Pods",
			err,
		)
	}

	prepared := make([]v1alpha1.SharedMountStatus, 0, len(volumes))
	for _, volume := range volumes {
		source := qualifiedResourceReference(volume.SourcePVC, namespace)
		pv := qualifiedResourceReference(volume.SourcePV, "")

		lvm, err := openEBSLVMSource(ctx, m.client, source, pv)
		if err != nil {
			return err
		}

		if !lvm || !podInventoryUsesPVC(pods.Items, source.Name) {
			continue
		}

		result, err := m.sharedVolumes.PrepareShared(ctx, pv)
		if err != nil {
			return err
		}

		if result.NeedsChange {
			prepared = append(prepared, v1alpha1.SharedMountStatus{
				SourcePV: volume.SourcePV, LVMVolume: result.LVMVolume,
				PreviousShared: result.PreviousShared, PreviousSharedSet: result.PreviousSharedSet,
			})
		}
	}

	if err := validatePodSharedMountCheckpoints(volumes, prepared); err != nil {
		return err
	}

	for _, mount := range prepared {
		previous := *mounts

		*mounts = append(slices.Clone(previous), mount)
		if err := persistCheckpoint(ctx, save); err != nil {
			*mounts = previous
			return err
		}

		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		if err := m.sharedVolumes.EnableShared(ctx, owner, mount); err != nil {
			return err
		}
	}

	return nil
}

func (m *podMigrationResources) warmSourceWritable(
	ctx context.Context, owner string, source, pv v1alpha1.ObjectReference,
	mounts []v1alpha1.SharedMountStatus,
) (bool, error) {
	if m.sharedVolumes == nil {
		return false, nil
	}

	lvm, err := openEBSLVMSource(ctx, m.client, source, pv)
	if err != nil || !lvm {
		return false, err
	}

	expected, heldBy := v1alpha1.ObjectReference{}, ""
	if mount, found := podSharedMountForPV(mounts, pv); found {
		expected, heldBy = mount.LVMVolume, owner
	}

	return m.sharedVolumes.Shared(ctx, pv, expected, heldBy)
}
