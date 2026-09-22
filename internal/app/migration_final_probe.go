package app

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (m *migrationResources) transferFinalSyncProbes(ctx context.Context,
	id, sourceNamespace, destinationNamespace, sourceNode, targetNode, image string,
	strategies []string, volumes []v1alpha1.VolumeSpec,
) ([]kube.ToolImageProbeResult, error) {
	if m.config.ToolImageProber == nil {
		return nil, nil
	}

	components := []string{kube.ToolComponentRsync}
	if slices.Contains(strategies, domain.StrategyLocal) {
		components = append(components, kube.ToolComponentSSHD)
	}

	targets := toolProbeTargetsForNamespaces(
		[]string{destinationNamespace},
		targetNode,
		components,
	)
	for _, volume := range volumes {
		source := kube.ToolProbeTarget{
			Namespace: sourceNamespace,
			NodeName:  sourceNode,
			PVCName:   volume.SourcePVC.Name,
			// The workload (or the shared warm-copy mount) may hold the
			// source filesystem read-write. ext4 refuses a fresh read-only
			// mount of an already-mounted device with EBUSY, so probe
			// read-write exactly like the real transfer tool does; the probe
			// itself only reads paths.
			WritablePVCMount: true,
		}
		if sessionNeedsSourceSSHD(strategies) {
			source.Components = []string{kube.ToolComponentSSHD}
		}

		if path := domain.SourceTransferPath(volume.TransferScope); path != domain.VolumeRootPath {
			source.RequiredPath = path
		}

		targets = append(targets, source)
		if path := domain.DestinationTransferPath(
			volume.TransferScope,
		); path != domain.VolumeRootPath {
			targets = append(targets, kube.ToolProbeTarget{
				Namespace:        destinationNamespace,
				NodeName:         targetNode,
				PVCName:          volume.DestinationPVC.Name,
				WritablePVCMount: true,
				RequiredPath:     path,
				CreatePath:       true,
				Components:       []string{kube.ToolComponentRsync},
			})
		}
	}

	return m.config.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
		OperationID: id,
		Image:       m.transfer.toolImage(image),
		Targets:     targets,
		Timeout:     m.config.ProbeTimeout,
		Writer:      m.config.Transfer.Writer,
		Logger:      m.config.Transfer.Logger,
	})
}
