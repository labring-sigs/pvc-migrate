package app

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func podWarmDestinationTargets(
	namespace, node string,
	strategies []string,
	volumes []v1alpha1.VolumeSpec,
) []kube.ToolProbeTarget {
	components := []string{kube.ToolComponentRsync}
	if slices.Contains(strategies, domain.StrategyLocal) {
		components = append(components, kube.ToolComponentSSHD)
	}

	targets := toolProbeTargetsForNamespaces([]string{namespace}, node, components)
	for _, volume := range volumes {
		if path := domain.DestinationTransferPath(
			volume.TransferScope,
		); path != domain.VolumeRootPath {
			targets = append(targets, kube.ToolProbeTarget{
				Namespace: namespace, NodeName: node, PVCName: volume.DestinationPVC.Name,
				WritablePVCMount: true, RequiredPath: path, CreatePath: true,
				Components: []string{kube.ToolComponentRsync},
			})
		}
	}

	return targets
}

func (m *podMigrationResources) probeWarmCopy(
	ctx context.Context, owner, image string, strategies []string,
	volumes []v1alpha1.VolumeSpec, sources, destinations []kube.ToolProbeTarget,
	mounts []v1alpha1.SharedMountStatus,
) ([]kube.ToolImageProbeResult, error) {
	if m.config.ToolImageProber == nil {
		return nil, nil
	}

	targets := slices.Clone(destinations)

	bySource := make(map[string]v1alpha1.VolumeSpec, len(volumes))
	for _, volume := range volumes {
		bySource[volume.SourcePVC.Name] = volume
	}

	for _, target := range sources {
		if sessionNeedsSourceSSHD(strategies) {
			target.Components = []string{kube.ToolComponentSSHD}
		}

		volume, exists := bySource[target.PVCName]
		if !exists {
			return nil, domain.NewError(
				domain.ErrorValidation,
				"pod migration probe",
				"source probe does not identify a planned volume",
			)
		}

		writable, err := m.warmSourceWritable(ctx, owner,
			qualifiedResourceReference(volume.SourcePVC, target.Namespace),
			qualifiedResourceReference(volume.SourcePV, ""), mounts)
		if err != nil {
			return nil, err
		}

		target.WritablePVCMount = writable
		targets = append(targets, target)
	}

	results, err := m.config.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
		OperationID: owner,
		Image:       m.transfer.toolImage(image),
		Targets:     targets,
		Timeout:     m.config.ProbeTimeout,
		Writer:      m.config.Transfer.Writer,
		Logger:      m.config.Transfer.Logger,
	})

	return results, warmCopyProbeError(domain.OperationMigratePod, targets, err)
}

func (m *migrationResources) cleanupCopyAttempt(
	ctx context.Context, request copyengine.CleanupRequest, destination v1alpha1.ObjectReference,
) error {
	request.KubeconfigPath, request.Context = m.config.Transfer.KubeconfigPath, m.config.Transfer.Context
	request.Strategies = slices.Clone(request.Strategies)

	if err := checkpointFenceError(ctx); err != nil {
		return err
	}

	if err := m.transfer.copier.Cleanup(ctx, request); err != nil {
		return err
	}

	return m.transfer.cleanupCopyToolPods(
		ctx,
		request.Source,
		destination,
		copyengine.OperationID(request.AttemptIdentity),
	)
}

func (m *podMigrationResources) restoreSharedMountsAfterFailure(
	ctx context.Context, owner string, namespaces []string,
	mounts *[]v1alpha1.SharedMountStatus, save func(context.Context) error,
) error {
	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	recovery, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		openEBSLVMSharedMountCleanupTimeout,
	)
	defer cancel()

	return m.restoreSharedMounts(recovery, owner, namespaces, mounts, save)
}

func podWarmCopyPhase(phase v1alpha1.WorkflowPhase) bool {
	return phase == domain.PhaseReserved || phase == domain.PhaseWarmCopying ||
		phase == domain.PhaseWarmCopied
}

func validatePodCopyRetry(reason string) error {
	if reason == domain.FailureDestinationCapacityExhausted {
		return domain.NewError(
			domain.ErrorConflict,
			"pod migration copy",
			"destination capacity was exhausted; abort and clean up this migration before creating one with larger storage",
		)
	}

	return nil
}
