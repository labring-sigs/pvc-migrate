package app

import (
	"context"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func reservationToolProbeTargets(session *domain.Session) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	options := session.Spec.WorkflowOptions()
	if options.TargetNode == "" {
		return nil
	}

	return toolProbeTargetsForNamespaces(
		destinationVolumeNamespaces(session),
		options.TargetNode,
		nil,
	)
}

func copyToolProbeTargets(session *domain.Session, mountSourcePVC bool) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	options := session.Spec.WorkflowOptions()
	switch session.Spec.Operation() {
	case domain.OperationCopy, domain.OperationMigrate, domain.OperationMigratePod:
		targets := destinationTransferPathProbeTargets(session, options.TargetNode)
		if options.TargetNode == "" {
			return targets
		}

		targetNamespaces := destinationVolumeNamespaces(session)
		targets = append(
			targets,
			toolProbeTargetsForNamespaces(
				targetNamespaces,
				options.TargetNode,
				[]string{kube.ToolComponentRsync},
			)...,
		)
		needsSSHD := sessionNeedsSourceSSHD(session)

		if slices.Contains(options.Strategies, domain.StrategyLocal) {
			targets = append(
				targets,
				toolProbeTargetsForNamespaces(
					targetNamespaces,
					options.TargetNode,
					[]string{kube.ToolComponentSSHD},
				)...,
			)
		}

		if (!needsSSHD && !mountSourcePVC) || options.SourceNode == "" {
			return targets
		}

		targets = append(
			targets,
			sourceToolProbeTargets(session, options.SourceNode, mountSourcePVC)...,
		)

		return targets
	}

	return nil
}

func sessionNeedsSourceSSHD(session *domain.Session) bool {
	if session == nil {
		return false
	}

	operation := session.Spec.Operation()
	if operation != domain.OperationCopy && operation != domain.OperationMigrate &&
		operation != domain.OperationMigratePod {
		return false
	}

	strategies := session.Spec.WorkflowOptions().Strategies
	if len(strategies) == 0 {
		return true
	}

	for _, strategy := range strategies {
		if strategy != domain.StrategyMount {
			return true
		}
	}

	return false
}

func destinationVolumeNamespaces(session *domain.Session) []string {
	return volumeNamespaces(
		session,
		func(volume domain.VolumeSpec) string { return volume.DestinationPVC.Namespace },
		session.Spec.TemporaryNamespace,
	)
}

func sourceVolumeNamespaces(session *domain.Session) []string {
	return volumeNamespaces(
		session,
		func(volume domain.VolumeSpec) string { return volume.SourcePVC.Namespace },
		session.Spec.SourceNamespace,
	)
}

func volumeNamespaces(
	session *domain.Session,
	namespaceFor func(domain.VolumeSpec) string,
	fallback string,
) []string {
	namespaces := make([]string, 0, len(session.Spec.Volumes))
	for _, volume := range session.Spec.Volumes {
		namespace := namespaceFor(volume)
		if namespace != "" && !slices.Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}

	if len(namespaces) == 0 && fallback != "" {
		namespaces = append(namespaces, fallback)
	}

	return namespaces
}

func toolProbeTargetsForNamespaces(
	namespaces []string,
	nodeName string,
	components []string,
) []kube.ToolProbeTarget {
	targets := make([]kube.ToolProbeTarget, 0, len(namespaces))
	for _, namespace := range namespaces {
		targets = append(
			targets,
			kube.ToolProbeTarget{
				Namespace:  namespace,
				NodeName:   nodeName,
				Components: slices.Clone(components),
			},
		)
	}

	return targets
}

func sourceToolProbeTargets(
	session *domain.Session,
	nodeName string,
	mountSourcePVC bool,
) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	targets := make([]kube.ToolProbeTarget, 0, len(session.Spec.Volumes))

	var sourceComponents []string
	if sessionNeedsSourceSSHD(session) {
		sourceComponents = []string{kube.ToolComponentSSHD}
	}

	for _, volume := range session.Spec.Volumes {
		target := kube.ToolProbeTarget{
			Namespace:    volume.SourcePVC.Namespace,
			NodeName:     nodeName,
			PVCName:      volume.SourcePVC.Name,
			SkipPVCMount: !mountSourcePVC,
			Components:   slices.Clone(sourceComponents),
		}
		if mountSourcePVC &&
			domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
			target.RequiredPath = domain.SourceTransferPath(volume.TransferScope)
		}

		targets = append(targets, target)
	}

	return targets
}

func destinationTransferPathProbeTargets(
	session *domain.Session,
	nodeName string,
) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	targets := make([]kube.ToolProbeTarget, 0, len(session.Spec.Volumes))
	for _, volume := range session.Spec.Volumes {
		path := domain.DestinationTransferPath(volume.TransferScope)
		if path == domain.VolumeRootPath {
			continue
		}

		targets = append(targets, kube.ToolProbeTarget{
			Namespace:        volume.DestinationPVC.Namespace,
			NodeName:         nodeName,
			PVCName:          volume.DestinationPVC.Name,
			WritablePVCMount: true,
			RequiredPath:     path,
			CreatePath:       true,
			Components:       []string{kube.ToolComponentRsync},
		})
	}

	return targets
}

func (s *Service) requireSessionNamespaces(
	ctx context.Context,
	plan *domain.MigrationPlan,
) error {
	s.logInfo("checking migration namespaces", "session", plan.SessionID)

	if err := kube.RequireNamespace(
		ctx,
		s.client,
		plan.SessionSpec.SessionNamespace,
	); err != nil {
		return err
	}

	if plan.SessionSpec.TemporaryNamespace != plan.SessionSpec.SessionNamespace {
		if err := kube.RequireNamespace(
			ctx,
			s.client,
			plan.SessionSpec.TemporaryNamespace,
		); err != nil {
			return err
		}
	}

	ensured := map[string]struct{}{
		plan.SessionSpec.SessionNamespace:   {},
		plan.SessionSpec.TemporaryNamespace: {},
	}
	for _, volume := range plan.SessionSpec.Volumes {
		if _, ok := ensured[volume.DestinationPVC.Namespace]; ok {
			continue
		}

		if err := kube.RequireNamespace(
			ctx,
			s.client,
			volume.DestinationPVC.Namespace,
		); err != nil {
			return err
		}

		ensured[volume.DestinationPVC.Namespace] = struct{}{}
	}

	return nil
}
