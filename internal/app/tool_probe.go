package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func (s *Service) probeToolImage(
	ctx context.Context,
	session *domain.Session,
	targets []kube.ToolProbeTarget,
) ([]kube.ToolImageProbeResult, error) {
	if session == nil || s.config.ToolImageProber == nil {
		return nil, nil
	}

	if len(targets) == 0 {
		return nil, nil
	}

	s.logInfo(
		"tool image validation started",
		"session",
		session.ID,
		"image",
		s.toolImage(session),
		"targets",
		len(targets),
	)

	return s.config.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
		OperationID: session.ID,
		Image:       s.toolImage(session),
		Targets:     targets,
		Timeout:     s.config.HelmTimeout,
		Writer:      s.config.Writer,
		Logger:      s.config.Logger,
	})
}

func (s *Service) resolveCopyToolProbeTargets(
	ctx context.Context,
	session *domain.Session,
	mountSourcePVC bool,
) ([]kube.ToolProbeTarget, error) {
	targets := copyToolProbeTargets(session, mountSourcePVC)
	if !sessionNeedsSourceSSHD(session) && !mountSourcePVC {
		return targets, nil
	}

	options := session.Spec.WorkflowOptions()

	sourceTargets, err := s.resolveSourceToolProbeTargets(ctx, session, mountSourcePVC)
	if err != nil {
		return nil, err
	}

	if options.SourceNode == "" {
		targets = append(targets, sourceTargets...)
	}

	if mountSourcePVC {
		if err := s.markSharedOpenEBSLVMProbeMounts(ctx, session, targets); err != nil {
			return nil, err
		}
	}

	return targets, nil
}
