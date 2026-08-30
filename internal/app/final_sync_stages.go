package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) finalSyncWithProbeResults(
	ctx context.Context,
	session *domain.Session,
	probeResults []kube.ToolImageProbeResult,
) error {
	pathTargets, err := s.sourceTransferPathProbeTargets(ctx, session)
	if err != nil {
		return err
	}

	pathProbeResults, err := s.probeToolImage(ctx, session, pathTargets)
	if err != nil {
		return err
	}

	probeResults = append(probeResults, pathProbeResults...)

	if session.Status.Phase == domain.PhaseFinalSynced {
		for i := range session.Status.Volumes {
			session.Status.Volumes[i].Sync.FinalCompletedAt = nil
			session.Status.Volumes[i].Sync.ChecksumVerified = false
		}
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseFinalSyncing,
		"running offline final sync",
	); err != nil {
		return err
	}

	if err := s.validateOfflineVolumes(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Sync.FinalCompletedAt != nil {
			continue
		}

		if err := s.switcher.VerifyVolumeOffline(ctx, volume); err != nil {
			return s.failContext(ctx, session, err)
		}

		if err := s.copyWithRetry(
			ctx,
			session,
			volume,
			status,
			copyengine.ModeFinal,
			probeResults,
		); err != nil {
			return s.failContext(ctx, session, err)
		}

		now := metav1.NewTime(s.now().UTC())
		status.Sync.FinalCompletedAt = &now
		status.Sync.ChecksumVerified = session.Spec.WorkflowOptions().VerifyChecksum
		status.Sync.LastError = ""

		if err := s.store.Update(ctx, session); err != nil {
			return err
		}
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseFinalSynced,
		"offline final sync completed for all volumes",
	)
}

func (s *Service) sourceTransferPathProbeTargets(
	ctx context.Context,
	session *domain.Session,
) ([]kube.ToolProbeTarget, error) {
	if session == nil {
		return nil, nil
	}

	hasPartialSource := false
	for _, volume := range session.Spec.Volumes {
		if domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
			hasPartialSource = true
			break
		}
	}

	if !hasPartialSource {
		return nil, nil
	}

	var (
		targets []kube.ToolProbeTarget
		err     error
	)
	if nodeName := session.Spec.WorkflowOptions().SourceNode; nodeName != "" {
		targets = sourceToolProbeTargets(session, nodeName, true)
	} else {
		targets, err = s.resolveSourceToolProbeTargets(ctx, session, true)
		if err != nil {
			return nil, err
		}
	}

	filtered := targets[:0]
	for _, target := range targets {
		if target.RequiredPath == "" {
			continue
		}

		target.Components = nil
		filtered = append(filtered, target)
	}

	return filtered, nil
}
