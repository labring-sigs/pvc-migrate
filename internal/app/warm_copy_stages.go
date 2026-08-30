package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) WarmCopy(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.warmCopy(lockedCtx, session) },
	)
}

func (s *Service) warmCopy(ctx context.Context, session *domain.Session) (resultErr error) {
	valid := session.Status.Phase == domain.PhaseReserved ||
		session.Status.Phase == domain.PhaseWarmCopied ||
		session.Status.Phase == domain.PhaseWarmCopying ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseWarmCopying)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"warm copy",
			fmt.Sprintf("session phase %s cannot warm-copy", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	s.logInfo(
		"warm copy preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
	)

	if err := s.ValidateWarmCopy(ctx, session); err != nil {
		return err
	}
	// Checkpoint inferred online-copy placement only after the full read-only
	// preflight has passed for every volume.
	if err := s.validateCopyConsumersBatch(ctx, session, true); err != nil {
		return err
	}

	if err := s.enableOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	restoreSharedMounts := true
	defer func() {
		if !restoreSharedMounts {
			return
		}

		if err := s.restoreOpenEBSLVMSharedMountsAfterFailure(ctx, session); err != nil {
			resultErr = errors.Join(resultErr, s.failContext(ctx, session, err))
		}
	}()

	targets, err := s.resolveCopyToolProbeTargets(ctx, session, true)
	if err != nil {
		return err
	}

	probeResults, err := s.probeToolImage(ctx, session, targets)
	if err != nil {
		return warmCopyProbeError(session.Spec.Operation(), targets, err)
	}

	if session.Status.Phase == domain.PhaseWarmCopied {
		for i := range session.Status.Volumes {
			session.Status.Volumes[i].Sync.WarmCompletedAt = nil
			session.Status.Volumes[i].Sync.LastError = ""
		}
	}

	if err := s.begin(ctx, session, domain.PhaseWarmCopying, "running warm copy"); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Sync.WarmCompletedAt != nil {
			continue
		}

		if err := s.validateCopyConsumers(ctx, session, volume); err != nil {
			return s.failContext(ctx, session, err)
		}

		if err := s.copyWithRetry(
			ctx,
			session,
			volume,
			status,
			copyengine.ModeWarm,
			probeResults,
		); err != nil {
			return s.failContext(ctx, session, err)
		}

		now := metav1.NewTime(s.now().UTC())
		status.Sync.WarmCompletedAt = &now
		status.Sync.LastError = ""

		if err := s.store.Update(ctx, session); err != nil {
			return err
		}
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	restoreSharedMounts = false

	if session.Spec.Operation() == domain.OperationMigratePod {
		session.CompleteWarmPass()
	}

	return s.finish(ctx, session, domain.PhaseWarmCopied, "warm copy completed for all volumes")
}

func warmCopyProbeError(
	operation domain.Operation,
	targets []kube.ToolProbeTarget,
	err error,
) error {
	if err == nil || !kube.IsConcurrentMountFailureMessage(err.Error()) {
		return err
	}

	pvcs := make([]string, 0, len(targets))
	for _, target := range targets {
		if target.PVCName == "" || target.SkipPVCMount {
			continue
		}

		ref := target.Namespace + "/" + target.PVCName
		if !slices.Contains(pvcs, ref) {
			pvcs = append(pvcs, ref)
		}
	}

	if len(pvcs) == 0 {
		return err
	}

	sort.Strings(pvcs)

	recovery := "disable warm copy after making sure the source PVC has no active Pod consumers"
	switch operation {
	case domain.OperationCopy:
		recovery = "rerun the copy without --online after the source PVC has no active Pod consumers"
	case domain.OperationMigratePod:
		recovery = "rerun the migration with --precopy-passes 0"
	}

	return domain.WrapError(
		domain.ErrorPrecondition,
		"warm-copy mount probe",
		fmt.Sprintf(
			"second-Pod mount failed for source PVC(s) %s while the source workload is active: %v; abort this pre-cutover session, clean its retained resources, and %s",
			strings.Join(pvcs, ","),
			err,
			recovery,
		),
		err,
	)
}

func (s *Service) resumeCopy(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving:
		if err := s.reserve(ctx, session); err != nil {
			return err
		}
		return s.warmCopy(ctx, session)
	case domain.PhaseReserved, domain.PhaseWarmCopying:
		return s.warmCopy(ctx, session)
	case domain.PhaseRollingBack:
		return s.rollback(ctx, session)
	case domain.PhaseAborting:
		return s.abort(ctx, session)
	case domain.PhaseWarmCopied, domain.PhaseCompleted,
		domain.PhaseAborted, domain.PhaseRolledBack:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	default:
		return invalidWorkflowResumePhase(phase, domain.OperationCopy)
	}
}
