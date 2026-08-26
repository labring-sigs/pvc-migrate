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

func (s *Service) Reserve(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.reserve(lockedCtx, session) },
	)
}

func (s *Service) reserve(ctx context.Context, session *domain.Session) error {
	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if session.Status.Phase == domain.PhaseReserved ||
		phaseAfter(session.Status.Phase, domain.PhaseReserved) {
		return nil
	}

	if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	if _, err := s.probeToolImage(ctx, session, reservationToolProbeTargets(session)); err != nil {
		return err
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseReserving,
		"reserving destination storage",
	); err != nil {
		return err
	}

	if err := kube.EnsureNamespace(
		ctx,
		s.client,
		session.Spec.TemporaryNamespace,
		session.ID,
		false,
	); err != nil {
		return s.failContext(ctx, session, err)
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Reserved && volume.DestinationPV.UID != "" {
			// A retry still revalidates backend settings applied after provisioning.
		} else {
			s.logInfo(
				"destination storage reservation started",
				"session",
				session.ID,
				"pvc",
				volume.SourcePVC.Name,
				"destination",
				volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
				"node",
				session.Spec.WorkflowOptions().TargetNode,
			)

			if err := s.reserver.ReserveVolume(ctx, session, volume, status, false); err != nil {
				return s.failContext(ctx, session, err)
			}

			if err := s.store.Update(ctx, session); err != nil {
				return s.failContext(ctx, session, err)
			}
		}

		if err := s.ensureConcurrentDestinationMount(ctx, session, index); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseReserved,
		"destination storage is provisioned and retained",
	)
}

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

	if session.Spec.Operation() == domain.OperationMigrate ||
		session.Spec.Operation() == domain.OperationMigratePod {
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
	case domain.OperationMigrate, domain.OperationMigratePod:
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

func (s *Service) Pause(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.pause(lockedCtx, session) },
	)
}

func (s *Service) pause(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause workload",
			"workload pause requires an orchestrated migration session",
		)
	}

	if session.Status.Phase == domain.PhasePaused ||
		session.Status.Phase == domain.PhaseFinalSyncing ||
		session.Status.Phase == domain.PhaseFinalSynced {
		if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
			return err
		}
		return s.controllers.VerifyPaused(ctx, session)
	}

	valid := session.Status.Phase == domain.PhaseReserved ||
		session.Status.Phase == domain.PhaseWarmCopied ||
		session.Status.Phase == domain.PhasePausing ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhasePausing)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause workload",
			fmt.Sprintf("session phase %s cannot pause", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	if err := s.begin(ctx, session, domain.PhasePausing, "pausing workload"); err != nil {
		return err
	}

	if err := s.controllers.Pause(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	// Controller adapters may record resource UIDs and original pause state
	// while applying the pause. Persist that recovery data before verification.
	if err := s.store.Update(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	return s.finish(ctx, session, domain.PhasePaused, "workload is safely paused")
}

func (s *Service) FinalSync(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.finalSync(lockedCtx, session) },
	)
}

func (s *Service) finalSync(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			"final sync requires an orchestrated migration session",
		)
	}

	valid := session.Status.Phase == domain.PhasePaused ||
		session.Status.Phase == domain.PhaseFinalSyncing ||
		session.Status.Phase == domain.PhaseFinalSynced ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseFinalSyncing)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"final sync preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
	)

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.validateOfflineVolumes(ctx, session); err != nil {
		return err
	}

	targets, err := s.resolveCopyToolProbeTargets(ctx, session, false)
	if err != nil {
		return err
	}

	probeResults, err := s.probeToolImage(ctx, session, targets)
	if err != nil {
		return err
	}

	return s.finalSyncWithProbeResults(ctx, session, probeResults)
}

// PauseAndFinalSync verifies the tool image while holding the same Session
// Lease used to pause the workload and launch the offline copy.
func (s *Service) PauseAndFinalSync(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.pauseAndFinalSync(lockedCtx, session)
	})
}

func (s *Service) pauseAndFinalSync(ctx context.Context, session *domain.Session) error {
	if session == nil || !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			"final sync requires an orchestrated migration session",
		)
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhaseReserved, domain.PhaseWarmCopied, domain.PhasePausing,
		domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"pause and final sync preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
		"alreadyPaused",
		phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
			phase == domain.PhaseFinalSynced,
	)

	alreadyPaused := phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced
	if alreadyPaused {
		if err := s.controllers.VerifyPaused(ctx, session); err != nil {
			return err
		}

		if err := s.verifyShrinkUsage(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}

		if err := s.validateOfflineVolumes(ctx, session); err != nil {
			return err
		}
	} else if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	targets, err := s.resolveCopyToolProbeTargets(ctx, session, false)
	if err != nil {
		return err
	}

	probeResults, err := s.probeToolImage(ctx, session, targets)
	if err != nil {
		return err
	}

	if !alreadyPaused {
		if err := s.pause(ctx, session); err != nil {
			return err
		}

		if err := s.verifyShrinkUsage(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finalSyncWithProbeResults(ctx, session, probeResults)
}

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

func (s *Service) Activate(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.activate(lockedCtx, session) },
	)
}

func (s *Service) activate(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate",
			"activation requires an orchestrated migration session",
		)
	}

	if session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseResuming ||
		session.Status.Phase == domain.PhaseCompleted ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming) {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	valid := session.Status.Phase == domain.PhaseFinalSynced ||
		session.Status.Phase == domain.PhaseActivating ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate",
			fmt.Sprintf("session phase %s cannot activate", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.ValidateActivation(ctx, session); err != nil {
		return err
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseActivating,
		"activating destination volumes",
	); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Activation.ActivatedAt != nil {
			if err := s.verifyActiveStorageVolume(ctx, session, index); err != nil {
				return s.failContext(ctx, session, err)
			}
			continue
		}

		s.logInfo(
			"volume activation started",
			"session",
			session.ID,
			"index",
			index,
			"source",
			volume.SourcePVC.Namespace+"/"+volume.SourcePVC.Name,
			"destination",
			volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
			"pv",
			volume.DestinationPV.Name,
		)

		if err := s.switcher.ActivateVolume(ctx, session, volume, status, func() error {
			return s.store.Update(ctx, session)
		}); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finish(ctx, session, domain.PhaseActivated, "all destination volumes are active")
}

func (s *Service) ResumeWorkload(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.resumeWorkload(lockedCtx, session) },
	)
}

func (s *Service) resumeWorkload(ctx context.Context, session *domain.Session) error {
	valid := session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseResuming ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume workload",
			fmt.Sprintf("session phase %s cannot resume", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.validateWorkloadResume(ctx, session); err != nil {
		return err
	}

	if err := s.begin(ctx, session, domain.PhaseResuming, "resuming workload"); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		if err := s.ensureConcurrentDestinationMount(
			ctx,
			session,
			index,
		); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	if err := s.controllers.Resume(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.verifyActiveVolumes(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseCompleted,
		"migration completed and workload is ready",
	)
}

func (s *Service) Migrate(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.migrate(lockedCtx, session)
	})
}

func (s *Service) migrateAfterWarmCopy(ctx context.Context, session *domain.Session) error {
	if err := s.PauseAndFinalSync(ctx, session); err != nil {
		return err
	}

	if err := s.Activate(ctx, session); err != nil {
		return err
	}

	return s.ResumeWorkload(ctx, session)
}

func (s *Service) migrate(ctx context.Context, session *domain.Session) error {
	warmPasses := session.Spec.PrecopyPasses()
	if warmPasses < 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"migrate",
			"warm passes must be non-negative",
		)
	}

	if session.Spec.WorkflowOptionsPtr() == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"migrate",
			"session workflow options are missing",
		)
	}

	if session.Status.WarmPassesCompleted < warmPasses {
		if err := s.ValidateWarmCopy(ctx, session); err != nil {
			return err
		}
	}

	if err := s.Reserve(ctx, session); err != nil {
		return err
	}

	if err := s.runRemainingWarmCopies(ctx, session, warmPasses); err != nil {
		return err
	}

	return s.migrateAfterWarmCopy(ctx, session)
}

func (s *Service) runRemainingWarmCopies(
	ctx context.Context,
	session *domain.Session,
	warmPasses int,
) error {
	for session.Status.WarmPassesCompleted < warmPasses {
		if err := s.WarmCopy(ctx, session); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) ResumeSession(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.resumeSession(lockedCtx, session) },
	)
}

func (s *Service) resumeSession(ctx context.Context, session *domain.Session) error {
	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	if session.Spec.Operation() == domain.OperationMigrate ||
		session.Spec.Operation() == domain.OperationMigratePod {
		return s.resumeComposite(ctx, session, phase)
	}

	return s.resumeSingle(ctx, session, phase)
}

func (s *Service) resumeSingle(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	if session.Spec.Operation() == domain.OperationBackup {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			"backup sessions require the backup resume workflow",
		)
	}

	if err := validateSingleResumePhase(session.Spec.Operation(), phase); err != nil {
		return err
	}

	if phase == domain.PhasePlanned {
		switch session.Spec.Operation() {
		case domain.OperationReserve:
			return s.Reserve(ctx, session)
		case domain.OperationCopy:
			if err := s.Reserve(ctx, session); err != nil {
				return err
			}
			return s.WarmCopy(ctx, session)
		case domain.OperationRename, domain.OperationMove:
			return s.Rename(ctx, session)
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume session",
				fmt.Sprintf(
					"planned phase cannot be resumed for operation %s",
					session.Spec.Operation(),
				),
			)
		}
	}

	switch phase {
	case domain.PhaseReserving:
		if session.Spec.Operation() == domain.OperationCopy {
			if err := s.Reserve(ctx, session); err != nil {
				return err
			}
			return s.WarmCopy(ctx, session)
		}

		return s.Reserve(ctx, session)
	case domain.PhaseReserved:
		if session.Spec.Operation() == domain.OperationCopy {
			return s.WarmCopy(ctx, session)
		}
		return nil
	case domain.PhaseWarmCopying:
		return s.WarmCopy(ctx, session)
	case domain.PhasePausing:
		return s.Pause(ctx, session)
	case domain.PhaseFinalSyncing:
		return s.FinalSync(ctx, session)
	case domain.PhaseActivating:
		return s.Activate(ctx, session)
	case domain.PhaseActivated, domain.PhaseResuming:
		return s.ResumeWorkload(ctx, session)
	case domain.PhaseRollingBack:
		return s.Rollback(ctx, session)
	case domain.PhaseAborting:
		return s.Abort(ctx, session)
	case domain.PhaseRenaming, domain.PhaseMoving:
		if (session.Spec.Operation() == domain.OperationRename && phase == domain.PhaseRenaming) ||
			(session.Spec.Operation() == domain.OperationMove && phase == domain.PhaseMoving) {
			return s.Rename(ctx, session)
		}

		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			fmt.Sprintf(
				"phase %s does not belong to operation %s",
				phase,
				session.Spec.Operation(),
			),
		)
	case domain.PhaseWarmCopied, domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			fmt.Sprintf(
				"phase %s cannot be resumed for operation %s",
				phase,
				session.Spec.Operation(),
			),
		)
	}
}

func (s *Service) resumeComposite(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving:
		if err := s.Reserve(ctx, session); err != nil {
			return err
		}
		return s.Migrate(ctx, session)
	case domain.PhaseReserved:
		return s.Migrate(ctx, session)
	case domain.PhaseWarmCopying:
		if err := s.WarmCopy(ctx, session); err != nil {
			return err
		}

		if err := s.runRemainingWarmCopies(ctx, session, session.Spec.PrecopyPasses()); err != nil {
			return err
		}

		return s.migrateAfterWarmCopy(ctx, session)
	case domain.PhaseWarmCopied, domain.PhasePausing:
		if phase == domain.PhaseWarmCopied {
			if err := s.runRemainingWarmCopies(
				ctx,
				session,
				session.Spec.PrecopyPasses(),
			); err != nil {
				return err
			}
		}

		return s.migrateAfterWarmCopy(ctx, session)
	case domain.PhasePaused, domain.PhaseFinalSyncing:
		if err := s.FinalSync(ctx, session); err != nil {
			return err
		}

		if err := s.Activate(ctx, session); err != nil {
			return err
		}

		return s.ResumeWorkload(ctx, session)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		if err := s.Activate(ctx, session); err != nil {
			return err
		}
		return s.ResumeWorkload(ctx, session)
	case domain.PhaseActivated, domain.PhaseResuming:
		return s.ResumeWorkload(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	case domain.PhaseRollingBack:
		return s.Rollback(ctx, session)
	case domain.PhaseAborting:
		return s.Abort(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			fmt.Sprintf("phase %s cannot be resumed", phase),
		)
	}
}

func (s *Service) Abort(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.abort(lockedCtx, session) },
	)
}

func (s *Service) abort(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseAborted {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	if session.Status.Phase == domain.PhaseRollingBack ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseRollingBack) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort session",
			"rollback recovery must continue through session resume or rollback",
		)
	}

	if session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseCompleted ||
		session.Status.ResumeFrom == domain.PhaseActivating ||
		session.Status.ResumeFrom == domain.PhaseResuming {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort session",
			"activated sessions require rollback",
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.ValidateAbort(ctx, session); err != nil {
		return err
	}

	resumeWorkload := abortRequiresWorkloadResume(session)
	if err := s.begin(ctx, session, domain.PhaseAborting, "aborting migration"); err != nil {
		return err
	}

	if resumeWorkload {
		if err := s.controllers.Resume(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	message := "migration aborted; reserved volumes are retained for cleanup"
	if session.Spec.Type == domain.SessionTypeBackup {
		message = "backup aborted; no recovery point was published"
	}

	return s.finish(ctx, session, domain.PhaseAborted, message)
}

func (s *Service) Rollback(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.rollback(lockedCtx, session) },
	)
}

func (s *Service) rollback(ctx context.Context, session *domain.Session) error {
	if session != nil && session.Spec.Type == domain.SessionTypeBackup {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback",
			"backup sessions do not change PVC identity and cannot be rolled back",
		)
	}

	return s.rollbackMigration(ctx, session)
}

func (s *Service) rollbackMigration(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseRolledBack {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	rollbackOrigin := session.Status.ResumeFrom
	if rollbackOrigin == domain.PhaseRollingBack {
		rollbackOrigin = phaseBefore(session, domain.PhaseRollingBack)
	}

	wasRunning := session.Status.Phase == domain.PhaseCompleted ||
		((session.Status.Phase == domain.PhaseFailed || session.Status.Phase == domain.PhaseRollingBack) && (rollbackOrigin == domain.PhaseResuming || rollbackOrigin == domain.PhaseCompleted))
	failedDuringCutover := session.Status.Phase == domain.PhaseFailed &&
		(session.Status.ResumeFrom == domain.PhaseActivating || session.Status.ResumeFrom == domain.PhaseResuming || session.Status.ResumeFrom == domain.PhaseRollingBack)

	valid := wasRunning || session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseActivating ||
		session.Status.Phase == domain.PhaseFinalSynced ||
		session.Status.Phase == domain.PhaseRollingBack ||
		failedDuringCutover
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback",
			fmt.Sprintf("session phase %s cannot roll back", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.prepareRollback(ctx, session, wasRunning); err != nil {
		return err
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseRollingBack,
		"rolling back to source volumes",
	); err != nil {
		return err
	}

	if session.Spec.Operation().RebindsPVC() {
		if len(session.Spec.Volumes) != 1 {
			return s.failContext(
				ctx,
				session,
				domain.NewError(
					domain.ErrorInternal,
					"rollback rename",
					"rename session must contain one volume",
				),
			)
		}

		volume := &session.Spec.Volumes[0]
		status := &session.Status.Volumes[0]
		reverse := *volume
		reverse.SourcePVC = volume.DestinationPVC
		reverse.SourcePVC.UID = status.Activation.ActivePVC.UID
		reverse.SourcePVC.ResourceVersion = status.Activation.ActivePVC.ResourceVersion
		reverse.DestinationPVC = volume.SourcePVC
		reverse.DestinationPVC.UID = ""
		reverse.DestinationPVC.ResourceVersion = ""

		pvc, err := s.switcher.RenamePVC(
			ctx,
			session,
			&reverse,
			func() error { return s.store.Update(ctx, session) },
		)
		if err != nil {
			return s.failContext(ctx, session, err)
		}

		now := metav1.NewTime(s.now().UTC())
		status.Activation.ActivePVC = domain.ObjectReference{
			APIVersion:      domain.CoreAPIVersion,
			Kind:            domain.KindPersistentVolumeClaim,
			Namespace:       pvc.Namespace,
			Name:            pvc.Name,
			UID:             pvc.UID,
			ResourceVersion: pvc.ResourceVersion,
		}
		status.Activation.RolledBackAt = &now

		return s.finish(ctx, session, domain.PhaseRolledBack, "PVC name restored")
	}

	if wasRunning {
		if err := s.controllers.Pause(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	for index := len(session.Spec.Volumes) - 1; index >= 0; index-- {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]
		s.logInfo(
			"volume rollback started",
			"session",
			session.ID,
			"index",
			index,
			"source",
			volume.SourcePVC.Namespace+"/"+volume.SourcePVC.Name,
			"destination",
			volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
			"pv",
			volume.DestinationPV.Name,
		)

		if err := s.switcher.RollbackVolume(ctx, session, volume, status, func() error {
			return s.store.Update(ctx, session)
		}); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	if err := s.verifyRollbackStorage(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.controllers.Resume(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseRolledBack,
		"source volumes restored and workload resumed",
	)
}

func (s *Service) prepareRollback(
	ctx context.Context,
	session *domain.Session,
	wasRunning bool,
) error {
	if err := s.ValidateRollback(ctx, session); err != nil {
		return err
	}

	if !wasRunning {
		return nil
	}

	return s.checkpointRollbackPods(ctx, session)
}

func (s *Service) checkpointRollbackPods(ctx context.Context, session *domain.Session) error {
	const operation = "rollback"

	current, err := s.controllers.CurrentRollbackPods(ctx, session)
	if err != nil {
		return err
	}

	workload := session.Spec.WorkloadPtr()
	if workload == nil || len(current) == 0 || !refreshRollbackPodReferences(workload, current) {
		return nil
	}

	if s.store == nil {
		return domain.NewError(
			domain.ErrorInternal,
			operation,
			"session store is required to checkpoint current workload Pods",
		)
	}

	if err := s.store.Update(ctx, session); err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			operation,
			"checkpoint current workload Pods",
			err,
		)
	}

	return nil
}

func refreshRollbackPodReferences(
	workload *domain.WorkloadSpec,
	current []domain.ObjectReference,
) bool {
	beforePod := workload.Pod
	beforeAffected := slices.Clone(workload.AffectedPods)

	switch workload.Adapter {
	case domain.WorkloadDeployment, domain.WorkloadGrafana:
		workload.AffectedPods = slices.Clone(current)
		workload.Pod = current[0]
	default:
		byName := make(map[string]domain.ObjectReference, len(current))
		for _, ref := range current {
			byName[ref.Namespace+"/"+ref.Name] = ref
		}

		if ref, ok := byName[workload.Pod.Namespace+"/"+workload.Pod.Name]; ok {
			workload.Pod = ref
		}

		if len(workload.AffectedPods) == 0 {
			workload.AffectedPods = slices.Clone(current)
		} else {
			seen := make(map[string]struct{}, len(workload.AffectedPods))
			for index, ref := range workload.AffectedPods {
				key := ref.Namespace + "/" + ref.Name
				seen[key] = struct{}{}

				if updated, ok := byName[key]; ok {
					workload.AffectedPods[index] = updated
				}
			}

			for _, ref := range current {
				key := ref.Namespace + "/" + ref.Name

				if _, ok := seen[key]; !ok {
					workload.AffectedPods = append(workload.AffectedPods, ref)
				}
			}
		}
	}

	return workload.Pod != beforePod || !slices.Equal(workload.AffectedPods, beforeAffected)
}

func (s *Service) Rename(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.rename(lockedCtx, session) },
	)
}

func (s *Service) rename(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Operation().RebindsPVC() || len(session.Spec.Volumes) != 1 {
		return domain.NewError(
			domain.ErrorValidation,
			"rebind PVC",
			"session is not a single-volume PVC identity operation",
		)
	}

	if session.Status.Phase == domain.PhaseCompleted {
		return nil
	}

	phase := domain.PhaseRenaming

	message := "renaming PVC while retaining its PV"
	if session.Spec.Operation() == domain.OperationMove {
		phase = domain.PhaseMoving
		message = "moving PVC while retaining its PV"
	}

	valid := session.Status.Phase == domain.PhasePlanned || session.Status.Phase == phase ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == phase)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC",
			fmt.Sprintf("session phase %s cannot rebind PVC", session.Status.Phase),
		)
	}

	if err := s.validateRebindOfflineVolumes(ctx, session); err != nil {
		return err
	}

	if err := s.begin(ctx, session, phase, message); err != nil {
		return err
	}

	s.logInfo(
		"PVC identity change started",
		"session",
		session.ID,
		"operation",
		session.Spec.Operation(),
		"source",
		session.Spec.Volumes[0].SourcePVC.Namespace+"/"+session.Spec.Volumes[0].SourcePVC.Name,
		"destination",
		session.Spec.Volumes[0].DestinationPVC.Namespace+"/"+session.Spec.Volumes[0].DestinationPVC.Name,
	)
	volume := &session.Spec.Volumes[0]
	status := &session.Status.Volumes[0]

	pvc, err := s.switcher.RenamePVC(
		ctx,
		session,
		volume,
		func() error { return s.store.Update(ctx, session) },
	)
	if err != nil {
		return s.failContext(ctx, session, err)
	}

	now := metav1.NewTime(s.now().UTC())
	status.Activation.ActivePVC = domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
	status.Activation.ActivatedAt = &now

	if session.Spec.Operation() == domain.OperationMove {
		return s.finish(ctx, session, domain.PhaseCompleted, "PVC move completed")
	}

	return s.finish(ctx, session, domain.PhaseCompleted, "PVC rename completed")
}
