package app

import (
	"context"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) abort(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseAborted {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	if session.Status.Phase == domain.PhaseRollingBack ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseRollingBack) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort session",
			"rollback recovery must continue through the owning workflow resume or rollback command",
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

	if err := s.validateAbort(ctx, session); err != nil {
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
		return s.rollbackPVCIdentity(ctx, session)
	}

	return s.rollbackMigrationVolumes(ctx, session, wasRunning)
}

func (s *Service) rollbackPVCIdentity(ctx context.Context, session *domain.Session) error {
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

func (s *Service) rollbackMigrationVolumes(
	ctx context.Context,
	session *domain.Session,
	wasRunning bool,
) error {
	podMigration := isPodMigrationSession(session)
	if podMigration && wasRunning {
		if err := s.controllers.Pause(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	if podMigration {
		if err := s.controllers.VerifyPaused(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
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

	if podMigration {
		if err := s.controllers.Resume(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	message := "source volumes restored; workload remains caller-managed"
	if podMigration {
		message = "source volumes restored and workload resumed"
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseRolledBack,
		message,
	)
}

func (s *Service) prepareRollback(
	ctx context.Context,
	session *domain.Session,
	wasRunning bool,
) error {
	if err := s.validateRollback(ctx, session); err != nil {
		return err
	}

	if !wasRunning || !isPodMigrationSession(session) {
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
