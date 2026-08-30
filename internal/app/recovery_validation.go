package app

import (
	"context"
	"fmt"
	"sort"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Recovery validation owns abort and rollback guards, including the
// workload-consumer checks required only by rollback.
func (s *Service) validateAbort(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "abort dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed || phase == domain.PhaseAborting {
		phase = session.Status.ResumeFrom
	}

	if phase == domain.PhaseRollingBack {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort dry-run",
			"rollback recovery must continue through the owning workflow resume or rollback command",
		)
	}

	if phase == domain.PhaseActivated || phase == domain.PhaseCompleted ||
		phase == domain.PhaseResuming ||
		session.Status.ResumeFrom == domain.PhaseActivating ||
		session.Status.ResumeFrom == domain.PhaseResuming {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort dry-run",
			"activated sessions require rollback",
		)
	}

	if abortRequiresWorkloadResume(session) {
		if err := s.verifySourceStorage(ctx, session); err != nil {
			return err
		}
	}

	if phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced {
		if !isPodMigrationSession(session) {
			return nil
		}
		return s.controllers.VerifyPaused(ctx, session)
	}

	return nil
}

func (s *Service) validateRollback(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "rollback dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if session.Spec.Type == domain.SessionTypeBackup {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback dry-run",
			"backup sessions do not change PVC identity and cannot be rolled back",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseRolledBack {
		return nil
	}

	rollbackOrigin := session.Status.ResumeFrom
	if rollbackOrigin == domain.PhaseRollingBack {
		rollbackOrigin = phaseBefore(session, domain.PhaseRollingBack)
	}

	recoveringRollback := phase == domain.PhaseRollingBack ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseRollingBack)
	wasRunning := phase == domain.PhaseCompleted ||
		((phase == domain.PhaseFailed || phase == domain.PhaseRollingBack) && (rollbackOrigin == domain.PhaseResuming || rollbackOrigin == domain.PhaseCompleted))
	failedDuringCutover := phase == domain.PhaseFailed &&
		(session.Status.ResumeFrom == domain.PhaseActivating || session.Status.ResumeFrom == domain.PhaseResuming || session.Status.ResumeFrom == domain.PhaseRollingBack)

	valid := wasRunning || phase == domain.PhaseActivated || phase == domain.PhaseActivating ||
		phase == domain.PhaseFinalSynced ||
		phase == domain.PhaseRollingBack ||
		failedDuringCutover
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback dry-run",
			fmt.Sprintf("session phase %s cannot roll back", phase),
		)
	}

	if session.Spec.Operation().RebindsPVC() {
		return s.validateRebindRollbackVolumes(ctx, session)
	}

	if err := s.validateRollbackStorage(
		ctx,
		session,
		phase,
		rollbackOrigin,
		recoveringRollback,
		wasRunning,
	); err != nil {
		return err
	}

	if recoveringRollback {
		if wasRunning {
			return s.validateRollbackConsumers(ctx, session)
		}
		return nil
	}

	if wasRunning {
		return s.validateRollbackConsumers(ctx, session)
	}

	if !isPodMigrationSession(session) {
		return s.validateRollbackConsumers(ctx, session)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	return nil
}

func (s *Service) validateRollbackConsumers(ctx context.Context, session *domain.Session) error {
	allowed := make(map[string]types.UID)

	workload := session.Spec.Workload()
	if workload.Adapter != domain.WorkloadNone {
		current, err := s.controllers.CurrentRollbackPods(ctx, session)
		if err != nil {
			return err
		}

		references := append([]domain.ObjectReference{workload.Pod}, workload.AffectedPods...)

		references = append(references, current...)
		for _, ref := range references {
			if ref.Namespace != "" && ref.Name != "" && ref.UID != "" {
				allowed[ref.Namespace+"/"+ref.Name] = ref.UID
			}
		}
	}

	namespaces := make([]string, 0)

	seenNamespaces := make(map[string]struct{})
	for index := range session.Spec.Volumes {
		namespace := session.Spec.Volumes[index].SourcePVC.Namespace
		if _, exists := seenNamespaces[namespace]; exists {
			continue
		}

		seenNamespaces[namespace] = struct{}{}
		namespaces = append(namespaces, namespace)
	}

	sort.Strings(namespaces)

	type podList struct {
		items []corev1.Pod
		err   error
	}

	results := make([]podList, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		pods, err := s.client.CoreV1().Pods(namespaces[index]).List(ctx, metav1.ListOptions{})
		if err == nil && pods == nil {
			err = fmt.Errorf("list Pods in %s returned an empty object", namespaces[index])
		}

		if pods != nil {
			results[index].items = pods.Items
		}

		results[index].err = err
	})

	for volumeIndex := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[volumeIndex]

		result := results[sort.SearchStrings(namespaces, volume.SourcePVC.Namespace)]
		if result.err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"rollback dry-run",
				"list Pods in "+volume.SourcePVC.Namespace,
				result.err,
			)
		}

		for podIndex := range result.items {
			pod := &result.items[podIndex]
			if !kube.PodPreventsSafePVCDeletion(pod, volume.SourcePVC.Name) {
				continue
			}

			expectedUID, controlled := allowed[pod.Namespace+"/"+pod.Name]
			if controlled && expectedUID == pod.UID {
				continue
			}

			return domain.NewError(
				domain.ErrorPrecondition,
				"rollback dry-run",
				fmt.Sprintf(
					"PVC %s/%s is referenced by Pod %s, which is outside the recorded workload pause scope",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					pod.Name,
				),
			)
		}
	}

	return nil
}

func (s *Service) validateRebindRollbackVolumes(
	ctx context.Context,
	session *domain.Session,
) error {
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]

		active := status.Activation.ActivePVC
		if active.Namespace == "" {
			active.Namespace = volume.DestinationPVC.Namespace
		}

		if active.Name == "" || active.UID == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"rollback dry-run",
				fmt.Sprintf("PVC %s has no recorded active identity", volume.SourcePVC.Name),
			)
		}

		if err := s.validateRebindTransition(
			ctx,
			session,
			volume,
			active,
			volume.SourcePVC,
		); err != nil {
			return err
		}
	}

	return nil
}
