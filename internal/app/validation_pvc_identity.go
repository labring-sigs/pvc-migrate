package app

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) validateRebindOfflineVolumes(ctx context.Context, session *domain.Session) error {
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		if err := s.validateRebindTransition(
			ctx,
			session,
			volume,
			volume.SourcePVC,
			volume.DestinationPVC,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateRebindTransition(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	from, to domain.ObjectReference,
) error {
	if from.Namespace == "" || from.Name == "" || to.Namespace == "" || to.Name == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC dry-run",
			"source and destination PVC identities are required",
		)
	}

	fromPVC, fromErr := s.client.CoreV1().
		PersistentVolumeClaims(from.Namespace).
		Get(ctx, from.Name, metav1.GetOptions{})
	sameEndpoint := from.Namespace == to.Namespace && from.Name == to.Name

	toPVC, toErr := fromPVC, fromErr
	if !sameEndpoint {
		toPVC, toErr = s.client.CoreV1().
			PersistentVolumeClaims(to.Namespace).
			Get(ctx, to.Name, metav1.GetOptions{})
	}

	if fromErr != nil && !apierrors.IsNotFound(fromErr) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rebind PVC dry-run",
			fmt.Sprintf("read PVC %s/%s", from.Namespace, from.Name),
			fromErr,
		)
	}

	if toErr != nil && !apierrors.IsNotFound(toErr) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rebind PVC dry-run",
			fmt.Sprintf("read PVC %s/%s", to.Namespace, to.Name),
			toErr,
		)
	}

	fromExists := fromErr == nil

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	if !fromExists &&
		(phase == domain.PhaseRenaming || phase == domain.PhaseMoving || phase == domain.PhaseRollingBack) {
		if phase == domain.PhaseRollingBack {
			// Rollback recreates the original name with a new Kubernetes UID.
			to.UID = ""
		}

		return s.switcher.VerifyPVCRebindRecovery(ctx, session.ID, from, to, volume.SourcePV)
	}

	toExists := toErr == nil
	if !sameEndpoint && fromExists && toExists {
		return domain.NewError(
			domain.ErrorConflict,
			"rebind PVC dry-run",
			fmt.Sprintf(
				"both PVC endpoints %s/%s and %s/%s exist",
				from.Namespace,
				from.Name,
				to.Namespace,
				to.Name,
			),
		)
	}

	if !fromExists && !toExists {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC dry-run",
			fmt.Sprintf(
				"neither PVC endpoint %s/%s nor %s/%s exists",
				from.Namespace,
				from.Name,
				to.Namespace,
				to.Name,
			),
		)
	}

	current := fromPVC

	expected := from
	if !fromExists {
		current = toPVC
		expected = to
	}

	if current == nil || current.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"rebind PVC dry-run",
			"read PVC endpoint returned an empty object",
		)
	}

	if !fromExists && current.Annotations[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"rebind PVC dry-run",
			fmt.Sprintf(
				"destination PVC %s/%s is not owned by session %s",
				current.Namespace,
				current.Name,
				session.ID,
			),
		)
	}

	if expected.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC dry-run",
			fmt.Sprintf("PVC %s/%s has no recorded UID", expected.Namespace, expected.Name),
		)
	}

	if current.UID != expected.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"rebind PVC dry-run",
			fmt.Sprintf("PVC %s/%s UID changed", current.Namespace, current.Name),
		)
	}

	currentRef := domain.ObjectReference{
		APIVersion: domain.CoreAPIVersion,
		Kind:       domain.KindPersistentVolumeClaim,
		Namespace:  current.Namespace,
		Name:       current.Name,
		UID:        current.UID,
	}
	check := *volume
	check.SourcePVC = currentRef
	check.SourcePV = volume.SourcePV
	check.DestinationPVC = currentRef
	check.DestinationPV = volume.SourcePV

	return s.switcher.VerifyVolumeOffline(ctx, &check)
}

func (s *Service) validatePVCIdentityResume(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
	operation domain.Operation,
) error {
	expected := domain.PhaseRenaming
	if operation == domain.OperationMove {
		expected = domain.PhaseMoving
	}

	switch phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	case domain.PhaseRollingBack:
		return s.validateRollback(ctx, session)
	case domain.PhaseAborting:
		return s.validateAbort(ctx, session)
	case domain.PhasePlanned, expected:
		return s.validateRebindOfflineVolumes(ctx, session)
	default:
		return invalidWorkflowResumePhase(phase, operation)
	}
}
