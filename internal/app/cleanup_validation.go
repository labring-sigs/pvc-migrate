package app

import (
	"context"
	"fmt"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Cleanup validation mirrors destructive cleanup guards without mutating
// Kubernetes resources.
func (s *Service) validateCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "cleanup dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if !cleanupPhaseAllowed(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("session phase %s is still active", session.Status.Phase),
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	if (session.Spec.Type == domain.SessionTypeBackup ||
		session.Spec.Type == domain.SessionTypeRestore) &&
		(options.Finalize || options.DeleteSession) {
		if err := kube.ValidateBackupCredentialsSecretCleanup(
			ctx,
			s.client,
			backupCredentialsCleanupReference(session),
			session.ID,
		); err != nil {
			return err
		}
	}

	if err := s.validateReservationPods(ctx, session); err != nil {
		return err
	}

	if options.Finalize {
		if err := s.validateStandalonePodOwnershipRelease(ctx, session); err != nil {
			return err
		}
	}

	s.logInfo(
		"cleanup preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
		"deleteTemporary",
		options.DeleteTemporary,
		"deleteRollback",
		options.DeleteRollback,
		"finalize",
		options.Finalize,
		"deleteSession",
		options.DeleteSession,
	)

	if options.DeleteSession && !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			"deleting the session requires --finalize",
		)
	}

	errors := make([]error, len(session.Spec.Volumes))
	parallel.For(len(session.Spec.Volumes), func(index int) {
		errors[index] = s.validateCleanupVolume(ctx, session, options, index)
	})

	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateCleanupVolume(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
) error {
	volumeValue := session.Spec.Volumes[index]

	volume := &volumeValue
	if !session.Spec.Operation().RebindsPVC() &&
		(options.DeleteTemporary || options.DeleteRollback || options.DeleteSession) {
		recoverySession := *session
		recoverySession.Spec.Volumes = slices.Clone(session.Spec.Volumes)

		recoverySession.Spec.Volumes[index] = volumeValue
		if _, err := s.discoverDestinationRefs(ctx, &recoverySession, index); err != nil {
			return err
		}

		volume = &recoverySession.Spec.Volumes[index]
	}

	active, rollback, policy := cleanupPVRefs(session, volume)
	if options.DeleteSession && rollback.Name != "" && !options.DeleteRollback &&
		!preservesCopyOutput(session, options) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			"deleting the session requires --delete-rollback-pv while a rollback PV is recorded",
		)
	}

	if err := s.validateCleanupPVC(ctx, session, options, volume); err != nil {
		return err
	}

	if err := s.validateCleanupRollbackPV(
		ctx,
		session,
		options,
		index,
		volume,
		rollback,
	); err != nil {
		return err
	}

	if uncheckpointedSource(session, index) {
		return s.validateUncheckpointedSource(ctx, session.ID, volume)
	}

	if !options.Finalize || active.Name == "" {
		return nil
	}

	return s.validateCleanupActivePV(ctx, session, options, index, volume, active, policy)
}

func (s *Service) validateCleanupPVC(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	volume *domain.VolumeSpec,
) error {
	if !options.DeleteTemporary || volume.DestinationPVC.UID == "" {
		return nil
	}

	pvc, err := s.inspectPVCUnusedForSession(ctx, volume.DestinationPVC, session)
	if err != nil || pvc == nil {
		return err
	}

	if pvc.UID != volume.DestinationPVC.UID || pvc.Labels[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PVC %s/%s identity or session ownership changed", pvc.Namespace, pvc.Name),
		)
	}

	return nil
}

func (s *Service) validateCleanupRollbackPV(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
	volume *domain.VolumeSpec,
	rollback domain.ObjectReference,
) error {
	if !options.DeleteRollback || rollback.Name == "" {
		return nil
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, rollback.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			"read rollback PV "+rollback.Name,
			err,
		)
	}

	var uncheckpointedClaim *domain.ObjectReference
	if uncheckpointedDestination(session, index) {
		uncheckpointedClaim = &volume.DestinationPVC
	}

	if !cleanupPVIdentityMatches(
		pv,
		rollback,
		session.ID,
		cleanupRollbackRole(session),
		uncheckpointedClaim,
	) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", pv.Name),
		)
	}

	deletionWillReleaseClaim := options.DeleteTemporary && pv.Status.Phase == corev1.VolumeBound &&
		pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == volume.DestinationPVC.Namespace &&
		pv.Spec.ClaimRef.Name == volume.DestinationPVC.Name && pv.Spec.ClaimRef.UID != "" &&
		pv.Spec.ClaimRef.UID == volume.DestinationPVC.UID
	if pv.Status.Phase != corev1.VolumeReleased && pv.Status.Phase != corev1.VolumeAvailable &&
		!deletionWillReleaseClaim {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase),
		)
	}

	policy := cleanupRollbackReclaimPolicy(session, volume)
	if !cleanupRollbackPVPolicyMatches(pv, policy, uncheckpointedClaim) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s original reclaim policy changed", pv.Name),
		)
	}

	return nil
}

func (s *Service) validateCleanupActivePV(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
	volume *domain.VolumeSpec,
	active domain.ObjectReference,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	maySkipMissing := session.Status.Phase == domain.PhaseAborted &&
		session.Status.Volumes[index].Activation.ActivePVC.Name == ""
	if policy == "" && !maySkipMissing {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", active.Name),
		)
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, active.Name, metav1.GetOptions{})
	if maySkipMissing && apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			"read active PV "+active.Name,
			err,
		)
	}

	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", active.Name),
		)
	}

	if err := validateFinalizablePV(pv, active, session.ID, policy); err != nil {
		return err
	}

	if !preservesCopyOutput(session, options) || volume.DestinationPV.Name == "" {
		return nil
	}

	destinationPV, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			"read copy destination PV "+volume.DestinationPV.Name,
			err,
		)
	}

	return validateFinalizablePV(
		destinationPV,
		volume.DestinationPV,
		session.ID,
		volume.DestinationPolicy,
	)
}

func validateFinalizablePV(
	pv *corev1.PersistentVolume,
	ref domain.ObjectReference,
	sessionID string,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", ref.Name),
		)
	}

	if pv.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	role := pv.Labels[kube.ResourceRoleLabel]
	if pv.Labels[kube.SessionKey] == "" && role == "" &&
		pv.Spec.PersistentVolumeReclaimPolicy == policy &&
		pv.Annotations[kube.OriginalPolicyAnnotation] == "" {
		return nil
	}

	if pv.Labels[kube.SessionKey] != sessionID ||
		(role != kube.ResourceRoleActive && role != kube.ResourceRoleSource && role != kube.ResourceRoleRename && role != kube.ResourceRoleDestination) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	return nil
}
