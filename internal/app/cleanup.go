package app

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

type CleanupOptions struct {
	DeleteTemporary bool
	DeleteRollback  bool
	Finalize        bool
	DeleteSession   bool
}

// CleanupPodBlockerError identifies the Pod and controller that keep a PVC
// inside Kubernetes PVC protection. The CLI uses these fields to render
// connection-aware inspection and deletion commands.
type CleanupPodBlockerError struct {
	PVCNamespace  string
	PVCName       string
	PodNamespace  string
	PodName       string
	PodPhase      corev1.PodPhase
	OwnerKind     string
	OwnerName     string
	OwnerVerified bool
	SessionOwned  bool
	Terminal      bool
	Cause         error
}

func (e *CleanupPodBlockerError) Error() string {
	if e == nil {
		return "cleanup is blocked by a Pod"
	}

	if e.Cause != nil {
		return e.Cause.Error()
	}

	return fmt.Sprintf("cleanup is blocked by Pod %s/%s", e.PodNamespace, e.PodName)
}

func (e *CleanupPodBlockerError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (s *Service) Cleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.cleanup(lockedCtx, session, options)
	})
}

func (s *Service) cleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "cleanup", "session is nil")
	}

	if !cleanupPhaseAllowed(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			fmt.Sprintf("session phase %s is still active", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.ValidateCleanup(ctx, session, options); err != nil {
		return err
	}

	s.logInfo(
		"cleanup started",
		"session",
		session.ID,
		"phase",
		session.Status.Phase,
		"deleteTemporary",
		options.DeleteTemporary,
		"deleteRollback",
		options.DeleteRollback,
		"finalize",
		options.Finalize,
		"deleteSession",
		options.DeleteSession,
	)

	if !session.Spec.Operation().RebindsPVC() &&
		(options.DeleteTemporary || options.DeleteRollback || options.DeleteSession) {
		if err := s.recoverDestinationRefs(ctx, session); err != nil {
			return err
		}
	}

	if err := validateCleanupSessionDeletion(session, options); err != nil {
		return err
	}

	if err := s.validateAbortedSources(ctx, session); err != nil {
		return err
	}

	if err := s.deleteReservationPods(ctx, session); err != nil {
		return err
	}

	if err := s.releaseAbortedSources(ctx, session); err != nil {
		return err
	}

	if options.DeleteTemporary {
		if err := s.deleteTemporaryPVCs(ctx, session); err != nil {
			return err
		}
	}

	if options.DeleteRollback {
		if err := s.deleteRollbackPVs(ctx, session); err != nil {
			return err
		}
	}

	if options.Finalize {
		if err := s.finalizeCleanupResources(ctx, session, options); err != nil {
			return err
		}
	}

	if session.Spec.Type == domain.SessionTypeBackup &&
		(options.Finalize || options.DeleteSession) {
		if err := s.cleanupBackupCredentials(ctx, session); err != nil {
			return err
		}
	}

	if options.DeleteSession {
		if err := s.deleteCleanupSession(ctx, session); err != nil {
			return err
		}
	}

	s.logInfo("cleanup completed", "session", session.ID)

	return nil
}

func (s *Service) cleanupBackupCredentials(ctx context.Context, session *domain.Session) error {
	ref := backupCredentialsCleanupReference(session)
	if ref.Name == "" {
		return nil
	}

	if err := kube.DeleteBackupCredentialsSecret(ctx, s.client, ref, session.ID); err != nil {
		return err
	}

	session.Spec.Backup.CredentialsSecret = domain.ObjectReference{}
	if session.ResourceVersion != "" {
		return s.persist(ctx, session)
	}

	return nil
}

func backupCredentialsCleanupReference(session *domain.Session) domain.ObjectReference {
	if session == nil || session.Spec.Backup == nil {
		return domain.ObjectReference{}
	}

	if session.Spec.Backup.CredentialsSecret.Name != "" {
		return session.Spec.Backup.CredentialsSecret
	}

	if session.ID == "" || session.Spec.SessionNamespace == "" {
		return domain.ObjectReference{}
	}

	return domain.ObjectReference{
		APIVersion: "v1",
		Kind:       "Secret",
		Namespace:  session.Spec.SessionNamespace,
		Name:       kube.BackupCredentialsSecretName(session.ID),
	}
}

func validateCleanupSessionDeletion(session *domain.Session, options CleanupOptions) error {
	if !options.DeleteSession {
		return nil
	}

	if !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			"deleting the session requires --finalize",
		)
	}

	for index := range session.Spec.Volumes {
		_, rollback, _ := cleanupPVRefs(session, &session.Spec.Volumes[index])
		if rollback.Name != "" && !options.DeleteRollback &&
			!preservesCopyOutput(session, options) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"cleanup",
				"deleting the session requires --delete-rollback-pv while a rollback PV is recorded",
			)
		}
	}

	return nil
}

func (s *Service) validateAbortedSources(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase != domain.PhaseAborted {
		return nil
	}

	for index := range session.Spec.Volumes {
		if !uncheckpointedSource(session, index) {
			continue
		}

		if err := s.validateUncheckpointedSource(
			ctx,
			session.ID,
			&session.Spec.Volumes[index],
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) releaseAbortedSources(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase != domain.PhaseAborted {
		return nil
	}

	for index := range session.Spec.Volumes {
		if !uncheckpointedSource(session, index) {
			continue
		}

		if err := s.releaseUncheckpointedSource(
			ctx,
			session.ID,
			&session.Spec.Volumes[index],
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) deleteTemporaryPVCs(ctx context.Context, session *domain.Session) error {
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		if volume.DestinationPVC.UID == "" {
			continue
		}

		if err := s.ensurePVCUnusedForSession(ctx, volume.DestinationPVC, session); err != nil {
			return err
		}

		if err := s.deleteManagedPVC(ctx, session.ID, volume.DestinationPVC); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) deleteRollbackPVs(ctx context.Context, session *domain.Session) error {
	expectedRole := cleanupRollbackRole(session)
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		_, rollback, _ := cleanupPVRefs(session, volume)
		if rollback.Name == "" {
			continue
		}

		var uncheckpointedClaim *domain.ObjectReference
		if uncheckpointedDestination(session, index) {
			uncheckpointedClaim = &volume.DestinationPVC
		}

		if err := s.deleteRollbackPV(
			ctx,
			session.ID,
			rollback,
			expectedRole,
			cleanupRollbackReclaimPolicy(session, volume),
			uncheckpointedClaim,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) finalizeCleanupResources(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	for index := range session.Spec.Volumes {
		if err := s.finalizeCleanupVolume(ctx, session, options, index); err != nil {
			return err
		}
	}

	return s.releaseStandalonePodOwnership(ctx, session)
}

func (s *Service) finalizeCleanupVolume(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
) error {
	volume := &session.Spec.Volumes[index]

	active, _, policy := cleanupPVRefs(session, volume)
	if active.Name == "" || uncheckpointedSource(session, index) {
		return nil
	}

	skip, err := s.validateAbortedActivePV(ctx, session, index, active)
	if err != nil {
		return err
	}

	if skip {
		return nil
	}

	activePVC := session.Status.Volumes[index].Activation.ActivePVC
	if activePVC.Name == "" &&
		(cleanupKeepsSource(session) || session.Status.Phase == domain.PhaseAborted) {
		activePVC = volume.SourcePVC
	}

	if activePVC.Name != "" {
		if err := kube.FinalizePVC(
			ctx,
			s.client,
			activePVC,
			session.ID,
			volume.SourcePVCMetadata,
		); err != nil {
			return err
		}
	}

	if err := s.finalizeActivePV(ctx, session.ID, active, policy); err != nil {
		return err
	}

	if preservesCopyOutput(session, options) && volume.DestinationPV.Name != "" {
		if err := kube.FinalizePVC(
			ctx,
			s.client,
			volume.DestinationPVC,
			session.ID,
			domain.PVCMetadata{},
		); err != nil {
			return err
		}

		return s.finalizeActivePV(
			ctx,
			session.ID,
			volume.DestinationPV,
			volume.DestinationPolicy,
		)
	}

	return nil
}

func (s *Service) validateAbortedActivePV(
	ctx context.Context,
	session *domain.Session,
	index int,
	active domain.ObjectReference,
) (bool, error) {
	if session.Status.Phase != domain.PhaseAborted ||
		session.Status.Volumes[index].Activation.ActivePVC.Name != "" {
		return false, nil
	}

	_, err := s.client.CoreV1().PersistentVolumes().Get(ctx, active.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"read PV "+active.Name,
			err,
		)
	}

	return false, nil
}

func (s *Service) deleteCleanupSession(ctx context.Context, session *domain.Session) error {
	if err := s.store.Delete(ctx, session); err != nil {
		return err
	}

	if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
		deleteCtx, cancelDelete := context.WithTimeout(context.Background(), 10*time.Second)
		err := held.lock.Delete(deleteCtx)

		cancelDelete()

		return err
	}

	if cleaner, ok := s.store.(kube.SessionLeaseCleaner); ok {
		return cleaner.DeleteSessionLease(ctx, session.Spec.SessionNamespace, session.ID)
	}

	return nil
}

func (s *Service) validateStandalonePodOwnershipRelease(
	ctx context.Context,
	session *domain.Session,
) error {
	if session == nil || session.Spec.Workload().Adapter != domain.WorkloadStandalone {
		return nil
	}

	ref := session.Spec.Workload().Pod

	pod, err := s.client.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			fmt.Sprintf("read standalone Pod %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	owner := pod.Annotations[kube.SessionKey]
	if pod.UID != ref.UID {
		if owner == session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"cleanup dry-run",
				fmt.Sprintf(
					"standalone Pod %s/%s UID changed while retaining session ownership",
					ref.Namespace,
					ref.Name,
				),
			)
		}

		return nil
	}

	if owner != "" && owner != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf(
				"standalone Pod %s/%s belongs to migration session %s",
				ref.Namespace,
				ref.Name,
				owner,
			),
		)
	}

	return nil
}

func (s *Service) releaseStandalonePodOwnership(
	ctx context.Context,
	session *domain.Session,
) error {
	if session == nil || session.Spec.Workload().Adapter != domain.WorkloadStandalone {
		return nil
	}

	ref := session.Spec.Workload().Pod

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := s.client.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}

		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"cleanup",
				fmt.Sprintf("read standalone Pod %s/%s", ref.Namespace, ref.Name),
				err,
			)
		}

		owner := pod.Annotations[kube.SessionKey]
		if pod.UID != ref.UID {
			if owner == session.ID {
				return domain.NewError(
					domain.ErrorConflict,
					"cleanup",
					fmt.Sprintf(
						"standalone Pod %s/%s UID changed while retaining session ownership",
						ref.Namespace,
						ref.Name,
					),
				)
			}

			return nil
		}

		if owner == "" {
			return nil
		}

		if owner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"cleanup",
				fmt.Sprintf(
					"standalone Pod %s/%s belongs to migration session %s",
					ref.Namespace,
					ref.Name,
					owner,
				),
			)
		}

		updated := pod.DeepCopy()
		delete(updated.Annotations, kube.SessionKey)

		if len(updated.Annotations) == 0 {
			updated.Annotations = nil
		}

		_, err = s.client.CoreV1().Pods(ref.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"cleanup",
				fmt.Sprintf("release standalone Pod %s/%s ownership", ref.Namespace, ref.Name),
				err,
			)
		}

		return nil
	})
}

func (s *Service) recoverDestinationRefs(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "cleanup", "session is nil")
	}

	recoveredSession := *session
	recoveredSession.Spec.Volumes = slices.Clone(session.Spec.Volumes)

	recovered := false
	for index := range recoveredSession.Spec.Volumes {
		changed, err := s.discoverDestinationRefs(ctx, &recoveredSession, index)
		if err != nil {
			return err
		}

		recovered = recovered || changed
	}

	if !recovered {
		return nil
	}

	if s.store == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"cleanup",
			"session store is required to checkpoint recovered destination references",
		)
	}

	if err := s.store.Update(ctx, &recoveredSession); err != nil {
		return err
	}

	session.Spec.Volumes = recoveredSession.Spec.Volumes
	session.ResourceVersion = recoveredSession.ResourceVersion

	return nil
}

func uncheckpointedSource(session *domain.Session, index int) bool {
	if session == nil || session.Status.Phase != domain.PhaseAborted || index < 0 ||
		index >= len(session.Status.Volumes) ||
		index >= len(session.Spec.Volumes) {
		return false
	}

	status := session.Status.Volumes[index]
	volume := session.Spec.Volumes[index]

	return !status.Reserved && status.Activation.ActivePVC.Name == "" &&
		volume.DestinationPVC.UID == "" &&
		volume.DestinationPV.Name == ""
}

func uncheckpointedDestination(session *domain.Session, index int) bool {
	if session == nil || session.Status.Phase != domain.PhaseAborted || index < 0 ||
		index >= len(session.Status.Volumes) ||
		index >= len(session.Spec.Volumes) {
		return false
	}

	status := session.Status.Volumes[index]

	return !status.Reserved && status.Sync.Attempts == 0 && status.Activation.ActivePVC.Name == ""
}

func (s *Service) ensurePVCUnused(
	ctx context.Context,
	ref domain.ObjectReference,
	sessionID string,
) error {
	_, err := s.inspectPVCUnused(ctx, ref, sessionID)
	return err
}

func (s *Service) inspectPVCUnused(
	ctx context.Context,
	ref domain.ObjectReference,
	sessionID string,
) (*corev1.PersistentVolumeClaim, error) {
	return s.inspectPVCUnusedWithOperations(ctx, ref, sessionID, nil)
}

func (s *Service) ensurePVCUnusedForSession(
	ctx context.Context,
	ref domain.ObjectReference,
	session *domain.Session,
) error {
	_, err := s.inspectPVCUnusedForSession(ctx, ref, session)
	return err
}

func (s *Service) inspectPVCUnusedForSession(
	ctx context.Context,
	ref domain.ObjectReference,
	session *domain.Session,
) (*corev1.PersistentVolumeClaim, error) {
	if session == nil {
		return nil, domain.NewError(domain.ErrorValidation, "cleanup", "session is nil")
	}

	return s.inspectPVCUnusedWithOperations(
		ctx,
		ref,
		session.ID,
		sessionPVMigrateOperationIDs(session),
	)
}

func (s *Service) inspectPVCUnusedWithOperations(
	ctx context.Context,
	ref domain.ObjectReference,
	sessionID string,
	operationIDs map[string]struct{},
) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("read PVC %s/%s consumers", ref.Namespace, ref.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("read PVC %s/%s returned an empty object", ref.Namespace, ref.Name),
		)
	}

	if ref.UID == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"cleanup",
			fmt.Sprintf("PVC %s/%s UID is required", ref.Namespace, ref.Name),
		)
	}

	if pvc.UID != ref.UID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("PVC %s/%s identity changed", ref.Namespace, ref.Name),
		)
	}

	var (
		pods                  *corev1.PodList
		attachments           *storagev1.VolumeAttachmentList
		podErr, attachmentErr error
	)

	count := 1
	if pvc.Spec.VolumeName != "" {
		count = 2
	}

	parallel.ForLimit(count, 2, func(index int) {
		if index == 0 {
			pods, podErr = s.client.CoreV1().Pods(ref.Namespace).List(ctx, metav1.ListOptions{})
			if podErr == nil && pods == nil {
				podErr = fmt.Errorf(
					"list PVC consumers in %s returned an empty object",
					ref.Namespace,
				)
			}

			return
		}

		attachments, attachmentErr = s.client.StorageV1().
			VolumeAttachments().
			List(ctx, metav1.ListOptions{})
		if attachmentErr == nil && attachments == nil {
			attachmentErr = fmt.Errorf(
				"list VolumeAttachments for PV %s returned an empty object",
				pvc.Spec.VolumeName,
			)
		}
	})

	if podErr != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"list PVC consumers in "+ref.Namespace,
			podErr,
		)
	}

	if pods == nil {
		pods = &corev1.PodList{}
	}

	for _, pod := range pods.Items {
		if sessionID != "" && pod.Labels[kube.SessionKey] == sessionID &&
			pod.Labels[kube.ResourceRoleLabel] == kube.ResourceRoleReservationConsumer {
			continue
		}

		if kube.PodPreventsSafePVCDeletion(&pod, ref.Name) {
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				cause := domain.NewError(
					domain.ErrorPrecondition,
					"cleanup",
					fmt.Sprintf(
						"PVC %s/%s is still protected by terminal Pod %s (phase %s); delete the Pod object before cleanup",
						ref.Namespace,
						ref.Name,
						pod.Name,
						pod.Status.Phase,
					),
				)

				return nil, s.cleanupPodBlockerError(
					ctx,
					ref,
					&pod,
					sessionID,
					operationIDs,
					true,
					cause,
				)
			}

			cause := domain.NewError(
				domain.ErrorPrecondition,
				"cleanup",
				fmt.Sprintf(
					"PVC %s/%s is still referenced by Pod %s (phase %s); stop its controller or delete the Pod before cleanup",
					ref.Namespace,
					ref.Name,
					pod.Name,
					pod.Status.Phase,
				),
			)

			return nil, s.cleanupPodBlockerError(
				ctx,
				ref,
				&pod,
				sessionID,
				operationIDs,
				false,
				cause,
			)
		}
	}

	if pvc.Spec.VolumeName == "" {
		return pvc, nil
	}

	if attachmentErr != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"list attachments for PV "+pvc.Spec.VolumeName,
			attachmentErr,
		)
	}

	if attachments == nil {
		attachments = &storagev1.VolumeAttachmentList{}
	}

	for _, attachment := range attachments.Items {
		if attachment.Spec.Source.PersistentVolumeName != nil &&
			*attachment.Spec.Source.PersistentVolumeName == pvc.Spec.VolumeName &&
			attachment.Status.Attached {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"cleanup",
				fmt.Sprintf(
					"PVC %s/%s still has an attached PV on node %s",
					ref.Namespace,
					ref.Name,
					attachment.Spec.NodeName,
				),
			)
		}
	}

	return pvc, nil
}

func (s *Service) cleanupPodBlockerError(
	ctx context.Context,
	pvc domain.ObjectReference,
	pod *corev1.Pod,
	sessionID string,
	operationIDs map[string]struct{},
	terminal bool,
	cause error,
) error {
	ownerKind, ownerName, ownerVerified := s.resolveCleanupPodOwner(ctx, pod)
	sessionOwned := (sessionID != "" && pod.Labels[kube.ManagedByLabel] == kube.ManagedByValue && pod.Labels[kube.SessionKey] == sessionID) ||
		isPVMigrateToolForOperationIDs(pod, operationIDs)

	return &CleanupPodBlockerError{
		PVCNamespace:  pvc.Namespace,
		PVCName:       pvc.Name,
		PodNamespace:  pod.Namespace,
		PodName:       pod.Name,
		PodPhase:      pod.Status.Phase,
		OwnerKind:     ownerKind,
		OwnerName:     ownerName,
		OwnerVerified: ownerVerified,
		SessionOwned:  sessionOwned,
		Terminal:      terminal,
		Cause:         cause,
	}
}

func sessionPVMigrateOperationIDs(session *domain.Session) map[string]struct{} {
	operationIDs := make(map[string]struct{})
	if session == nil {
		return operationIDs
	}

	for index, volume := range session.Spec.Volumes {
		if index >= len(session.Status.Volumes) {
			break
		}

		for attempt := 1; attempt <= session.Status.Volumes[index].Sync.Attempts; attempt++ {
			for _, mode := range []copyengine.Mode{copyengine.ModeWarm, copyengine.ModeFinal} {
				operationIDs[copyengine.OperationID(copyengine.Request{
					SessionID: session.ID,
					Source:    volume.SourcePVC,
					Mode:      mode,
					Attempt:   attempt,
				})] = struct{}{}
			}
		}
	}

	return operationIDs
}

func isPVMigrateToolForOperationIDs(pod *corev1.Pod, operationIDs map[string]struct{}) bool {
	instance, tool := pvmigrateToolInstance(pod)
	if !tool || len(operationIDs) == 0 {
		return false
	}

	for operationID := range operationIDs {
		if strings.HasPrefix(instance, "pv-migrate-"+operationID+"-") {
			return true
		}
	}

	return false
}

func (s *Service) resolveCleanupPodOwner(
	ctx context.Context,
	pod *corev1.Pod,
) (string, string, bool) {
	if pod == nil {
		return "", "", false
	}

	owner := controllerOwnerReference(pod.OwnerReferences)
	if owner == nil {
		return "", "", false
	}

	if s == nil || s.client == nil {
		return owner.Kind, owner.Name, false
	}

	switch owner.Kind {
	case "Job":
		job, err := s.client.BatchV1().Jobs(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		return owner.Kind, owner.Name, err == nil && job != nil && ownerReferenceMatches(owner, job)
	case "Deployment":
		deployment, err := s.client.AppsV1().
			Deployments(pod.Namespace).
			Get(ctx, owner.Name, metav1.GetOptions{})

		return owner.Kind, owner.Name, err == nil && deployment != nil &&
			ownerReferenceMatches(owner, deployment)
	case "StatefulSet":
		statefulSet, err := s.client.AppsV1().
			StatefulSets(pod.Namespace).
			Get(ctx, owner.Name, metav1.GetOptions{})

		return owner.Kind, owner.Name, err == nil && statefulSet != nil &&
			ownerReferenceMatches(owner, statefulSet)
	case "DaemonSet":
		daemonSet, err := s.client.AppsV1().
			DaemonSets(pod.Namespace).
			Get(ctx, owner.Name, metav1.GetOptions{})

		return owner.Kind, owner.Name, err == nil && daemonSet != nil &&
			ownerReferenceMatches(owner, daemonSet)
	case "ReplicaSet":
		return s.resolveCleanupReplicaSetOwner(ctx, pod.Namespace, owner)
	default:
		return owner.Kind, owner.Name, false
	}
}

func (s *Service) resolveCleanupReplicaSetOwner(
	ctx context.Context,
	namespace string,
	owner *metav1.OwnerReference,
) (string, string, bool) {
	replicaSet, err := s.client.AppsV1().
		ReplicaSets(namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil || replicaSet == nil || !ownerReferenceMatches(owner, replicaSet) {
		return owner.Kind, owner.Name, false
	}

	parent := controllerOwnerReference(replicaSet.OwnerReferences)
	if parent == nil {
		return owner.Kind, owner.Name, true
	}

	if parent.Kind != "Deployment" {
		return parent.Kind, parent.Name, false
	}

	deployment, err := s.client.AppsV1().
		Deployments(namespace).
		Get(ctx, parent.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) ||
		(err == nil && deployment != nil && !ownerReferenceMatches(parent, deployment)) {
		// The verified ReplicaSet is orphaned; deleting it cannot delete a
		// replacement Deployment that happens to reuse the old name.
		return owner.Kind, owner.Name, true
	}

	if err != nil || deployment == nil || !ownerReferenceMatches(parent, deployment) {
		return parent.Kind, parent.Name, false
	}

	return parent.Kind, parent.Name, true
}

func ownerReferenceMatches(owner *metav1.OwnerReference, object metav1.Object) bool {
	return owner != nil && object != nil && owner.UID != "" && owner.Name == object.GetName() &&
		owner.UID == object.GetUID()
}

func controllerOwnerReference(owners []metav1.OwnerReference) *metav1.OwnerReference {
	for index := range owners {
		if owners[index].Controller != nil && *owners[index].Controller {
			return &owners[index]
		}
	}

	return nil
}

// discoverDestinationRefs recovers references lost after a destination PVC
// was created but before the session checkpoint reached the store. The exact
// PVC name and session ownership labels keep this lookup scoped to one
// migration and protect foreign resources from cleanup.
func (s *Service) discoverDestinationRefs(
	ctx context.Context,
	session *domain.Session,
	index int,
) (bool, error) {
	if session == nil || index < 0 || index >= len(session.Spec.Volumes) {
		return false, domain.NewError(
			domain.ErrorInternal,
			"cleanup",
			"destination recovery index is invalid",
		)
	}

	volume := &session.Spec.Volumes[index]
	sessionID := session.ID

	if volume.DestinationPVC.Name == "" || volume.DestinationPVC.UID != "" {
		return false, nil
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.DestinationPVC.Namespace).
		Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf(
				"read destination PVC %s/%s",
				volume.DestinationPVC.Namespace,
				volume.DestinationPVC.Name,
			),
			err,
		)
	}

	if pvc.Labels[kube.ManagedByLabel] != kube.ManagedByValue ||
		pvc.Labels[kube.SessionKey] != sessionID ||
		pvc.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleDestination ||
		pvc.Annotations[kube.SessionKey] != sessionID ||
		pvc.Annotations[kube.SourcePVCUIDAnnotation] != string(volume.SourcePVC.UID) {
		return false, domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf(
				"destination PVC %s/%s identity or session ownership changed",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	volume.DestinationPVC.UID = pvc.UID

	volume.DestinationPVC.ResourceVersion = pvc.ResourceVersion
	if pvc.Spec.VolumeName == "" || volume.DestinationPV.Name != "" {
		return true, nil
	}

	pv, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"read destination PV "+pvc.Spec.VolumeName,
			err,
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("destination PV %s claimRef does not match destination PVC", pv.Name),
		)
	}

	owned := pv.Labels[kube.ManagedByLabel] == kube.ManagedByValue &&
		pv.Labels[kube.SessionKey] == sessionID &&
		pv.Labels[kube.ResourceRoleLabel] == kube.ResourceRoleDestination
	if !owned &&
		(!uncheckpointedDestination(session, index) || !uncheckpointedDestinationPVMatches(pv, volume.DestinationPVC)) {
		return false, domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("destination PV %s identity or session ownership changed", pv.Name),
		)
	}

	volume.DestinationPV = domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolume,
		Name:            pv.Name,
		UID:             pv.UID,
		ResourceVersion: pv.ResourceVersion,
	}

	volume.DestinationPolicy = corev1.PersistentVolumeReclaimPolicy(
		pv.Annotations[kube.OriginalPolicyAnnotation],
	)
	if volume.DestinationPolicy == "" {
		volume.DestinationPolicy = pv.Spec.PersistentVolumeReclaimPolicy
	}

	return true, nil
}

func cleanupPVRefs(
	session *domain.Session,
	volume *domain.VolumeSpec,
) (active, rollback domain.ObjectReference, policy corev1.PersistentVolumeReclaimPolicy) {
	if cleanupKeepsSource(session) {
		return volume.SourcePV, volume.DestinationPV, volume.SourceReclaimPolicy
	}

	if session.Spec.Operation().RebindsPVC() {
		return volume.SourcePV, domain.ObjectReference{}, volume.SourceReclaimPolicy
	}

	if session.Status.Phase == domain.PhaseRolledBack ||
		session.Status.Phase == domain.PhaseAborted {
		return volume.SourcePV, volume.DestinationPV, volume.SourceReclaimPolicy
	}

	return volume.DestinationPV, volume.SourcePV, volume.DestinationPolicy
}

func cleanupRollbackReclaimPolicy(
	session *domain.Session,
	volume *domain.VolumeSpec,
) corev1.PersistentVolumeReclaimPolicy {
	if cleanupKeepsSource(session) || session.Status.Phase == domain.PhaseRolledBack ||
		session.Status.Phase == domain.PhaseAborted {
		return volume.DestinationPolicy
	}

	if session.Spec.Operation().RebindsPVC() {
		return ""
	}

	return volume.SourceReclaimPolicy
}

func cleanupKeepsSource(session *domain.Session) bool {
	if session == nil {
		return false
	}

	switch session.Spec.Operation() {
	case domain.OperationReserve, domain.OperationCopy:
		return true
	default:
		return false
	}
}

func preservesCopyOutput(session *domain.Session, options CleanupOptions) bool {
	return session != nil && session.Spec.Operation() == domain.OperationCopy &&
		session.Status.Phase == domain.PhaseWarmCopied &&
		!options.DeleteTemporary &&
		!options.DeleteRollback
}

func cleanupPhaseAllowed(session *domain.Session) bool {
	if session == nil {
		return false
	}

	phase := session.Status.Phase
	if phase == domain.PhaseAborted {
		return true
	}

	switch session.Spec.Operation() {
	case domain.OperationReserve:
		return phase == domain.PhaseReserved
	case domain.OperationCopy:
		return phase == domain.PhaseWarmCopied
	default:
		return phase == domain.PhaseCompleted || phase == domain.PhaseRolledBack
	}
}

func cleanupRollbackRole(session *domain.Session) string {
	if cleanupKeepsSource(session) || session.Status.Phase == domain.PhaseAborted {
		return kube.ResourceRoleDestination
	}
	return kube.ResourceRoleRollback
}

func (s *Service) deleteReservationPods(ctx context.Context, session *domain.Session) error {
	namespaces := map[string]struct{}{}
	for _, volume := range session.Spec.Volumes {
		namespaces[volume.DestinationPVC.Namespace] = struct{}{}
	}

	selector := reservationPodSelector(session.ID)
	for namespace := range namespaces {
		pods, err := s.client.CoreV1().
			Pods(namespace).
			List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil && !apierrors.IsNotFound(err) {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"cleanup",
				"list reservation Pods in "+namespace,
				err,
			)
		}

		if err != nil {
			continue
		}

		for i := range pods.Items {
			uid := pods.Items[i].UID
			if err := s.client.CoreV1().
				Pods(namespace).
				Delete(ctx, pods.Items[i].Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil &&
				!apierrors.IsNotFound(err) {
				return domain.WrapError(
					domain.ErrorKubernetes,
					"cleanup",
					fmt.Sprintf("delete Pod %s/%s", namespace, pods.Items[i].Name),
					err,
				)
			}

			podName := pods.Items[i].Name
			s.logInfo(
				"waiting for reservation Pod deletion",
				"session",
				session.ID,
				"namespace",
				namespace,
				"pod",
				podName,
			)

			if err := kube.WaitFor(
				ctx,
				time.Second,
				fmt.Sprintf("reservation Pod %s/%s deletion", namespace, podName),
				func(waitCtx context.Context) (bool, error) {
					current, getErr := s.client.CoreV1().
						Pods(namespace).
						Get(waitCtx, podName, metav1.GetOptions{})
					if apierrors.IsNotFound(getErr) {
						return true, nil
					}

					if getErr == nil && current.UID != uid {
						return false, domain.NewError(
							domain.ErrorConflict,
							"cleanup",
							fmt.Sprintf(
								"reservation Pod %s/%s was replaced while waiting for deletion",
								namespace,
								podName,
							),
						)
					}

					return false, getErr
				},
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateReservationPods inventories every reservation consumer before any
// cleanup deletion. This keeps a later namespace-list failure from leaving a
// partially cleaned session and limits deletion to pvc-migrate-owned Pods.
func (s *Service) validateReservationPods(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "cleanup dry-run", "session is nil")
	}

	namespaces := make(map[string]struct{})
	for _, volume := range session.Spec.Volumes {
		if volume.DestinationPVC.Namespace != "" {
			namespaces[volume.DestinationPVC.Namespace] = struct{}{}
		}
	}

	selector := reservationPodSelector(session.ID)
	for namespace := range namespaces {
		pods, err := s.client.CoreV1().
			Pods(namespace).
			List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil && !apierrors.IsNotFound(err) {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"cleanup dry-run",
				"list reservation Pods in "+namespace,
				err,
			)
		}

		if err == nil && pods == nil {
			return domain.NewError(
				domain.ErrorKubernetes,
				"cleanup dry-run",
				fmt.Sprintf("list reservation Pods in %s returned an empty object", namespace),
			)
		}

		if err != nil {
			continue
		}

		for index := range pods.Items {
			pod := &pods.Items[index]
			if pod.Labels[kube.ManagedByLabel] != kube.ManagedByValue ||
				pod.Labels[kube.SessionKey] != session.ID ||
				pod.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleReservationConsumer {
				return domain.NewError(
					domain.ErrorConflict,
					"cleanup dry-run",
					fmt.Sprintf(
						"reservation Pod %s/%s is not owned by session %s",
						pod.Namespace,
						pod.Name,
						session.ID,
					),
				)
			}
		}
	}

	return nil
}

func reservationPodSelector(sessionID string) string {
	return kube.ManagedByLabel + "=" + kube.ManagedByValue + "," + kube.SessionKey + "=" + sessionID + "," + kube.ResourceRoleLabel + "=" + kube.ResourceRoleReservationConsumer
}

func (s *Service) releaseUncheckpointedSource(
	ctx context.Context,
	sessionID string,
	volume *domain.VolumeSpec,
) error {
	if volume == nil || volume.SourcePVC.Name == "" || volume.SourcePV.Name == "" {
		return nil
	}

	if err := s.validateUncheckpointedSource(ctx, sessionID, volume); err != nil {
		return err
	}

	if err := kube.ReleasePVC(ctx, s.client, volume.SourcePVC, sessionID); err != nil {
		return err
	}

	pv, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"read source PV "+volume.SourcePV.Name,
			err,
		)
	}

	if pv.UID != volume.SourcePV.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PV %s identity changed", pv.Name),
		)
	}

	if pv.Labels[kube.SessionKey] != sessionID {
		return nil
	}

	if role := pv.Labels[kube.ResourceRoleLabel]; role != kube.ResourceRoleSource {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PV %s has unexpected session role %q", pv.Name, role),
		)
	}

	return s.finalizeActivePV(ctx, sessionID, volume.SourcePV, volume.SourceReclaimPolicy)
}

// validateUncheckpointedSource mirrors the ownership checks performed before
// releasing a source acquired before the reservation checkpoint was stored.
// It deliberately permits an unowned PV so cleanup can close a session whose
// inventory references never became session-owned resources.
func (s *Service) validateUncheckpointedSource(
	ctx context.Context,
	sessionID string,
	volume *domain.VolumeSpec,
) error {
	if volume == nil || volume.SourcePVC.Name == "" || volume.SourcePV.Name == "" {
		return nil
	}

	pvc, pvcErr := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if pvcErr != nil && !apierrors.IsNotFound(pvcErr) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("read source PVC %s/%s", volume.SourcePVC.Namespace, volume.SourcePVC.Name),
			pvcErr,
		)
	}

	if pvcErr == nil && pvc.UID != volume.SourcePVC.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PVC %s/%s identity changed", pvc.Namespace, pvc.Name),
		)
	}

	pv, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			"read source PV "+volume.SourcePV.Name,
			err,
		)
	}

	if pv.UID != volume.SourcePV.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PV %s identity changed", pv.Name),
		)
	}

	if pv.Labels[kube.SessionKey] != sessionID {
		return nil
	}

	if pvcErr == nil && pvc.Annotations[kube.SessionKey] != "" &&
		pvc.Annotations[kube.SessionKey] != sessionID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf(
				"source PVC %s/%s belongs to session %s",
				pvc.Namespace,
				pvc.Name,
				pvc.Annotations[kube.SessionKey],
			),
		)
	}

	if role := pv.Labels[kube.ResourceRoleLabel]; role != kube.ResourceRoleSource {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("source PV %s has unexpected session role %q", pv.Name, role),
		)
	}

	if volume.SourceReclaimPolicy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup",
			fmt.Sprintf("source PV %s has no recorded reclaim policy", pv.Name),
		)
	}

	return nil
}

func (s *Service) deleteManagedPVC(
	ctx context.Context,
	sessionID string,
	ref domain.ObjectReference,
) error {
	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("read PVC %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	if pvc.UID != ref.UID || pvc.Labels[kube.SessionKey] != sessionID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup",
			fmt.Sprintf("PVC %s/%s identity or session ownership changed", ref.Namespace, ref.Name),
		)
	}

	uid := pvc.UID

	resourceVersion := pvc.ResourceVersion
	if err := s.client.CoreV1().
		PersistentVolumeClaims(ref.Namespace).
		Delete(ctx, ref.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}}); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup",
			fmt.Sprintf("delete PVC %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	return nil
}

func (s *Service) deleteRollbackPV(
	ctx context.Context,
	sessionID string,
	ref domain.ObjectReference,
	expectedRole string,
	policy corev1.PersistentVolumeReclaimPolicy,
	uncheckpointedClaim *domain.ObjectReference,
) error {
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup rollback PV",
			"read PV "+ref.Name,
			err,
		)
	}

	if !cleanupPVIdentityMatches(pv, ref, sessionID, expectedRole, uncheckpointedClaim) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup rollback PV",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}
	// PVC deletion and PV release are asynchronous Kubernetes controller updates.
	if pv.Status.Phase == corev1.VolumeBound {
		pv, err = s.waitForRollbackPVRelease(
			ctx,
			pv,
			ref,
			sessionID,
			expectedRole,
			uncheckpointedClaim,
		)
		if err != nil || pv == nil {
			return err
		}
	}

	if pv.Status.Phase != corev1.VolumeReleased && pv.Status.Phase != corev1.VolumeAvailable {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup rollback PV",
			fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase),
		)
	}

	pv, err = s.restoreRollbackPVReclaimPolicy(
		ctx,
		ref,
		sessionID,
		expectedRole,
		policy,
		uncheckpointedClaim,
	)
	if err != nil || pv == nil {
		return err
	}

	uid, resourceVersion := pv.UID, pv.ResourceVersion
	if err := s.client.CoreV1().
		PersistentVolumes().
		Delete(ctx, pv.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}}); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup rollback PV",
			"delete PV "+pv.Name,
			err,
		)
	}

	return s.waitForRollbackPVDeletion(ctx, ref)
}

func (s *Service) restoreRollbackPVReclaimPolicy(
	ctx context.Context,
	ref domain.ObjectReference,
	sessionID string,
	expectedRole string,
	policy corev1.PersistentVolumeReclaimPolicy,
	uncheckpointedClaim *domain.ObjectReference,
) (*corev1.PersistentVolume, error) {
	if !validReclaimPolicy(policy) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"cleanup rollback PV",
			fmt.Sprintf("PV %s has no valid original reclaim policy", ref.Name),
		)
	}

	var prepared *corev1.PersistentVolume

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.client.CoreV1().
			PersistentVolumes().
			Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			prepared = nil

			return nil
		}

		if err != nil {
			return err
		}

		if !cleanupPVIdentityMatches(current, ref, sessionID, expectedRole, uncheckpointedClaim) ||
			(current.Status.Phase != corev1.VolumeReleased && current.Status.Phase != corev1.VolumeAvailable) ||
			!cleanupRollbackPVPolicyMatches(current, policy, uncheckpointedClaim) {
			return domain.NewError(
				domain.ErrorConflict,
				"cleanup rollback PV",
				fmt.Sprintf(
					"PV %s identity, ownership, state, or reclaim policy changed",
					ref.Name,
				),
			)
		}

		if current.Spec.PersistentVolumeReclaimPolicy == policy {
			prepared = current

			return nil
		}

		current.Spec.PersistentVolumeReclaimPolicy = policy
		prepared, err = s.client.CoreV1().PersistentVolumes().Update(
			ctx,
			current,
			metav1.UpdateOptions{},
		)

		return err
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return nil, err
		}

		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup rollback PV",
			"restore reclaim policy for PV "+ref.Name,
			err,
		)
	}

	return prepared, nil
}

func cleanupRollbackPVPolicyMatches(
	pv *corev1.PersistentVolume,
	policy corev1.PersistentVolumeReclaimPolicy,
	uncheckpointedClaim *domain.ObjectReference,
) bool {
	if pv == nil || !validReclaimPolicy(policy) {
		return false
	}

	if uncheckpointedClaim != nil && uncheckpointedDestinationPVMatches(pv, *uncheckpointedClaim) {
		return policy == corev1.PersistentVolumeReclaimDelete
	}

	return pv.Annotations[kube.OriginalPolicyAnnotation] == string(policy) &&
		(pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain ||
			pv.Spec.PersistentVolumeReclaimPolicy == policy)
}

func (s *Service) waitForRollbackPVDeletion(
	ctx context.Context,
	ref domain.ObjectReference,
) error {
	err := kube.WaitFor(
		ctx,
		time.Second,
		fmt.Sprintf("PV %s deletion", ref.Name),
		func(waitCtx context.Context) (bool, error) {
			current, err := s.client.CoreV1().PersistentVolumes().Get(
				waitCtx,
				ref.Name,
				metav1.GetOptions{},
			)
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			if err != nil {
				return false, err
			}

			if current.UID != ref.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"cleanup rollback PV",
					fmt.Sprintf("PV %s was replaced while waiting for deletion", ref.Name),
				)
			}

			return false, nil
		},
	)
	if err != nil && domain.CategoryOf(err) == domain.ErrorInternal {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup rollback PV",
			"wait for PV "+ref.Name+" deletion",
			err,
		)
	}

	return err
}

func (s *Service) waitForRollbackPVRelease(
	ctx context.Context,
	pv *corev1.PersistentVolume,
	ref domain.ObjectReference,
	sessionID, expectedRole string,
	uncheckpointedClaim *domain.ObjectReference,
) (*corev1.PersistentVolume, error) {
	if err := s.validateDeletingRollbackClaim(ctx, pv); err != nil {
		return nil, err
	}

	s.logInfo("waiting for PV release", "session", sessionID, "pv", pv.Name)

	err := kube.WaitFor(
		ctx,
		time.Second,
		fmt.Sprintf("PV %s release", pv.Name),
		func(waitCtx context.Context) (bool, error) {
			return s.rollbackPVReleased(
				waitCtx,
				ref,
				sessionID,
				expectedRole,
				uncheckpointedClaim,
			)
		},
	)
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorInternal {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				"cleanup rollback PV",
				fmt.Sprintf("wait for PV %s release", ref.Name),
				err,
			)
		}

		return nil, err
	}

	current, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup rollback PV",
			fmt.Sprintf("read PV %s after release", ref.Name),
			err,
		)
	}

	if !cleanupPVIdentityMatches(current, ref, sessionID, expectedRole, uncheckpointedClaim) {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"cleanup rollback PV",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	return current, nil
}

func (s *Service) validateDeletingRollbackClaim(
	ctx context.Context,
	pv *corev1.PersistentVolume,
) error {
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace == "" ||
		pv.Spec.ClaimRef.Name == "" ||
		pv.Spec.ClaimRef.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup rollback PV",
			fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase),
		)
	}

	claim, err := s.client.CoreV1().
		PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).
		Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup rollback PV",
			fmt.Sprintf("read PVC %s/%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name),
			err,
		)
	}

	if claim.UID != pv.Spec.ClaimRef.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup rollback PV",
			fmt.Sprintf("PV %s ClaimRef UID changed", pv.Name),
		)
	}

	if claim.DeletionTimestamp == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup rollback PV",
			fmt.Sprintf(
				"PV %s is still claimed by PVC %s/%s",
				pv.Name,
				pv.Spec.ClaimRef.Namespace,
				pv.Spec.ClaimRef.Name,
			),
		)
	}

	return nil
}

func (s *Service) rollbackPVReleased(
	ctx context.Context,
	ref domain.ObjectReference,
	sessionID, expectedRole string,
	uncheckpointedClaim *domain.ObjectReference,
) (bool, error) {
	current, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, err
	}

	if !cleanupPVIdentityMatches(current, ref, sessionID, expectedRole, uncheckpointedClaim) {
		return false, domain.NewError(
			domain.ErrorConflict,
			"cleanup rollback PV",
			fmt.Sprintf(
				"PV %s identity, ownership, or role changed while waiting for release",
				ref.Name,
			),
		)
	}

	if current.Status.Phase == corev1.VolumeReleased ||
		current.Status.Phase == corev1.VolumeAvailable {
		return true, nil
	}

	if current.Status.Phase != corev1.VolumeBound {
		return false, domain.NewError(
			domain.ErrorPrecondition,
			"cleanup rollback PV",
			fmt.Sprintf(
				"PV %s phase %s must be Released or Available",
				current.Name,
				current.Status.Phase,
			),
		)
	}

	if current.Spec.ClaimRef == nil || current.Spec.ClaimRef.Namespace == "" ||
		current.Spec.ClaimRef.Name == "" ||
		current.Spec.ClaimRef.UID == "" {
		return false, domain.NewError(
			domain.ErrorPrecondition,
			"cleanup rollback PV",
			fmt.Sprintf("PV %s phase %s has no ClaimRef", current.Name, current.Status.Phase),
		)
	}

	claim, claimErr := s.client.CoreV1().
		PersistentVolumeClaims(current.Spec.ClaimRef.Namespace).
		Get(ctx, current.Spec.ClaimRef.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(claimErr) {
		return false, nil
	}

	if claimErr != nil {
		return false, claimErr
	}

	if claim.UID != current.Spec.ClaimRef.UID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"cleanup rollback PV",
			fmt.Sprintf("PV %s ClaimRef UID changed while waiting for release", ref.Name),
		)
	}

	if claim.DeletionTimestamp == nil {
		return false, domain.NewError(
			domain.ErrorPrecondition,
			"cleanup rollback PV",
			fmt.Sprintf(
				"PV %s is still claimed by PVC %s/%s",
				current.Name,
				current.Spec.ClaimRef.Namespace,
				current.Spec.ClaimRef.Name,
			),
		)
	}

	return false, nil
}

func cleanupPVIdentityMatches(
	pv *corev1.PersistentVolume,
	ref domain.ObjectReference,
	sessionID, expectedRole string,
	uncheckpointedClaim *domain.ObjectReference,
) bool {
	if pv == nil || pv.UID != ref.UID {
		return false
	}

	if pv.Labels[kube.SessionKey] == sessionID &&
		pv.Labels[kube.ResourceRoleLabel] == expectedRole {
		return true
	}

	return uncheckpointedClaim != nil &&
		uncheckpointedDestinationPVMatches(pv, *uncheckpointedClaim)
}

func uncheckpointedDestinationPVMatches(
	pv *corev1.PersistentVolume,
	claim domain.ObjectReference,
) bool {
	if pv == nil || claim.Namespace == "" || claim.Name == "" || claim.UID == "" ||
		pv.Spec.ClaimRef == nil {
		return false
	}

	if pv.Labels[kube.ManagedByLabel] != "" || pv.Labels[kube.SessionKey] != "" ||
		pv.Labels[kube.ResourceRoleLabel] != "" ||
		pv.Annotations[kube.SessionKey] != "" {
		return false
	}

	if pv.Annotations[kube.OriginalPolicyAnnotation] != "" ||
		pv.Annotations[kube.PairedPVAnnotation] != "" ||
		pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		return false
	}

	return pv.Spec.ClaimRef.Namespace == claim.Namespace && pv.Spec.ClaimRef.Name == claim.Name &&
		pv.Spec.ClaimRef.UID == claim.UID
}

func (s *Service) finalizeActivePV(
	ctx context.Context,
	sessionID string,
	ref domain.ObjectReference,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"finalize active PV",
			fmt.Sprintf("PV %s has no recorded reclaim policy", ref.Name),
		)
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		role := pv.Labels[kube.ResourceRoleLabel]
		if pv.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"finalize active PV",
				fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
			)
		}

		if pv.Labels[kube.SessionKey] == "" && role == "" &&
			pv.Spec.PersistentVolumeReclaimPolicy == policy &&
			pv.Annotations[kube.OriginalPolicyAnnotation] == "" {
			return nil
		}

		if pv.Labels[kube.SessionKey] != sessionID ||
			(role != kube.ResourceRoleActive && role != kube.ResourceRoleSource && role != kube.ResourceRoleRename && role != kube.ResourceRoleDestination) {
			return domain.NewError(
				domain.ErrorConflict,
				"finalize active PV",
				fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
			)
		}

		pv.Spec.PersistentVolumeReclaimPolicy = policy
		delete(pv.Labels, kube.SessionKey)
		delete(pv.Labels, kube.ResourceRoleLabel)

		if pv.Labels[kube.ManagedByLabel] == kube.ManagedByValue {
			delete(pv.Labels, kube.ManagedByLabel)
		}

		delete(pv.Annotations, kube.OriginalPolicyAnnotation)
		delete(pv.Annotations, kube.PairedPVAnnotation)
		_, err = s.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

		return err
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"finalize active PV",
			"update PV "+ref.Name,
			err,
		)
	}

	return nil
}
