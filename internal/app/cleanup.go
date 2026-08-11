package app

import (
	"context"
	"fmt"
	"time"

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

func (s *Service) Cleanup(ctx context.Context, session *domain.Session, options CleanupOptions) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.cleanup(lockedCtx, session, options)
	})
}

func (s *Service) cleanup(ctx context.Context, session *domain.Session, options CleanupOptions) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "cleanup", "session is nil")
	}
	if !cleanupPhaseAllowed(session) {
		return domain.NewError(domain.ErrorPrecondition, "cleanup", fmt.Sprintf("session phase %s is still active", session.Status.Phase))
	}
	if !session.Spec.Operation().RebindsPVC() && (options.DeleteTemporary || options.DeleteRollback || options.DeleteSession) {
		for index := range session.Spec.Volumes {
			if err := s.discoverDestinationRefs(ctx, session.ID, &session.Spec.Volumes[index]); err != nil {
				return err
			}
		}
	}
	if options.DeleteSession {
		if !options.Finalize {
			return domain.NewError(domain.ErrorPrecondition, "cleanup", "deleting the session requires --finalize")
		}
		for index := range session.Spec.Volumes {
			_, rollback, _ := cleanupPVRefs(session, &session.Spec.Volumes[index])
			if rollback.Name != "" && !options.DeleteRollback && !preservesCopyOutput(session, options) {
				return domain.NewError(domain.ErrorPrecondition, "cleanup", "deleting the session requires --delete-rollback-pv while a rollback PV is recorded")
			}
		}
	}
	if session.Status.Phase == domain.PhaseAborted {
		for index := range session.Spec.Volumes {
			if !uncheckpointedSource(session, index) {
				continue
			}
			if err := s.validateUncheckpointedSource(ctx, session.ID, &session.Spec.Volumes[index]); err != nil {
				return err
			}
		}
	}
	if err := s.deleteReservationPods(ctx, session); err != nil {
		return err
	}
	if session.Status.Phase == domain.PhaseAborted {
		for index := range session.Spec.Volumes {
			if !uncheckpointedSource(session, index) {
				continue
			}
			if err := s.releaseUncheckpointedSource(ctx, session.ID, &session.Spec.Volumes[index]); err != nil {
				return err
			}
		}
	}
	if options.DeleteTemporary {
		for index := range session.Spec.Volumes {
			volume := &session.Spec.Volumes[index]
			if volume.DestinationPVC.UID == "" {
				continue
			}
			if err := s.ensurePVCUnused(ctx, volume.DestinationPVC, session.ID); err != nil {
				return err
			}
			if err := s.deleteManagedPVC(ctx, session.ID, volume.DestinationPVC); err != nil {
				return err
			}
		}
	}
	if options.DeleteRollback {
		for index := range session.Spec.Volumes {
			_, rollback, _ := cleanupPVRefs(session, &session.Spec.Volumes[index])
			if rollback.Name == "" {
				continue
			}
			expectedRole := cleanupRollbackRole(session)
			if err := s.deleteRollbackPV(ctx, session.ID, rollback, expectedRole); err != nil {
				return err
			}
		}
	}
	if options.Finalize {
		for index := range session.Spec.Volumes {
			volume := &session.Spec.Volumes[index]
			active, _, policy := cleanupPVRefs(session, volume)
			if active.Name == "" {
				continue
			}
			// A workflow can be aborted before its reservation checkpoint. Its
			// source reference is still inventory, so release only ownership
			// acquired by this session and skip active-PV finalization.
			if uncheckpointedSource(session, index) {
				continue
			}
			// An aborted migration before activation has no active ownership to
			// finalize. A controller may have recreated or removed the recorded
			// source PV while the workload was recovering; preserve that workload
			// state and close only this session's retained resources.
			if session.Status.Phase == domain.PhaseAborted && session.Status.Volumes[index].Activation.ActivePVC.Name == "" {
				if _, err := s.client.CoreV1().PersistentVolumes().Get(ctx, active.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
					continue
				} else if err != nil {
					return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("read PV %s", active.Name), err)
				}
			}
			activePVC := session.Status.Volumes[index].Activation.ActivePVC
			if activePVC.Name == "" && cleanupKeepsSource(session) {
				activePVC = volume.SourcePVC
			}
			if activePVC.Name == "" && session.Status.Phase == domain.PhaseAborted {
				activePVC = volume.SourcePVC
			}
			if activePVC.Name != "" {
				if err := kube.FinalizePVC(ctx, s.client, activePVC, session.ID, volume.SourcePVCMetadata); err != nil {
					return err
				}
			}
			if err := s.finalizeActivePV(ctx, session.ID, active, policy); err != nil {
				return err
			}
			if preservesCopyOutput(session, options) && volume.DestinationPV.Name != "" {
				if err := kube.FinalizePVC(ctx, s.client, volume.DestinationPVC, session.ID, domain.PVCMetadata{}); err != nil {
					return err
				}
				if err := s.finalizeActivePV(ctx, session.ID, volume.DestinationPV, volume.DestinationPolicy); err != nil {
					return err
				}
			}
		}
	}
	if options.DeleteSession {
		if err := s.store.Delete(ctx, session); err != nil {
			return err
		}
		if cleaner, ok := s.store.(kube.SessionLeaseCleaner); ok {
			if err := cleaner.DeleteSessionLease(ctx, session.Spec.SessionNamespace, session.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func uncheckpointedSource(session *domain.Session, index int) bool {
	if session == nil || session.Status.Phase != domain.PhaseAborted || index < 0 || index >= len(session.Status.Volumes) || index >= len(session.Spec.Volumes) {
		return false
	}
	status := session.Status.Volumes[index]
	volume := session.Spec.Volumes[index]
	return !status.Reserved && status.Activation.ActivePVC.Name == "" && volume.DestinationPVC.UID == "" && volume.DestinationPV.Name == ""
}

func (s *Service) ensurePVCUnused(ctx context.Context, ref domain.ObjectReference, sessionID string) error {
	_, err := s.inspectPVCUnused(ctx, ref, sessionID)
	return err
}

func (s *Service) inspectPVCUnused(ctx context.Context, ref domain.ObjectReference, sessionID string) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := s.client.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("read PVC %s/%s consumers", ref.Namespace, ref.Name), err)
	}
	if ref.UID != "" && pvc.UID != ref.UID {
		return nil, domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("PVC %s/%s identity changed", ref.Namespace, ref.Name))
	}
	var pods *corev1.PodList
	var attachments *storagev1.VolumeAttachmentList
	var podErr, attachmentErr error
	count := 1
	if pvc.Spec.VolumeName != "" {
		count = 2
	}
	parallel.ForLimit(count, 2, func(index int) {
		if index == 0 {
			pods, podErr = s.client.CoreV1().Pods(ref.Namespace).List(ctx, metav1.ListOptions{})
			if podErr == nil && pods == nil {
				podErr = fmt.Errorf("list PVC consumers in %s returned an empty object", ref.Namespace)
			}
			return
		}
		attachments, attachmentErr = s.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
		if attachmentErr == nil && attachments == nil {
			attachmentErr = fmt.Errorf("list VolumeAttachments for PV %s returned an empty object", pvc.Spec.VolumeName)
		}
	})
	if podErr != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("list PVC consumers in %s", ref.Namespace), podErr)
	}
	if pods == nil {
		pods = &corev1.PodList{}
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if sessionID != "" && pod.Labels[kube.SessionKey] == sessionID && pod.Labels[kube.ResourceRoleLabel] == kube.ResourceRoleReservationConsumer {
			continue
		}
		if kube.PodUsesPVC(&pod, ref.Name) {
			return nil, domain.NewError(domain.ErrorPrecondition, "cleanup", fmt.Sprintf("PVC %s/%s is still referenced by Pod %s", ref.Namespace, ref.Name, pod.Name))
		}
	}
	if pvc.Spec.VolumeName == "" {
		return pvc, nil
	}
	if attachmentErr != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("list attachments for PV %s", pvc.Spec.VolumeName), attachmentErr)
	}
	if attachments == nil {
		attachments = &storagev1.VolumeAttachmentList{}
	}
	for _, attachment := range attachments.Items {
		if attachment.Spec.Source.PersistentVolumeName != nil && *attachment.Spec.Source.PersistentVolumeName == pvc.Spec.VolumeName && attachment.Status.Attached {
			return nil, domain.NewError(domain.ErrorPrecondition, "cleanup", fmt.Sprintf("PVC %s/%s still has an attached PV on node %s", ref.Namespace, ref.Name, attachment.Spec.NodeName))
		}
	}
	return pvc, nil
}

// discoverDestinationRefs recovers references lost after a destination PVC
// was created but before the session checkpoint reached the store. The exact
// PVC name and session ownership labels keep this lookup scoped to one
// migration and protect foreign resources from cleanup.
func (s *Service) discoverDestinationRefs(ctx context.Context, sessionID string, volume *domain.VolumeSpec) error {
	if volume.DestinationPVC.Name == "" || volume.DestinationPVC.UID != "" {
		return nil
	}
	pvc, err := s.client.CoreV1().PersistentVolumeClaims(volume.DestinationPVC.Namespace).Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("read destination PVC %s/%s", volume.DestinationPVC.Namespace, volume.DestinationPVC.Name), err)
	}
	if pvc.Labels[kube.ManagedByLabel] != kube.ManagedByValue || pvc.Labels[kube.SessionKey] != sessionID || pvc.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleDestination || pvc.Annotations[kube.SessionKey] != sessionID || pvc.Annotations[kube.SourcePVCUIDAnnotation] != string(volume.SourcePVC.UID) {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("destination PVC %s/%s identity or session ownership changed", pvc.Namespace, pvc.Name))
	}
	volume.DestinationPVC.UID = pvc.UID
	volume.DestinationPVC.ResourceVersion = pvc.ResourceVersion
	if pvc.Spec.VolumeName == "" || volume.DestinationPV.Name != "" {
		return nil
	}
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("read destination PV %s", pvc.Spec.VolumeName), err)
	}
	if pv.Labels[kube.ManagedByLabel] != kube.ManagedByValue || pv.Labels[kube.SessionKey] != sessionID || pv.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleDestination {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("destination PV %s identity or session ownership changed", pv.Name))
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("destination PV %s claimRef does not match destination PVC", pv.Name))
	}
	volume.DestinationPV = domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolume, Name: pv.Name, UID: pv.UID, ResourceVersion: pv.ResourceVersion}
	volume.DestinationPolicy = corev1.PersistentVolumeReclaimPolicy(pv.Annotations[kube.OriginalPolicyAnnotation])
	if volume.DestinationPolicy == "" {
		volume.DestinationPolicy = pv.Spec.PersistentVolumeReclaimPolicy
	}
	return nil
}

func cleanupPVRefs(session *domain.Session, volume *domain.VolumeSpec) (active domain.ObjectReference, rollback domain.ObjectReference, policy corev1.PersistentVolumeReclaimPolicy) {
	if cleanupKeepsSource(session) {
		return volume.SourcePV, volume.DestinationPV, volume.SourceReclaimPolicy
	}
	if session.Spec.Operation().RebindsPVC() {
		return volume.SourcePV, domain.ObjectReference{}, volume.SourceReclaimPolicy
	}
	if session.Status.Phase == domain.PhaseRolledBack || session.Status.Phase == domain.PhaseAborted {
		return volume.SourcePV, volume.DestinationPV, volume.SourceReclaimPolicy
	}
	return volume.DestinationPV, volume.SourcePV, volume.DestinationPolicy
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
	return session != nil && session.Spec.Operation() == domain.OperationCopy && session.Status.Phase == domain.PhaseWarmCopied && !options.DeleteTemporary && !options.DeleteRollback
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
	selector := kube.SessionKey + "=" + session.ID + "," + kube.ResourceRoleLabel + "=" + kube.ResourceRoleReservationConsumer
	for namespace := range namespaces {
		pods, err := s.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil && !apierrors.IsNotFound(err) {
			return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("list reservation Pods in %s", namespace), err)
		}
		if err != nil {
			continue
		}
		for i := range pods.Items {
			uid := pods.Items[i].UID
			if err := s.client.CoreV1().Pods(namespace).Delete(ctx, pods.Items[i].Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
				return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("delete Pod %s/%s", namespace, pods.Items[i].Name), err)
			}
			podName := pods.Items[i].Name
			if err := kube.WaitFor(ctx, time.Second, fmt.Sprintf("reservation Pod %s/%s deletion", namespace, podName), func(waitCtx context.Context) (bool, error) {
				_, getErr := s.client.CoreV1().Pods(namespace).Get(waitCtx, podName, metav1.GetOptions{})
				if apierrors.IsNotFound(getErr) {
					return true, nil
				}
				return false, getErr
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) releaseUncheckpointedSource(ctx context.Context, sessionID string, volume *domain.VolumeSpec) error {
	if volume == nil || volume.SourcePVC.Name == "" || volume.SourcePV.Name == "" {
		return nil
	}
	if err := s.validateUncheckpointedSource(ctx, sessionID, volume); err != nil {
		return err
	}
	if err := kube.ReleasePVC(ctx, s.client, volume.SourcePVC, sessionID); err != nil {
		return err
	}
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("read source PV %s", volume.SourcePV.Name), err)
	}
	if volume.SourcePV.UID != "" && pv.UID != volume.SourcePV.UID {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("source PV %s identity changed", pv.Name))
	}
	if pv.Labels[kube.SessionKey] != sessionID {
		return nil
	}
	if role := pv.Labels[kube.ResourceRoleLabel]; role != kube.ResourceRoleSource {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("source PV %s has unexpected session role %q", pv.Name, role))
	}
	return s.finalizeActivePV(ctx, sessionID, volume.SourcePV, volume.SourceReclaimPolicy)
}

// validateUncheckpointedSource mirrors the ownership checks performed before
// releasing a source acquired before the reservation checkpoint was stored.
// It deliberately permits an unowned PV so cleanup can close a session whose
// inventory references never became session-owned resources.
func (s *Service) validateUncheckpointedSource(ctx context.Context, sessionID string, volume *domain.VolumeSpec) error {
	if volume == nil || volume.SourcePVC.Name == "" || volume.SourcePV.Name == "" {
		return nil
	}
	pvc, pvcErr := s.client.CoreV1().PersistentVolumeClaims(volume.SourcePVC.Namespace).Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if pvcErr != nil && !apierrors.IsNotFound(pvcErr) {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("read source PVC %s/%s", volume.SourcePVC.Namespace, volume.SourcePVC.Name), pvcErr)
	}
	if pvcErr == nil && volume.SourcePVC.UID != "" && pvc.UID != volume.SourcePVC.UID {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("source PVC %s/%s identity changed", pvc.Namespace, pvc.Name))
	}
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("read source PV %s", volume.SourcePV.Name), err)
	}
	if volume.SourcePV.UID != "" && pv.UID != volume.SourcePV.UID {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("source PV %s identity changed", pv.Name))
	}
	if pv.Labels[kube.SessionKey] != sessionID {
		return nil
	}
	if pvcErr == nil && pvc.Annotations[kube.SessionKey] != "" && pvc.Annotations[kube.SessionKey] != sessionID {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("source PVC %s/%s belongs to session %s", pvc.Namespace, pvc.Name, pvc.Annotations[kube.SessionKey]))
	}
	if role := pv.Labels[kube.ResourceRoleLabel]; role != kube.ResourceRoleSource {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("source PV %s has unexpected session role %q", pv.Name, role))
	}
	if volume.SourceReclaimPolicy == "" {
		return domain.NewError(domain.ErrorPrecondition, "cleanup", fmt.Sprintf("source PV %s has no recorded reclaim policy", pv.Name))
	}
	return nil
}

func (s *Service) deleteManagedPVC(ctx context.Context, sessionID string, ref domain.ObjectReference) error {
	pvc, err := s.client.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("read PVC %s/%s", ref.Namespace, ref.Name), err)
	}
	if pvc.UID != ref.UID || pvc.Labels[kube.SessionKey] != sessionID {
		return domain.NewError(domain.ErrorConflict, "cleanup", fmt.Sprintf("PVC %s/%s identity or session ownership changed", ref.Namespace, ref.Name))
	}
	uid := pvc.UID
	resourceVersion := pvc.ResourceVersion
	if err := s.client.CoreV1().PersistentVolumeClaims(ref.Namespace).Delete(ctx, ref.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}}); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup", fmt.Sprintf("delete PVC %s/%s", ref.Namespace, ref.Name), err)
	}
	return nil
}

func (s *Service) deleteRollbackPV(ctx context.Context, sessionID string, ref domain.ObjectReference, expectedRole string) error {
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup rollback PV", fmt.Sprintf("read PV %s", ref.Name), err)
	}
	if pv.UID != ref.UID || pv.Labels[kube.SessionKey] != sessionID || pv.Labels[kube.ResourceRoleLabel] != expectedRole {
		return domain.NewError(domain.ErrorConflict, "cleanup rollback PV", fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name))
	}
	// PVC deletion and PV release are asynchronous Kubernetes controller updates.
	if pv.Status.Phase == corev1.VolumeBound {
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace == "" || pv.Spec.ClaimRef.Name == "" {
			return domain.NewError(domain.ErrorPrecondition, "cleanup rollback PV", fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase))
		}
		claim, claimErr := s.client.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
		if claimErr == nil {
			if pv.Spec.ClaimRef.UID != "" && claim.UID != pv.Spec.ClaimRef.UID {
				return domain.NewError(domain.ErrorConflict, "cleanup rollback PV", fmt.Sprintf("PV %s ClaimRef UID changed", pv.Name))
			}
			if claim.DeletionTimestamp == nil {
				return domain.NewError(domain.ErrorPrecondition, "cleanup rollback PV", fmt.Sprintf("PV %s is still claimed by PVC %s/%s", pv.Name, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name))
			}
		} else if !apierrors.IsNotFound(claimErr) {
			return domain.WrapError(domain.ErrorKubernetes, "cleanup rollback PV", fmt.Sprintf("read PVC %s/%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name), claimErr)
		}
		if err := kube.WaitFor(ctx, time.Second, fmt.Sprintf("PV %s release", pv.Name), func(waitCtx context.Context) (bool, error) {
			current, getErr := s.client.CoreV1().PersistentVolumes().Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}
			if getErr != nil {
				return false, getErr
			}
			if current.UID != ref.UID || current.Labels[kube.SessionKey] != sessionID || current.Labels[kube.ResourceRoleLabel] != expectedRole {
				return false, domain.NewError(domain.ErrorConflict, "cleanup rollback PV", fmt.Sprintf("PV %s identity, ownership, or role changed while waiting for release", ref.Name))
			}
			if current.Status.Phase == corev1.VolumeReleased || current.Status.Phase == corev1.VolumeAvailable {
				return true, nil
			}
			if current.Status.Phase != corev1.VolumeBound {
				return false, domain.NewError(domain.ErrorPrecondition, "cleanup rollback PV", fmt.Sprintf("PV %s phase %s must be Released or Available", current.Name, current.Status.Phase))
			}
			if current.Spec.ClaimRef == nil || current.Spec.ClaimRef.Namespace == "" || current.Spec.ClaimRef.Name == "" {
				return false, domain.NewError(domain.ErrorPrecondition, "cleanup rollback PV", fmt.Sprintf("PV %s phase %s has no ClaimRef", current.Name, current.Status.Phase))
			}
			claim, claimErr := s.client.CoreV1().PersistentVolumeClaims(current.Spec.ClaimRef.Namespace).Get(waitCtx, current.Spec.ClaimRef.Name, metav1.GetOptions{})
			if claimErr == nil {
				if current.Spec.ClaimRef.UID != "" && claim.UID != current.Spec.ClaimRef.UID {
					return false, domain.NewError(domain.ErrorConflict, "cleanup rollback PV", fmt.Sprintf("PV %s ClaimRef UID changed while waiting for release", ref.Name))
				}
				if claim.DeletionTimestamp == nil {
					return false, domain.NewError(domain.ErrorPrecondition, "cleanup rollback PV", fmt.Sprintf("PV %s is still claimed by PVC %s/%s", current.Name, current.Spec.ClaimRef.Namespace, current.Spec.ClaimRef.Name))
				}
			} else if !apierrors.IsNotFound(claimErr) {
				return false, claimErr
			}
			return false, nil
		}); err != nil {
			if domain.CategoryOf(err) == domain.ErrorInternal {
				return domain.WrapError(domain.ErrorKubernetes, "cleanup rollback PV", fmt.Sprintf("wait for PV %s release", ref.Name), err)
			}
			return err
		}
		pv, err = s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "cleanup rollback PV", fmt.Sprintf("read PV %s after release", ref.Name), err)
		}
		if pv.UID != ref.UID || pv.Labels[kube.SessionKey] != sessionID || pv.Labels[kube.ResourceRoleLabel] != expectedRole {
			return domain.NewError(domain.ErrorConflict, "cleanup rollback PV", fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name))
		}
	}
	if pv.Status.Phase != corev1.VolumeReleased && pv.Status.Phase != corev1.VolumeAvailable {
		return domain.NewError(domain.ErrorPrecondition, "cleanup rollback PV", fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase))
	}
	uid := pv.UID
	resourceVersion := pv.ResourceVersion
	if err := s.client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}}); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup rollback PV", fmt.Sprintf("delete PV %s", pv.Name), err)
	}
	return nil
}

func (s *Service) finalizeActivePV(ctx context.Context, sessionID string, ref domain.ObjectReference, policy corev1.PersistentVolumeReclaimPolicy) error {
	if policy == "" {
		return domain.NewError(domain.ErrorPrecondition, "finalize active PV", fmt.Sprintf("PV %s has no recorded reclaim policy", ref.Name))
	}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		role := pv.Labels[kube.ResourceRoleLabel]
		if pv.UID != ref.UID {
			return domain.NewError(domain.ErrorConflict, "finalize active PV", fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name))
		}
		if pv.Labels[kube.SessionKey] == "" && role == "" && pv.Spec.PersistentVolumeReclaimPolicy == policy && pv.Annotations[kube.OriginalPolicyAnnotation] == "" {
			return nil
		}
		if pv.Labels[kube.SessionKey] != sessionID || (role != kube.ResourceRoleActive && role != kube.ResourceRoleSource && role != kube.ResourceRoleRename && role != kube.ResourceRoleDestination) {
			return domain.NewError(domain.ErrorConflict, "finalize active PV", fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name))
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
		return domain.WrapError(domain.ErrorKubernetes, "finalize active PV", fmt.Sprintf("update PV %s", ref.Name), err)
	}
	return nil
}
