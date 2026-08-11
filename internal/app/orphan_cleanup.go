package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

type OrphanCleanupOptions struct {
	SessionID        string
	SessionNamespace string
	SourceNamespace  string
	SourcePVC        string
}

// PlanOrphanCleanup reconstructs the exact active/rollback relationship from
// Kubernetes metadata after the durable session ConfigMap was lost.
func (s *Service) PlanOrphanCleanup(ctx context.Context, options OrphanCleanupOptions) (*domain.OrphanCleanupPlan, error) {
	plan := &domain.OrphanCleanupPlan{
		APIVersion:       domain.SessionAPIVersion,
		Kind:             domain.OrphanCleanupPlanKind,
		SessionID:        options.SessionID,
		SessionNamespace: options.SessionNamespace,
		Ready:            true,
	}
	if problems := validation.IsDNS1123Label(options.SessionID); len(problems) > 0 {
		plan.AddCheck(orphanFailed("session-id", strings.Join(problems, "; ")))
	}
	if options.SessionNamespace == "" || options.SourceNamespace == "" || options.SourcePVC == "" {
		plan.AddCheck(orphanFailed("identity", "session namespace, source namespace, and source PVC are required"))
	}
	if !plan.Ready {
		return plan, nil
	}

	_, err := s.client.CoreV1().ConfigMaps(options.SessionNamespace).Get(ctx, kube.SessionConfigMapName(options.SessionID), metav1.GetOptions{})
	switch {
	case err == nil:
		plan.AddCheck(orphanFailed("session-configmap", fmt.Sprintf("session ConfigMap %s/%s still exists; use `session cleanup` after reading its status", options.SessionNamespace, kube.SessionConfigMapName(options.SessionID))))
	case !apierrors.IsNotFound(err):
		plan.AddCheck(orphanFailed("session-configmap", fmt.Sprintf("read session ConfigMap: %v", err)))
	default:
		plan.AddCheck(orphanPassed("session-configmap", "session ConfigMap is absent; orphan ownership recovery is required"))
	}
	pvc, err := s.client.CoreV1().PersistentVolumeClaims(options.SourceNamespace).Get(ctx, options.SourcePVC, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		plan.AddCheck(orphanFailed("source-pvc", fmt.Sprintf("source PVC %s/%s does not exist", options.SourceNamespace, options.SourcePVC)))
		return plan, nil
	}
	if err != nil {
		plan.AddCheck(orphanFailed("source-pvc", fmt.Sprintf("read source PVC: %v", err)))
		return plan, nil
	}
	plan.SourcePVC = pvcObjectReference(pvc)
	ownershipMarkers := 0
	for _, owner := range []struct {
		name  string
		value string
	}{
		{name: "PVC annotation", value: pvc.Annotations[kube.SessionKey]},
		{name: "PVC label", value: pvc.Labels[kube.SessionKey]},
	} {
		if owner.value == options.SessionID {
			ownershipMarkers++
			continue
		}
		if owner.value != "" {
			plan.AddCheck(orphanFailed("source-ownership", fmt.Sprintf("source PVC %s/%s %s belongs to session %s", pvc.Namespace, pvc.Name, owner.name, owner.value)))
		}
	}
	if ownershipMarkers > 0 && pvc.Labels[kube.SessionKey] == "" {
		plan.AddCheck(orphanWarning("source-ownership", fmt.Sprintf("source PVC %s/%s has annotation ownership for session %s; its session label is absent", pvc.Namespace, pvc.Name, options.SessionID)))
	}
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		plan.AddCheck(orphanFailed("source-pvc", fmt.Sprintf("source PVC %s/%s must be Bound with a volumeName", pvc.Namespace, pvc.Name)))
		return plan, nil
	}
	active, err := s.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		plan.AddCheck(orphanFailed("active-pv", fmt.Sprintf("read active PV %s: %v", pvc.Spec.VolumeName, err)))
		return plan, nil
	}
	plan.ActivePV = pvObjectReference(active)
	if active.Labels[kube.SessionKey] != options.SessionID || active.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleActive {
		plan.AddCheck(orphanFailed("active-ownership", fmt.Sprintf("active PV %s must carry session=%s and role=active", active.Name, options.SessionID)))
	}
	if pvc.UID == "" || active.Spec.ClaimRef == nil || active.Spec.ClaimRef.Namespace != pvc.Namespace || active.Spec.ClaimRef.Name != pvc.Name || active.Spec.ClaimRef.UID == "" || active.Spec.ClaimRef.UID != pvc.UID {
		plan.AddCheck(orphanFailed("active-claim", fmt.Sprintf("active PV %s claimRef does not match PVC %s/%s UID %s", active.Name, pvc.Namespace, pvc.Name, pvc.UID)))
	}
	originalPolicy := corev1.PersistentVolumeReclaimPolicy(active.Annotations[kube.OriginalPolicyAnnotation])
	if !validReclaimPolicy(originalPolicy) {
		plan.AddCheck(orphanFailed("active-policy", fmt.Sprintf("active PV %s has no valid %s annotation", active.Name, kube.OriginalPolicyAnnotation)))
	}
	rollbackName := pvc.Annotations[kube.RollbackPVAnnotation]
	if rollbackName == "" {
		rollbackName = active.Annotations[kube.PairedPVAnnotation]
		if rollbackName == "" {
			plan.AddCheck(orphanFailed("rollback-pv", "source PVC and active PV have no rollback PV reference"))
			return plan, nil
		}
		plan.AddCheck(orphanWarning("rollback-pv", fmt.Sprintf("source PVC metadata is already finalized; active PV records rollback PV %s", rollbackName)))
	}
	if active.Annotations[kube.PairedPVAnnotation] != rollbackName {
		plan.AddCheck(orphanFailed("pv-pair", fmt.Sprintf("active PV %s does not point to rollback PV %s", active.Name, rollbackName)))
	}
	rollback, err := s.client.CoreV1().PersistentVolumes().Get(ctx, rollbackName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		plan.RollbackPV = domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolume, Name: rollbackName}
		plan.AddCheck(orphanWarning("rollback-pv", fmt.Sprintf("rollback PV %s is already absent; deletion will remain idempotent", rollbackName)))
	case err != nil:
		plan.AddCheck(orphanFailed("rollback-pv", fmt.Sprintf("read rollback PV %s: %v", rollbackName, err)))
		return plan, nil
	default:
		plan.RollbackPV = pvObjectReference(rollback)
		if rollback.Labels[kube.SessionKey] != options.SessionID || rollback.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleRollback {
			plan.AddCheck(orphanFailed("rollback-ownership", fmt.Sprintf("rollback PV %s must carry session=%s and role=rollback", rollback.Name, options.SessionID)))
		}
		if rollback.Annotations[kube.PairedPVAnnotation] != active.Name {
			plan.AddCheck(orphanFailed("pv-pair", fmt.Sprintf("rollback PV %s does not point back to active PV %s", rollback.Name, active.Name)))
		}
		if rollback.Status.Phase != corev1.VolumeReleased && rollback.Status.Phase != corev1.VolumeAvailable {
			plan.AddCheck(orphanFailed("rollback-state", fmt.Sprintf("rollback PV %s phase %s must be Released or Available", rollback.Name, rollback.Status.Phase)))
		}
		if rollback.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			plan.AddCheck(orphanFailed("rollback-policy", fmt.Sprintf("rollback PV %s reclaim policy must be Retain before deletion", rollback.Name)))
		}
	}
	if ownershipMarkers == 0 {
		if active.Labels[kube.SessionKey] == options.SessionID && active.Labels[kube.ResourceRoleLabel] == kube.ResourceRoleActive {
			plan.AddCheck(orphanWarning("source-ownership", fmt.Sprintf("source PVC %s/%s metadata is already finalized; active PV ownership still proves session %s", pvc.Namespace, pvc.Name, options.SessionID)))
		} else {
			plan.AddCheck(orphanFailed("source-ownership", fmt.Sprintf("source PVC %s/%s has no ownership marker for orphan session %s", pvc.Namespace, pvc.Name, options.SessionID)))
		}
	}
	if plan.Ready {
		plan.AddCheck(orphanPassed("resources", fmt.Sprintf("validated active PVC %s/%s, active PV %s, and rollback PV %s", pvc.Namespace, pvc.Name, active.Name, rollbackName)))
	}
	return plan, nil
}

// CleanupOrphan performs the validated metadata cleanup and removes the
// session lease. It never deletes the active PVC or active PV.
func (s *Service) CleanupOrphan(ctx context.Context, options OrphanCleanupOptions) (*domain.OrphanCleanupPlan, error) {
	var result *domain.OrphanCleanupPlan
	err := s.withSessionIDLock(ctx, options.SessionNamespace, options.SessionID, func(lockedCtx context.Context) error {
		plan, err := s.PlanOrphanCleanup(lockedCtx, options)
		if err != nil {
			return err
		}
		result = plan
		if !plan.Ready {
			return domain.NewError(domain.ErrorPrecondition, "cleanup orphan", "orphan cleanup plan contains failed checks")
		}
		if err := s.deleteOrphanRollbackPV(lockedCtx, options.SessionID, plan.RollbackPV); err != nil {
			return err
		}
		if err := s.finalizeOrphanPVC(lockedCtx, options.SessionID, plan.SourcePVC); err != nil {
			return err
		}
		if err := s.finalizeOrphanPV(lockedCtx, options.SessionID, plan.ActivePV); err != nil {
			return err
		}
		if cleaner, ok := s.store.(kube.SessionLeaseCleaner); ok {
			if err := cleaner.DeleteSessionLease(lockedCtx, options.SessionNamespace, options.SessionID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, nil
}

func pvcObjectReference(pvc *corev1.PersistentVolumeClaim) domain.ObjectReference {
	return domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolumeClaim, Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID, ResourceVersion: pvc.ResourceVersion}
}

func pvObjectReference(pv *corev1.PersistentVolume) domain.ObjectReference {
	return domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolume, Name: pv.Name, UID: pv.UID, ResourceVersion: pv.ResourceVersion}
}

func orphanPassed(name, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityInfo, Passed: true, Message: message}
}

func orphanFailed(name, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityError, Passed: false, Message: message}
}

func orphanWarning(name, message string) domain.Check {
	return domain.Check{Name: name, Severity: domain.SeverityWarning, Passed: true, Message: message}
}

func validReclaimPolicy(policy corev1.PersistentVolumeReclaimPolicy) bool {
	return policy == corev1.PersistentVolumeReclaimDelete || policy == corev1.PersistentVolumeReclaimRetain || policy == corev1.PersistentVolumeReclaimRecycle
}

func (s *Service) deleteOrphanRollbackPV(ctx context.Context, sessionID string, ref domain.ObjectReference) error {
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("read rollback PV %s", ref.Name), err)
	}
	if pv.UID != ref.UID || pv.ResourceVersion != ref.ResourceVersion || pv.Labels[kube.SessionKey] != sessionID || pv.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleRollback || (pv.Status.Phase != corev1.VolumeReleased && pv.Status.Phase != corev1.VolumeAvailable) {
		return domain.NewError(domain.ErrorConflict, "cleanup orphan", fmt.Sprintf("rollback PV %s identity, ownership, or released state changed", ref.Name))
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		return domain.NewError(domain.ErrorPrecondition, "cleanup orphan", fmt.Sprintf("rollback PV %s reclaim policy must remain Retain before deletion", ref.Name))
	}
	uid, resourceVersion := pv.UID, pv.ResourceVersion
	if err := s.client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}}); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("delete rollback PV %s", pv.Name), err)
	}
	return nil
}

func (s *Service) finalizeOrphanPVC(ctx context.Context, sessionID string, ref domain.ObjectReference) error {
	pvc, err := s.client.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("read source PVC %s/%s", ref.Namespace, ref.Name), err)
	}
	annotationOwner := pvc.Annotations[kube.SessionKey]
	labelOwner := pvc.Labels[kube.SessionKey]
	owned := annotationOwner == sessionID || labelOwner == sessionID
	foreign := (annotationOwner != "" && annotationOwner != sessionID) || (labelOwner != "" && labelOwner != sessionID)
	if pvc.UID != ref.UID || pvc.ResourceVersion != ref.ResourceVersion || foreign {
		return domain.NewError(domain.ErrorConflict, "cleanup orphan", fmt.Sprintf("source PVC %s/%s identity or ownership changed", ref.Namespace, ref.Name))
	}
	if !owned {
		if pvc.Labels[kube.ManagedByLabel] == "" && pvc.Labels[kube.ResourceRoleLabel] == "" && pvc.Annotations[kube.SessionKey] == "" && pvc.Annotations[kube.RollbackPVAnnotation] == "" {
			return nil
		}
		return domain.NewError(domain.ErrorConflict, "cleanup orphan", fmt.Sprintf("source PVC %s/%s has unexpected metadata after ownership cleanup", ref.Namespace, ref.Name))
	}
	if pvc.Labels[kube.ManagedByLabel] == kube.ManagedByValue {
		delete(pvc.Labels, kube.ManagedByLabel)
	}
	delete(pvc.Labels, kube.SessionKey)
	delete(pvc.Labels, kube.ResourceRoleLabel)
	delete(pvc.Annotations, kube.SessionKey)
	delete(pvc.Annotations, kube.RollbackPVAnnotation)
	delete(pvc.Annotations, kube.SourcePVAnnotation)
	delete(pvc.Annotations, kube.SourcePVCUIDAnnotation)
	_, err = s.client.CoreV1().PersistentVolumeClaims(ref.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("finalize source PVC %s/%s", ref.Namespace, ref.Name), err)
	}
	return nil
}

func (s *Service) finalizeOrphanPV(ctx context.Context, sessionID string, ref domain.ObjectReference) error {
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("read active PV %s", ref.Name), err)
	}
	if pv.UID != ref.UID || pv.ResourceVersion != ref.ResourceVersion || pv.Labels[kube.SessionKey] != sessionID || pv.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleActive {
		return domain.NewError(domain.ErrorConflict, "cleanup orphan", fmt.Sprintf("active PV %s identity or ownership changed", ref.Name))
	}
	policy := corev1.PersistentVolumeReclaimPolicy(pv.Annotations[kube.OriginalPolicyAnnotation])
	if !validReclaimPolicy(policy) {
		return domain.NewError(domain.ErrorPrecondition, "cleanup orphan", fmt.Sprintf("active PV %s has no valid original reclaim policy", ref.Name))
	}
	pv.Spec.PersistentVolumeReclaimPolicy = policy
	delete(pv.Labels, kube.ManagedByLabel)
	delete(pv.Labels, kube.SessionKey)
	delete(pv.Labels, kube.ResourceRoleLabel)
	delete(pv.Annotations, kube.OriginalPolicyAnnotation)
	delete(pv.Annotations, kube.PairedPVAnnotation)
	_, err = s.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("finalize active PV %s", ref.Name), err)
	}
	return nil
}
