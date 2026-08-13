package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

type OrphanCleanupOptions struct {
	SessionID        string
	SessionNamespace string
	SourceNamespace  string
	SourcePVC        string
}

// PlanOrphanCleanup reconstructs either a pre-activation source/destination
// relationship or a post-activation active/rollback relationship after the
// durable session ConfigMap was lost.
func (s *Service) PlanOrphanCleanup(ctx context.Context, options OrphanCleanupOptions) (*domain.OrphanCleanupPlan, error) {
	s.logInfo("orphan cleanup planning started", "session", options.SessionID, "namespace", options.SessionNamespace, "source", options.SourceNamespace+"/"+options.SourcePVC)
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
	current, err := s.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		plan.AddCheck(orphanFailed("current-pv", fmt.Sprintf("read current PV %s: %v", pvc.Spec.VolumeName, err)))
		return plan, nil
	}
	if current.Labels[kube.SessionKey] != options.SessionID {
		plan.AddCheck(orphanFailed("current-ownership", fmt.Sprintf("current PV %s must carry session=%s", current.Name, options.SessionID)))
		return plan, nil
	}
	if pvc.UID == "" || current.Spec.ClaimRef == nil || current.Spec.ClaimRef.Namespace != pvc.Namespace || current.Spec.ClaimRef.Name != pvc.Name || current.Spec.ClaimRef.UID == "" || current.Spec.ClaimRef.UID != pvc.UID {
		plan.AddCheck(orphanFailed("current-claim", fmt.Sprintf("current PV %s claimRef does not match PVC %s/%s UID %s", current.Name, pvc.Namespace, pvc.Name, pvc.UID)))
	}
	originalPolicy := corev1.PersistentVolumeReclaimPolicy(current.Annotations[kube.OriginalPolicyAnnotation])
	if !validReclaimPolicy(originalPolicy) {
		plan.AddCheck(orphanFailed("current-policy", fmt.Sprintf("current PV %s has no valid %s annotation", current.Name, kube.OriginalPolicyAnnotation)))
	}
	if current.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		plan.AddCheck(orphanFailed("current-policy", fmt.Sprintf("current PV %s reclaim policy must be Retain during orphan cleanup", current.Name)))
	}
	if ownershipMarkers == 0 {
		plan.AddCheck(orphanWarning("source-ownership", fmt.Sprintf("source PVC %s/%s metadata is already finalized; current PV ownership still proves session %s", pvc.Namespace, pvc.Name, options.SessionID)))
	}

	switch current.Labels[kube.ResourceRoleLabel] {
	case kube.ResourceRoleSource:
		return s.planPreActivationOrphan(ctx, plan, options, pvc, current)
	case kube.ResourceRoleActive:
		return s.planPostActivationOrphan(ctx, plan, options, pvc, current, ownershipMarkers)
	default:
		plan.AddCheck(orphanFailed("current-ownership", fmt.Sprintf("current PV %s has unsupported orphan role %q", current.Name, current.Labels[kube.ResourceRoleLabel])))
		return plan, nil
	}
}

func (s *Service) planPostActivationOrphan(ctx context.Context, plan *domain.OrphanCleanupPlan, options OrphanCleanupOptions, pvc *corev1.PersistentVolumeClaim, active *corev1.PersistentVolume, ownershipMarkers int) (*domain.OrphanCleanupPlan, error) {
	plan.Mode = domain.OrphanCleanupPostActivation
	resources := &domain.OrphanPostActivationCleanup{SourcePVC: pvcObjectReference(pvc), ActivePV: pvObjectReference(active)}
	plan.PostActivation = resources
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
		resources.RollbackPV = domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolume, Name: rollbackName}
		plan.AddCheck(orphanWarning("rollback-pv", fmt.Sprintf("rollback PV %s is already absent; deletion will remain idempotent", rollbackName)))
	case err != nil:
		plan.AddCheck(orphanFailed("rollback-pv", fmt.Sprintf("read rollback PV %s: %v", rollbackName, err)))
		return plan, nil
	default:
		resources.RollbackPV = pvObjectReference(rollback)
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
			// The shared planner already recorded this resumable checkpoint.
		} else {
			plan.AddCheck(orphanFailed("source-ownership", fmt.Sprintf("source PVC %s/%s has no ownership marker for orphan session %s", pvc.Namespace, pvc.Name, options.SessionID)))
		}
	}
	if plan.Ready {
		plan.AddCheck(orphanPassed("resources", fmt.Sprintf("validated active PVC %s/%s, active PV %s, and rollback PV %s", pvc.Namespace, pvc.Name, active.Name, rollbackName)))
	}
	return plan, nil
}

func (s *Service) planPreActivationOrphan(ctx context.Context, plan *domain.OrphanCleanupPlan, options OrphanCleanupOptions, pvc *corev1.PersistentVolumeClaim, sourcePV *corev1.PersistentVolume) (*domain.OrphanCleanupPlan, error) {
	plan.Mode = domain.OrphanCleanupPreActivation
	resources := &domain.OrphanPreActivationCleanup{SourcePVC: pvcObjectReference(pvc), SourcePV: pvObjectReference(sourcePV)}
	plan.PreActivation = resources
	if pvc.Annotations[kube.RollbackPVAnnotation] != "" || sourcePV.Annotations[kube.PairedPVAnnotation] != "" {
		plan.AddCheck(orphanFailed("activation-state", "source-role resources contain rollback pairing metadata; activation state is ambiguous"))
		return plan, nil
	}

	destinationSelector := fmt.Sprintf("%s=%s,%s=%s,%s=%s", kube.ManagedByLabel, kube.ManagedByValue, kube.SessionKey, options.SessionID, kube.ResourceRoleLabel, kube.ResourceRoleDestination)
	sourceSelector := fmt.Sprintf("%s=%s,%s=%s,%s=%s", kube.ManagedByLabel, kube.ManagedByValue, kube.SessionKey, options.SessionID, kube.ResourceRoleLabel, kube.ResourceRoleSource)
	sourcePVs, err := s.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{LabelSelector: sourceSelector})
	if err != nil {
		plan.AddCheck(orphanFailed("source-pv", fmt.Sprintf("list source PVs: %v", err)))
		return plan, nil
	}
	destinationPVCs, err := s.client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{LabelSelector: destinationSelector})
	if err != nil {
		plan.AddCheck(orphanFailed("destination-pvc", fmt.Sprintf("list destination PVCs: %v", err)))
		return plan, nil
	}
	matchingPVCs := make([]corev1.PersistentVolumeClaim, 0, 1)
	pvToPVC := map[string]struct{}{}
	for i := range destinationPVCs.Items {
		candidate := &destinationPVCs.Items[i]
		if candidate.Spec.VolumeName != "" {
			pvToPVC[candidate.Spec.VolumeName] = struct{}{}
		}
		if candidate.Annotations[kube.SourcePVCUIDAnnotation] == string(pvc.UID) {
			matchingPVCs = append(matchingPVCs, *candidate)
		}
	}
	if len(matchingPVCs) > 1 {
		plan.AddCheck(orphanFailed("destination-pvc", fmt.Sprintf("found %d destination PVCs for source PVC UID %s", len(matchingPVCs), pvc.UID)))
		return plan, nil
	}
	if len(matchingPVCs) == 1 {
		destination := &matchingPVCs[0]
		resources.DestinationPVC = pvcObjectReference(destination)
		if destination.Annotations[kube.SessionKey] != options.SessionID || destination.UID == "" {
			plan.AddCheck(orphanFailed("destination-ownership", fmt.Sprintf("destination PVC %s/%s ownership or UID is incomplete", destination.Namespace, destination.Name)))
		}
	}

	destinationPVs, err := s.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{LabelSelector: destinationSelector})
	if err != nil {
		plan.AddCheck(orphanFailed("destination-pv", fmt.Sprintf("list destination PVs: %v", err)))
		return plan, nil
	}
	var destinationPV *corev1.PersistentVolume
	if resources.DestinationPVC.Name != "" {
		claim, getErr := s.client.CoreV1().PersistentVolumeClaims(resources.DestinationPVC.Namespace).Get(ctx, resources.DestinationPVC.Name, metav1.GetOptions{})
		if getErr == nil && claim.Spec.VolumeName != "" {
			destinationPV, getErr = s.client.CoreV1().PersistentVolumes().Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				plan.AddCheck(orphanWarning("destination-pv", fmt.Sprintf("destination PV %s is already absent", claim.Spec.VolumeName)))
				destinationPV = nil
			} else if getErr != nil {
				plan.AddCheck(orphanFailed("destination-pv", fmt.Sprintf("read destination PV %s: %v", claim.Spec.VolumeName, getErr)))
			}
		} else if getErr != nil && !apierrors.IsNotFound(getErr) {
			plan.AddCheck(orphanFailed("destination-pvc", fmt.Sprintf("read destination PVC %s/%s: %v", resources.DestinationPVC.Namespace, resources.DestinationPVC.Name, getErr)))
		}
	}
	if resources.DestinationPVC.Name == "" {
		orphanPVs := make([]corev1.PersistentVolume, 0, len(destinationPVs.Items))
		for i := range destinationPVs.Items {
			if _, referenced := pvToPVC[destinationPVs.Items[i].Name]; !referenced {
				orphanPVs = append(orphanPVs, destinationPVs.Items[i])
			}
		}
		switch {
		case len(orphanPVs) == 1 && len(sourcePVs.Items) == 1:
			destinationPV = &orphanPVs[0]
			if destinationPV.Spec.ClaimRef != nil {
				resources.DestinationPVC = domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolumeClaim, Namespace: destinationPV.Spec.ClaimRef.Namespace, Name: destinationPV.Spec.ClaimRef.Name, UID: destinationPV.Spec.ClaimRef.UID}
			}
			plan.AddCheck(orphanWarning("destination-pvc", "destination PVC is already absent; its retained PV will be removed"))
		case len(orphanPVs) > 0:
			plan.AddCheck(orphanFailed("destination-pv", fmt.Sprintf("%d unclaimed destination PVs cannot be mapped safely to source PVC %s/%s", len(orphanPVs), pvc.Namespace, pvc.Name)))
		}
	}
	if destinationPV != nil {
		resources.DestinationPV = pvObjectReference(destinationPV)
		if destinationPV.Labels[kube.ManagedByLabel] != kube.ManagedByValue || destinationPV.Labels[kube.SessionKey] != options.SessionID || destinationPV.Labels[kube.ResourceRoleLabel] != kube.ResourceRoleDestination {
			plan.AddCheck(orphanFailed("destination-ownership", fmt.Sprintf("destination PV %s ownership changed", destinationPV.Name)))
		}
		if destinationPV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			plan.AddCheck(orphanFailed("destination-policy", fmt.Sprintf("destination PV %s reclaim policy must be Retain before deletion", destinationPV.Name)))
		}
		if resources.DestinationPVC.UID != "" && (destinationPV.Spec.ClaimRef == nil || destinationPV.Spec.ClaimRef.Namespace != resources.DestinationPVC.Namespace || destinationPV.Spec.ClaimRef.Name != resources.DestinationPVC.Name || destinationPV.Spec.ClaimRef.UID != resources.DestinationPVC.UID) {
			plan.AddCheck(orphanFailed("destination-claim", fmt.Sprintf("destination PV %s claimRef does not match destination PVC identity", destinationPV.Name)))
		}
	}
	if resources.DestinationPVC.Name != "" {
		if err := s.ensurePVCUnused(ctx, resources.DestinationPVC, options.SessionID); err != nil {
			plan.AddCheck(orphanFailed("destination-consumers", err.Error()))
		}
	}
	if len(destinationPVCs.Items) > len(matchingPVCs) {
		if len(sourcePVs.Items) == 1 {
			plan.AddCheck(orphanFailed("other-resources", "single-volume orphan ownership includes an unrelated destination PVC"))
		} else {
			plan.AddCheck(orphanWarning("other-resources", "the orphan session owns additional destination PVCs; clean each source PVC before the final Lease is removed"))
		}
	}
	if plan.Ready {
		plan.AddCheck(orphanPassed("resources", fmt.Sprintf("validated pre-activation source PVC %s/%s, source PV %s, destination PVC %s/%s, and destination PV %s", pvc.Namespace, pvc.Name, sourcePV.Name, resources.DestinationPVC.Namespace, resources.DestinationPVC.Name, resources.DestinationPV.Name)))
	}
	return plan, nil
}

// CleanupOrphan performs the validated metadata cleanup and removes the
// session lease. It never deletes the active PVC or active PV.
func (s *Service) CleanupOrphan(ctx context.Context, options OrphanCleanupOptions) (*domain.OrphanCleanupPlan, error) {
	s.logInfo("orphan cleanup started", "session", options.SessionID, "namespace", options.SessionNamespace)
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
		switch plan.Mode {
		case domain.OrphanCleanupPreActivation:
			if err := s.cleanupPreActivationOrphan(lockedCtx, options.SessionID, plan.PreActivation); err != nil {
				return err
			}
		case domain.OrphanCleanupPostActivation:
			if err := s.cleanupPostActivationOrphan(lockedCtx, options.SessionID, plan.PostActivation); err != nil {
				return err
			}
		default:
			return domain.NewError(domain.ErrorInternal, "cleanup orphan", fmt.Sprintf("unsupported orphan cleanup mode %q", plan.Mode))
		}
		remaining, err := s.hasOrphanSessionResources(lockedCtx, options.SessionID)
		if err != nil {
			return err
		}
		if !remaining {
			if held, ok := lockedCtx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
				deleteCtx, cancelDelete := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancelDelete()
				if err := held.lock.Delete(deleteCtx); err != nil {
					return err
				}
			} else if cleaner, ok := s.store.(kube.SessionLeaseCleaner); ok {
				if err := cleaner.DeleteSessionLease(lockedCtx, options.SessionNamespace, options.SessionID); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, nil
}

func (s *Service) cleanupPostActivationOrphan(ctx context.Context, sessionID string, resources *domain.OrphanPostActivationCleanup) error {
	if resources == nil {
		return domain.NewError(domain.ErrorInternal, "cleanup orphan", "post-activation resources are missing")
	}
	if err := s.deleteOrphanRollbackPV(ctx, sessionID, resources.RollbackPV); err != nil {
		return err
	}
	if err := s.finalizeOrphanPVC(ctx, sessionID, resources.SourcePVC); err != nil {
		return err
	}
	return s.finalizeOrphanPV(ctx, sessionID, resources.ActivePV, kube.ResourceRoleActive)
}

func (s *Service) cleanupPreActivationOrphan(ctx context.Context, sessionID string, resources *domain.OrphanPreActivationCleanup) error {
	if resources == nil {
		return domain.NewError(domain.ErrorInternal, "cleanup orphan", "pre-activation resources are missing")
	}
	if resources.DestinationPVC.Namespace != "" {
		session := &domain.Session{
			ID: sessionID,
			Spec: domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
				TemporaryNamespace: resources.DestinationPVC.Namespace,
				SessionNamespace:   resources.DestinationPVC.Namespace,
				Volumes:            []domain.VolumeSpec{{DestinationPVC: resources.DestinationPVC}},
			}, domain.WorkloadSpec{Adapter: domain.WorkloadNone}, false),
		}
		if err := s.deleteReservationPods(ctx, session); err != nil {
			return err
		}
		if err := s.ensurePVCUnused(ctx, resources.DestinationPVC, sessionID); err != nil {
			return err
		}
		if err := s.deleteOrphanDestinationPVC(ctx, sessionID, resources.SourcePVC.UID, resources.DestinationPVC); err != nil {
			return err
		}
	}
	if resources.DestinationPV.Name != "" {
		if err := s.deleteRollbackPV(ctx, sessionID, resources.DestinationPV, kube.ResourceRoleDestination, nil); err != nil {
			return err
		}
	}
	if err := s.finalizeOrphanPVC(ctx, sessionID, resources.SourcePVC); err != nil {
		return err
	}
	return s.finalizeOrphanPV(ctx, sessionID, resources.SourcePV, kube.ResourceRoleSource)
}

func (s *Service) deleteOrphanDestinationPVC(ctx context.Context, sessionID string, sourcePVCUID types.UID, ref domain.ObjectReference) error {
	pvc, err := s.client.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("read destination PVC %s/%s", ref.Namespace, ref.Name), err)
	}
	owned := pvc.Labels[kube.ManagedByLabel] == kube.ManagedByValue && pvc.Labels[kube.SessionKey] == sessionID && pvc.Labels[kube.ResourceRoleLabel] == kube.ResourceRoleDestination && pvc.Annotations[kube.SessionKey] == sessionID && pvc.Annotations[kube.SourcePVCUIDAnnotation] == string(sourcePVCUID)
	if pvc.UID != ref.UID || pvc.ResourceVersion != ref.ResourceVersion || !owned {
		return domain.NewError(domain.ErrorConflict, "cleanup orphan", fmt.Sprintf("destination PVC %s/%s identity or ownership changed", ref.Namespace, ref.Name))
	}
	uid, resourceVersion := pvc.UID, pvc.ResourceVersion
	if err := s.client.CoreV1().PersistentVolumeClaims(ref.Namespace).Delete(ctx, ref.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}}); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("delete destination PVC %s/%s", ref.Namespace, ref.Name), err)
	}
	return nil
}

func (s *Service) hasOrphanSessionResources(ctx context.Context, sessionID string) (bool, error) {
	pvcs, err := s.client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", "list remaining PVC ownership", err)
	}
	for i := range pvcs.Items {
		if pvcs.Items[i].Labels[kube.SessionKey] == sessionID || pvcs.Items[i].Annotations[kube.SessionKey] == sessionID {
			return true, nil
		}
	}
	selector := kube.SessionKey + "=" + sessionID
	pvs, err := s.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", "list remaining PV ownership", err)
	}
	if len(pvs.Items) > 0 {
		return true, nil
	}
	pods, err := s.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", "list remaining Pod ownership", err)
	}
	return len(pods.Items) > 0, nil
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

func (s *Service) finalizeOrphanPV(ctx context.Context, sessionID string, ref domain.ObjectReference, expectedRole string) error {
	role := orphanPVRoleName(expectedRole)
	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("read %s PV %s", role, ref.Name), err)
	}
	if pv.UID != ref.UID || pv.ResourceVersion != ref.ResourceVersion || pv.Labels[kube.SessionKey] != sessionID || pv.Labels[kube.ResourceRoleLabel] != expectedRole {
		return domain.NewError(domain.ErrorConflict, "cleanup orphan", fmt.Sprintf("%s PV %s identity or ownership changed", role, ref.Name))
	}
	policy := corev1.PersistentVolumeReclaimPolicy(pv.Annotations[kube.OriginalPolicyAnnotation])
	if !validReclaimPolicy(policy) {
		return domain.NewError(domain.ErrorPrecondition, "cleanup orphan", fmt.Sprintf("%s PV %s has no valid original reclaim policy", role, ref.Name))
	}
	pv.Spec.PersistentVolumeReclaimPolicy = policy
	delete(pv.Labels, kube.ManagedByLabel)
	delete(pv.Labels, kube.SessionKey)
	delete(pv.Labels, kube.ResourceRoleLabel)
	delete(pv.Annotations, kube.OriginalPolicyAnnotation)
	delete(pv.Annotations, kube.PairedPVAnnotation)
	_, err = s.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "cleanup orphan", fmt.Sprintf("finalize %s PV %s", role, ref.Name), err)
	}
	return nil
}

func orphanPVRoleName(role string) string {
	switch role {
	case kube.ResourceRoleSource:
		return "source"
	case kube.ResourceRoleDestination:
		return "destination"
	case kube.ResourceRoleActive:
		return "active"
	default:
		return "orphan"
	}
}
