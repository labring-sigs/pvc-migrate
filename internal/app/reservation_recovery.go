package app

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// recoverReservationVolume discovers provisioned storage after a checkpoint
// failure. It returns an independent snapshot and never mutates its input.
func recoverReservationVolume(
	ctx context.Context,
	client kubernetes.Interface,
	id string,
	source, destination v1alpha1.ObjectReference,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
	allowUnownedPV bool,
) (v1alpha1.ClusterVolumeReservationStatus, error) {
	result := *checkpoint.DeepCopy()
	conflict := func(message string) (v1alpha1.ClusterVolumeReservationStatus, error) {
		return checkpoint, domain.NewError(domain.ErrorConflict, "reservation recovery", message)
	}

	if destination.Name == "" || destination.Namespace == "" || source.UID == "" || id == "" {
		return checkpoint, domain.NewError(
			domain.ErrorValidation,
			"reservation recovery",
			"workflow, source identity and destination PVC are required",
		)
	}

	if source.Namespace == destination.Namespace && source.Name == destination.Name {
		return conflict("destination PVC aliases the source")
	}

	if checkpoint.DestinationPVC != nil &&
		(checkpoint.DestinationPVC.Name != destination.Name || checkpoint.DestinationPVC.Namespace != destination.Namespace) {
		return conflict("checkpoint destination differs from the planned PVC")
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(destination.Namespace).
		Get(ctx, destination.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return result, nil
	}

	if err != nil {
		return checkpoint, err
	}

	if (destination.UID != "" && destination.UID != pvc.UID) ||
		(checkpoint.DestinationPVC != nil && checkpoint.DestinationPVC.UID != "" && checkpoint.DestinationPVC.UID != pvc.UID) {
		return conflict("destination PVC was replaced")
	}

	knownPVC := destination.UID != "" ||
		(checkpoint.DestinationPVC != nil && checkpoint.DestinationPVC.UID != "")
	if !reservationPVCRecoveryOwned(pvc, id, string(source.UID), knownPVC) {
		// A terminal pass (deletion convergence or finalize cleanup) must
		// always converge. An earlier finalize pass may have already
		// stripped the PVC's ownership metadata; a PVC with no pvc-migrate
		// metadata at all is ours being finalized, not a foreign claim —
		// report it as already reclaimed so the session can close.
		terminal := ctx.Value(workflowDeletionContextKey{}) != nil ||
			ctx.Value(workflowFinalizeContextKey{}) != nil

		unowned := pvc.Labels[kube.ManagedByLabel] == "" &&
			pvc.Labels[kube.SessionKey] == "" &&
			pvc.Labels[kube.ResourceRoleLabel] == "" &&
			pvc.Annotations[kube.SessionKey] == ""
		if terminal && unowned {
			return result, nil
		}

		if !terminal || !unowned {
			return conflict("destination PVC ownership changed")
		}
	}

	ref := kube.PVCReference(pvc)

	result.DestinationPVC = &ref
	if pvc.Spec.VolumeName == "" {
		return result, nil
	}

	pv, policy, err := recoverReservationPV(
		ctx,
		client,
		id,
		ref,
		pvc.Spec.VolumeName,
		checkpoint.DestinationPV,
		checkpoint.DestinationPolicy,
		allowUnownedPV && !checkpoint.Reserved,
	)
	if err != nil {
		return checkpoint, err
	}

	if pv != nil {
		result.DestinationPV = pv
		result.DestinationPolicy = policy
	}

	return result, nil
}

func recoverReservationPV(
	ctx context.Context,
	client kubernetes.Interface,
	id string,
	claim v1alpha1.ObjectReference,
	pvName string,
	expected *v1alpha1.ObjectReference,
	recordedPolicy corev1.PersistentVolumeReclaimPolicy,
	allowUnownedPV bool,
) (*v1alpha1.ObjectReference, corev1.PersistentVolumeReclaimPolicy, error) {
	conflict := func(message string) (*v1alpha1.ObjectReference, corev1.PersistentVolumeReclaimPolicy, error) {
		return nil, recordedPolicy, domain.NewError(
			domain.ErrorConflict,
			"reservation recovery",
			message,
		)
	}

	if expected != nil && expected.Name != pvName {
		return conflict("destination PVC now binds a different PV")
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, pvName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, recordedPolicy, nil
	}

	if err != nil {
		return nil, recordedPolicy, err
	}

	if expected != nil && expected.UID != "" &&
		expected.UID != pv.UID {
		return conflict("destination PV was replaced")
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != claim.Namespace ||
		pv.Spec.ClaimRef.Name != claim.Name || pv.Spec.ClaimRef.UID != claim.UID {
		return conflict(fmt.Sprintf("destination PV %s ClaimRef does not match its PVC", pv.Name))
	}

	if !reservationPVRecoveryOwned(pv, id, expected, recordedPolicy) &&
		(!allowUnownedPV || !uncheckpointedDestinationPVMatches(pv, claim)) {
		return conflict("destination PV ownership changed")
	}

	ref := &v1alpha1.ObjectReference{
		APIVersion:      "v1",
		Kind:            "PersistentVolume",
		Name:            pv.Name,
		UID:             pv.UID,
		ResourceVersion: pv.ResourceVersion,
	}

	policy := corev1.PersistentVolumeReclaimPolicy(pv.Annotations[kube.OriginalPolicyAnnotation])
	if policy == "" {
		policy = pv.Spec.PersistentVolumeReclaimPolicy
	}

	if !validReclaimPolicy(policy) ||
		(recordedPolicy != "" && recordedPolicy != policy) ||
		(pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain && pv.Spec.PersistentVolumeReclaimPolicy != policy) {
		return conflict("destination PV original reclaim policy changed")
	}

	return ref, policy, nil
}

func reservationPVRecoveryOwned(
	pv *corev1.PersistentVolume,
	id string,
	expected *v1alpha1.ObjectReference,
	recordedPolicy corev1.PersistentVolumeReclaimPolicy,
) bool {
	if pv.Labels[kube.ManagedByLabel] == kube.ManagedByValue &&
		pv.Labels[kube.SessionKey] == id &&
		pv.Labels[kube.ResourceRoleLabel] == kube.ResourceRoleDestination {
		return true
	}

	// A known PV can have already been finalized by an earlier cleanup attempt.
	return expected != nil && expected.UID == pv.UID &&
		pv.Labels[kube.SessionKey] == "" && pv.Labels[kube.ResourceRoleLabel] == "" &&
		pv.Annotations[kube.SessionKey] == "" && pv.Annotations[kube.OriginalPolicyAnnotation] == "" &&
		validReclaimPolicy(
			recordedPolicy,
		) && pv.Spec.PersistentVolumeReclaimPolicy == recordedPolicy
}

func reservationPVCRecoveryOwned(
	pvc *corev1.PersistentVolumeClaim,
	id, sourceUID string,
	known bool,
) bool {
	if known {
		// Finalization can remove ownership metadata after the UID is saved.
		// Explicit foreign ownership and source identity still conflict.
		return (pvc.Labels[kube.SessionKey] == "" || pvc.Labels[kube.SessionKey] == id) &&
			(pvc.Annotations[kube.SessionKey] == "" || pvc.Annotations[kube.SessionKey] == id) &&
			(pvc.Labels[kube.ResourceRoleLabel] == "" || pvc.Labels[kube.ResourceRoleLabel] == kube.ResourceRoleDestination) &&
			(pvc.Annotations[kube.SourcePVCUIDAnnotation] == "" || pvc.Annotations[kube.SourcePVCUIDAnnotation] == sourceUID)
	}

	return pvc.Labels[kube.ManagedByLabel] == kube.ManagedByValue &&
		pvc.Labels[kube.SessionKey] == id &&
		pvc.Labels[kube.ResourceRoleLabel] == kube.ResourceRoleDestination &&
		pvc.Annotations[kube.SessionKey] == id &&
		pvc.Annotations[kube.SourcePVCUIDAnnotation] == sourceUID
}
