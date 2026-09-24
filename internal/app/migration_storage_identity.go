package app

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func verifySourceStorage(
	ctx context.Context,
	client kubernetes.Interface,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
) error {
	if sourcePVC.Namespace == "" || sourcePVC.Name == "" ||
		sourcePVC.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifySourceStoragePhase,
			"source PVC reference is incomplete",
		)
	}

	if sourcePV.Name == "" || sourcePV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifySourceStoragePhase,
			fmt.Sprintf(
				"source PV reference for PVC %s/%s is incomplete",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
		)
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(sourcePVC.Namespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifySourceStoragePhase,
			fmt.Sprintf(
				"read source PVC %s/%s",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifySourceStoragePhase,
			fmt.Sprintf(
				"read source PVC %s/%s returned an empty object",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
		)
	}

	if pvc.UID != sourcePVC.UID || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != sourcePV.Name {
		return domain.NewError(
			domain.ErrorConflict,
			verifySourceStoragePhase,
			fmt.Sprintf(
				"source PVC %s/%s identity or binding changed",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, sourcePV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifySourceStoragePhase,
			"read source PV "+sourcePV.Name,
			err,
		)
	}

	if pv == nil || pv.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifySourceStoragePhase,
			fmt.Sprintf("read source PV %s returned an empty object", sourcePV.Name),
		)
	}

	if pv.UID != sourcePV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			verifySourceStoragePhase,
			fmt.Sprintf("source PV %s identity or claimRef changed", pv.Name),
		)
	}

	return nil
}

func verifyRollbackStorageVolume(
	ctx context.Context,
	client kubernetes.Interface,
	sessionID string, sourcePVC, expectedPV v1alpha1.ObjectReference,
	active *v1alpha1.ObjectReference,
) error {
	if active == nil || active.Namespace == "" || active.Name == "" || active.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifyRollbackPhase,
			fmt.Sprintf(
				"PVC %s/%s has no recorded restored identity",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
		)
	}

	if active.Namespace != sourcePVC.Namespace || active.Name != sourcePVC.Name {
		return domain.NewError(
			domain.ErrorConflict,
			verifyRollbackPhase,
			fmt.Sprintf(
				"recorded restored PVC %s/%s does not match source PVC %s/%s",
				active.Namespace,
				active.Name,
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
		)
	}

	if expectedPV.Name == "" || expectedPV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifyRollbackPhase,
			fmt.Sprintf(
				"PVC %s/%s has no recorded source PV identity",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
		)
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(active.Namespace).
		Get(ctx, active.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			fmt.Sprintf("read restored PVC %s/%s", active.Namespace, active.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			fmt.Sprintf(
				"read restored PVC %s/%s returned an empty object",
				active.Namespace,
				active.Name,
			),
		)
	}

	if pvc.UID != active.UID || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != expectedPV.Name {
		return domain.NewError(
			domain.ErrorConflict,
			verifyRollbackPhase,
			fmt.Sprintf("restored PVC %s/%s identity or binding changed", pvc.Namespace, pvc.Name),
		)
	}

	if pvc.UID != sourcePVC.UID && pvc.Annotations[kube.SessionKey] != sessionID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyRollbackPhase,
			fmt.Sprintf(
				"restored PVC %s/%s is not the original or session-owned PVC",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, expectedPV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			"read restored PV "+expectedPV.Name,
			err,
		)
	}

	if pv == nil || pv.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifyRollbackPhase,
			fmt.Sprintf("read restored PV %s returned an empty object", expectedPV.Name),
		)
	}

	if pv.UID != expectedPV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyRollbackPhase,
			fmt.Sprintf("restored PV %s identity or claimRef changed", pv.Name),
		)
	}

	return nil
}

// verifyActiveStorageVolume revalidates an activated volume. The activated claim
// keeps the source PVC's name but may land in destinationNamespace (cross-
// namespace migration); same-namespace callers pass the source namespace.
func verifyActiveStorageVolume(
	ctx context.Context,
	client kubernetes.Interface,
	sessionID string,
	sourcePVC v1alpha1.ObjectReference,
	destinationNamespace string,
	expectedPV v1alpha1.ObjectReference,
	active *v1alpha1.ObjectReference,
) error {
	if active == nil || active.Namespace == "" || active.Name == "" || active.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifyMigrationPhase,
			fmt.Sprintf(
				"PVC %s/%s has no recorded active identity",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
		)
	}

	if active.Namespace != destinationNamespace || active.Name != sourcePVC.Name {
		return domain.NewError(
			domain.ErrorConflict,
			verifyMigrationPhase,
			fmt.Sprintf(
				"recorded active PVC %s/%s does not match activated PVC %s/%s",
				active.Namespace,
				active.Name,
				destinationNamespace,
				sourcePVC.Name,
			),
		)
	}

	if expectedPV.Name == "" || expectedPV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifyMigrationPhase,
			fmt.Sprintf(
				"PVC %s/%s has no recorded destination PV identity",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
		)
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(destinationNamespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf("read PVC %s/%s", destinationNamespace, sourcePVC.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf(
				"read PVC %s/%s returned an empty object",
				destinationNamespace,
				sourcePVC.Name,
			),
		)
	}

	// A resumed workload cannot mount a claim that is being deleted — the
	// scheduler refuses it, so the resume would wait out its whole timeout
	// with the cause hidden in pod events. Surface the loss up front; a
	// deletion pass keeps converging instead (it skips the workload resume).
	if pvc.DeletionTimestamp != nil && !workflowDeletionInProgress(ctx) {
		return domain.NewError(
			domain.ErrorPrecondition,
			verifyMigrationPhase,
			fmt.Sprintf(
				"active PVC %s/%s is terminating (deletion requested at %s); the workload cannot resume onto it",
				pvc.Namespace,
				pvc.Name,
				pvc.DeletionTimestamp.UTC().Format(time.RFC3339),
			),
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName != expectedPV.Name ||
		pvc.Annotations[kube.SessionKey] != sessionID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyMigrationPhase,
			fmt.Sprintf(
				"PVC %s/%s is not active on destination PV %s",
				pvc.Namespace,
				pvc.Name,
				expectedPV.Name,
			),
		)
	}

	if pvc.UID != active.UID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyMigrationPhase,
			fmt.Sprintf("active PVC %s/%s UID changed", pvc.Namespace, pvc.Name),
		)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, expectedPV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			"read active PV "+expectedPV.Name,
			err,
		)
	}

	if pv == nil || pv.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf("read active PV %s returned an empty object", expectedPV.Name),
		)
	}

	if pv.UID != expectedPV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			verifyMigrationPhase,
			fmt.Sprintf("active PV %s identity or claimRef changed", pv.Name),
		)
	}

	return nil
}

func unrecordedActivePVC(
	ctx context.Context,
	client kubernetes.Interface,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	destinationNamespace string,
) (*v1alpha1.ObjectReference, bool, error) {
	// The activated claim keeps the source PVC's name and lands in the plan's
	// destination namespace, so recovery probes the destination. Before
	// activation the lookup is NotFound and the caller proceeds.
	pvc, err := client.CoreV1().
		PersistentVolumeClaims(destinationNamespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, domain.WrapError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf("read PVC %s/%s", destinationNamespace, sourcePVC.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return nil, false, domain.NewError(
			domain.ErrorKubernetes,
			verifyMigrationPhase,
			fmt.Sprintf(
				"read PVC %s/%s returned an empty object",
				destinationNamespace,
				sourcePVC.Name,
			),
		)
	}

	if pvc.UID == sourcePVC.UID && pvc.Spec.VolumeName == sourcePV.Name {
		return nil, true, nil
	}

	return &v1alpha1.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}, true, nil
}
