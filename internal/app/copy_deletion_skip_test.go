package app

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A copy whose source pair was removed while it sat Failed must still
// converge its deletion: the reserved-volume validation re-reads the live
// source PVC, so the deletion pass skips it exactly as the migration families
// already do, and the finalizer cannot wedge on storage that is already gone.
func TestClusterCopyFinalizeDeletedConvergesWhenSourceDeleted(t *testing.T) {
	executor, object, store, _ := copyExecutorFixture(t)
	object.Status.Phase = domain.PhaseFailed
	object.Status.Volumes = failedCopyAttemptStatuses(object.Status.Plan.Volumes)
	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	store.object = object.DeepCopy()

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatalf("half-deleted source wedged the copy finalizer: %v", err)
	}

	if store.object != nil || object.Status.Phase != domain.PhaseAborted {
		t.Fatalf("deletion did not converge: phase=%s", object.Status.Phase)
	}
}

func TestNamespacedCopyFinalizeDeletedConvergesWhenSourceDeleted(t *testing.T) {
	executor, object, _ := namespacedCopyFixture(t, interceptor.Funcs{})
	object.Status.Phase = domain.PhaseFailed
	object.Status.Volumes = namespacedFailedCopyAttemptStatuses(object.Status.Plan.Volumes)
	object.DeletionTimestamp = &metav1.Time{Time: object.CreationTimestamp.Time}

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatalf("half-deleted source wedged the copy finalizer: %v", err)
	}

	if object.Status.Phase != domain.PhaseAborted {
		t.Fatalf("deletion did not converge: phase=%s", object.Status.Phase)
	}
}

func failedCopyAttemptStatuses(volumes []v1alpha1.VolumeSpec) []v1alpha1.ClusterCopyVolumeStatus {
	statuses := make([]v1alpha1.ClusterCopyVolumeStatus, 0, len(volumes))
	for _, volume := range volumes {
		statuses = append(statuses, v1alpha1.ClusterCopyVolumeStatus{
			ClusterVolumeReservationStatus: v1alpha1.ClusterVolumeReservationStatus{
				SourcePVCName: volume.SourcePVC.Name,
				DestinationPVC: &v1alpha1.ObjectReference{
					Kind: "PersistentVolumeClaim", Namespace: "destination",
					Name: volume.DestinationPVC.Name, UID: "destination-pvc",
				},
				DestinationPV: &v1alpha1.ObjectReference{
					Kind: "PersistentVolume", Name: "destination-pv", UID: "destination-pv",
				},
				DestinationPolicy: corev1.PersistentVolumeReclaimRetain,
				Reserved:          true,
			},
			Sync: v1alpha1.CopySyncStatus{Attempts: 1},
		})
	}

	return statuses
}

func namespacedFailedCopyAttemptStatuses(
	volumes []v1alpha1.VolumeSpec,
) []v1alpha1.CopyVolumeStatus {
	statuses := make([]v1alpha1.CopyVolumeStatus, 0, len(volumes))
	for _, volume := range volumes {
		statuses = append(statuses, v1alpha1.CopyVolumeStatus{
			VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
				SourcePVCName: volume.SourcePVC.Name,
				DestinationPVC: &v1alpha1.LocalResourceReference{
					Name: volume.DestinationPVC.Name, UID: "destination-pvc",
				},
				DestinationPV: &v1alpha1.LocalResourceReference{
					Name: "destination-pv", UID: "destination-pv",
				},
				DestinationPolicy: corev1.PersistentVolumeReclaimRetain,
				Reserved:          true,
			},
			Sync: v1alpha1.CopySyncStatus{Attempts: 1},
		})
	}

	return statuses
}
