package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestMigrationCleanupUsesCutoverIdentityAndExplicitPolicies(t *testing.T) {
	executor, object, _, _ := migrationExecutorFixture(t)
	executor.transfer.copier = &concreteCopyEngine{}
	executor.switcher = &scriptedSwitcher{client: executor.client}

	for i := range object.Status.Plan.Volumes {
		object.Status.Plan.Volumes[i].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.DeepCopy()

	_, volumes, _, err := executor.prepareCleanup(t.Context(), object, MigrationCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(before, object) {
		t.Fatal("preview changed migration")
	}

	if len(volumes) != 4 {
		t.Fatalf("reclaim volumes = %d", len(volumes))
	}

	for i := range object.Status.Plan.Volumes {
		source, destination := volumes[2*i], volumes[2*i+1]
		if source.delete || source.policy != corev1.PersistentVolumeReclaimRetain ||
			source.pvc.Name != "" ||
			source.role != kube.ResourceRoleRollback {
			t.Fatalf("original source policy incorrectly authorized deletion: %+v", source)
		}

		if destination.delete || destination.role != kube.ResourceRoleActive ||
			destination.pvc != *object.Status.Volumes[i].Activation.ActivePVC {
			t.Fatalf("cleanup lost active identity: %+v", destination)
		}
	}
}

func TestMigrationCleanupAbortedDeletesStagedDestinationButNeverTheSource(t *testing.T) {
	executor, object, _, _ := migrationExecutorFixture(t)

	object.Status.Phase = domain.PhaseAborted
	for _, volume := range object.Status.Plan.Volumes {
		object.Status.Volumes = append(object.Status.Volumes,
			v1alpha1.ClusterMigrationVolumeStatus{
				ClusterVolumeReservationStatus: v1alpha1.ClusterVolumeReservationStatus{
					SourcePVCName:     volume.SourcePVC.Name,
					Reserved:          true,
					DestinationPolicy: corev1.PersistentVolumeReclaimRetain,
					DestinationPVC: &v1alpha1.ObjectReference{
						Kind:      "PersistentVolumeClaim",
						Namespace: "temporary",
						Name:      "reserved-" + volume.SourcePVC.Name,
						UID:       types.UID("reserved-" + volume.SourcePVC.Name),
					},
					DestinationPV: &v1alpha1.ObjectReference{
						Kind: "PersistentVolume",
						Name: "reserved-pv-" + volume.SourcePVC.Name,
						UID:  types.UID("reserved-pv-" + volume.SourcePVC.Name),
					},
				},
			},
		)
	}

	// the source-identity probe needs the live source PVCs
	for _, volume := range object.Status.Plan.Volumes {
		if _, err := executor.client.CoreV1().PersistentVolumeClaims("source").
			Create(t.Context(), &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "source", Name: volume.SourcePVC.Name,
					UID: volume.SourcePVC.UID,
				},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	_, volumes, _, err := executor.prepareCleanup(
		t.Context(),
		object,
		MigrationCleanupOptions{UnusedStoragePolicy: "Delete"},
	)
	if err != nil {
		t.Fatal(err)
	}

	sourcePVs := map[string]bool{}
	for _, volume := range object.Status.Plan.Volumes {
		sourcePVs[volume.SourcePV.Name] = true
	}

	deleted := 0
	for _, volume := range volumes {
		if sourcePVs[volume.pv.Name] && volume.delete {
			t.Fatalf("aborted cleanup authorized deleting the source PV: %+v", volume)
		}

		if volume.delete {
			deleted++
		}
	}

	if deleted != len(object.Status.Plan.Volumes) {
		t.Fatalf(
			"aborted cleanup deleted %d volumes, want only the staged destinations",
			deleted,
		)
	}
}

func TestMigrationDeletionCheckpointFailurePreservesState(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixture(t)
	object.Status.Plan = nil

	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	failure := errors.New("checkpoint rejected")
	store.failAt, store.err = store.writes+1, failure

	before := object.Status.DeepCopy()
	if err := executor.FinalizeDeleted(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(*before, object.Status) {
		t.Fatal("rejected deletion checkpoint leaked")
	}
}

func TestMigrationUnplannedDeletionRemovesStoredObject(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixture(t)
	object.Status.Plan = nil

	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	executor.client = nil
	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Load(
		t.Context(),
		crclient.ObjectKey{Name: object.Name},
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("deleted migration remains in store: %v", err)
	}
}

func TestClusterMigrationFailSourceDeletedConvergesToFailed(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixture(t)

	// The fixture world holds no source PVCs: the planned source storage was
	// deleted underneath the Reserved phase.
	object.Status.Phase = domain.PhaseReserved

	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.FailSourceDeleted(t.Context(), object); err == nil {
		t.Fatal("expected the recorded source-loss failure")
	}

	if object.Status.Phase != domain.PhaseFailed {
		t.Fatalf("phase = %s, want Failed", object.Status.Phase)
	}

	// Idempotent on a Failed workflow.
	historyLen := len(object.Status.History)
	if err := executor.FailSourceDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(object.Status.History) != historyLen {
		t.Fatal("failed workflow accumulated duplicate source-loss records")
	}
}

func TestClusterMigrationDeletionConvergesWhenSourceStorageDeleted(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixture(t)

	object.Status.Phase = domain.PhaseReserved
	for _, volume := range object.Status.Plan.Volumes {
		object.Status.Volumes = append(object.Status.Volumes,
			v1alpha1.ClusterMigrationVolumeStatus{
				ClusterVolumeReservationStatus: v1alpha1.ClusterVolumeReservationStatus{
					SourcePVCName:     volume.SourcePVC.Name,
					Reserved:          true,
					DestinationPolicy: corev1.PersistentVolumeReclaimRetain,
					DestinationPVC: &v1alpha1.ObjectReference{
						Kind:      "PersistentVolumeClaim",
						Namespace: "temporary",
						Name:      "reserved-" + volume.SourcePVC.Name,
						UID:       types.UID("reserved-" + volume.SourcePVC.Name),
					},
					DestinationPV: &v1alpha1.ObjectReference{
						Kind: "PersistentVolume",
						Name: "reserved-pv-" + volume.SourcePVC.Name,
						UID:  types.UID("reserved-pv-" + volume.SourcePVC.Name),
					},
				},
			},
		)
	}

	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatalf("deleted source storage must not wedge finalization: %v", err)
	}

	if _, err := store.Load(
		t.Context(),
		crclient.ObjectKeyFromObject(object),
	); !apierrors.IsNotFound(err) {
		t.Fatalf("converged workflow remains in store: %v", err)
	}
}
