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

func TestNamespacedMigrationCleanupUsesCutoverIdentityAndExplicitPolicies(t *testing.T) {
	executor, object, _, _ := namespacedMigrationFixture(t)
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
			destination.pvc != qualifiedResourceReference(
				*object.Status.Volumes[i].Activation.ActivePVC,
				object.Namespace,
			) {
			t.Fatalf("cleanup lost active identity: %+v", destination)
		}
	}
}

func TestNamespacedMigrationCleanupRejectsActiveSourceDeletion(t *testing.T) {
	executor, object, _, _ := namespacedMigrationFixture(t)
	object.Status.Phase = domain.PhaseAborted

	executor.client = nil
	if err := executor.ValidateCleanup(
		t.Context(),
		object,
		MigrationCleanupOptions{SourcePVReclaimPolicy: "Delete"},
	); err == nil {
		t.Fatal("active source deletion accepted")
	}
}

func TestNamespacedMigrationDeletionCheckpointFailurePreservesState(t *testing.T) {
	executor, object, store, _ := namespacedMigrationFixture(t)
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

func TestNamespacedMigrationUnplannedDeletionRemovesStoredObject(t *testing.T) {
	executor, object, store, _ := namespacedMigrationFixture(t)
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
		crclient.ObjectKeyFromObject(object),
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("deleted migration remains in store: %v", err)
	}
}

func TestNamespacedMigrationFailSourceDeletedConvergesToFailed(t *testing.T) {
	executor, object, store, _ := namespacedMigrationFixture(t)

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

	historyLen := len(object.Status.History)
	if err := executor.FailSourceDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(object.Status.History) != historyLen {
		t.Fatal("failed workflow accumulated duplicate source-loss records")
	}
}

func TestNamespacedMigrationDeletionConvergesWhenSourceStorageDeleted(t *testing.T) {
	executor, object, store, _ := namespacedMigrationFixture(t)

	object.Status.Phase = domain.PhaseReserved
	for _, volume := range object.Status.Plan.Volumes {
		object.Status.Volumes = append(object.Status.Volumes,
			v1alpha1.MigrationVolumeStatus{
				VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
					SourcePVCName:     volume.SourcePVC.Name,
					Reserved:          true,
					DestinationPolicy: corev1.PersistentVolumeReclaimRetain,
					DestinationPVC: &v1alpha1.LocalResourceReference{
						Kind: "PersistentVolumeClaim",
						Name: "reserved-" + volume.SourcePVC.Name,
						UID:  types.UID("reserved-" + volume.SourcePVC.Name),
					},
					DestinationPV: &v1alpha1.LocalResourceReference{
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
