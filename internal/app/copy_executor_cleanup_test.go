package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func copyCleanupFixture(
	t *testing.T,
) (*ClusterCopyExecutor, *v1alpha1.ClusterCopy, *copyCheckpointStore, *fake.Clientset) {
	t.Helper()

	_, _, _, client := reservationCleanupFixture(t)

	executor, object, store, _ := copyExecutorFixture(t)
	for i := range object.Status.Plan.Volumes {
		object.Status.Plan.Volumes[i].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	}

	store.object = object.DeepCopy()
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	executor.client = client
	executor.transfer.client = client

	return executor, object, store, client
}

func TestCopyExecutorCleanupRecoveryIsReadOnlyUntilCheckpointSaved(t *testing.T) {
	executor, object, store, client := copyCleanupFixture(t)
	object.Status.Phase = domain.PhaseAborted
	object.Status.Volumes[0].Reserved = false
	object.Status.Volumes[0].DestinationPV = nil
	object.Status.Volumes[0].Sync = v1alpha1.CopySyncStatus{}
	store.object = object.DeepCopy()
	before := object.DeepCopy()
	options := CopyCleanupOptions{Finalize: true}

	if err := executor.ValidateCleanup(t.Context(), object, options); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("cleanup preview mutated the workflow")
	}

	assertReservationCleanupReadOnly(t, client)

	failure := errors.New("checkpoint unavailable")
	store.save = func(*v1alpha1.ClusterCopy) error { return failure }

	if err := executor.Cleanup(t.Context(), object, options); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("failed checkpoint changed in-memory progress")
	}

	assertReservationCleanupReadOnly(t, client)

	store.save = nil

	for range 2 {
		if err := executor.Cleanup(t.Context(), object, options); err != nil {
			t.Fatal(err)
		}
	}

	for _, volume := range object.Status.Plan.Volumes {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims("source").
			Get(t.Context(), volume.SourcePVC.Name, metav1.GetOptions{})
		if err != nil || pvc.UID != volume.SourcePVC.UID || pvc.Annotations[kube.SessionKey] != "" {
			t.Fatalf("source not retained and released: %+v, %v", pvc, err)
		}
	}
}

func TestCopyExecutorDeletionRetainsMetadataUntilLeaseRemoval(t *testing.T) {
	executor, object, store, _ := copyCleanupFixture(t)
	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	store.object = object.DeepCopy()
	failure := errors.New("lease deletion failed")
	lock := &renameDeletionLock{deleteErr: failure}

	executor.locker = &fakeSessionLocker{lock: lock}
	if err := executor.FinalizeDeleted(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if store.object == nil {
		t.Fatal("lease failure removed workflow metadata")
	}

	lock.deleteErr = nil

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.object != nil || lock.deletes != 2 {
		t.Fatal("deletion retry did not finish")
	}
}

func TestCopyExecutorDeletionBeforePlanningNeedsNoDataPlane(t *testing.T) {
	executor, object, store, _ := copyExecutorFixture(t)
	object.Status = v1alpha1.ClusterCopyStatus{}
	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	store.object = object.DeepCopy()
	executor.client = nil

	executor.transfer = nil
	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.object != nil || object.Status.Phase != domain.PhaseAborted {
		t.Fatal("unplanned deletion did not complete")
	}
}

func TestCopyExecutorCapacityFailureRejectsRetryWithoutSideEffects(t *testing.T) {
	executor, object, store, engine := copyExecutorFixture(t)
	object.Status.Phase = domain.PhaseFailed
	object.Status.ResumeFrom = domain.PhasePlanned
	object.Status.FailureReason = domain.FailureDestinationCapacityExhausted
	store.object = object.DeepCopy()

	before := object.DeepCopy()
	for _, action := range []func() error{
		func() error { return executor.Validate(t.Context(), object) },
		func() error { return executor.RequestResume(t.Context(), object) },
		func() error { return executor.Run(t.Context(), object) },
	} {
		if err := action(); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("error = %v", err)
		}
	}

	if !reflect.DeepEqual(object, before) || len(engine.requests) != 0 ||
		len(engine.cleanups) != 0 {
		t.Fatal("capacity failure retry changed progress or reached the engine")
	}
}
