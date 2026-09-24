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

func TestNamespacedPodMigrationDeletionRetriesWorkloadResumeBeforeReleasingStorage(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	engine := &concreteCopyEngine{}
	executor.transfer.copier = engine
	executor.switcher = &scriptedSwitcher{client: executor.client}
	failure := errors.New("replacement workload is not ready")
	controller := &fakeController{resumeErr: failure}
	executor.workloads = controller

	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.ResumeFrom != domain.PhaseResuming {
		t.Fatalf("unexpected resume phase: %s", object.Status.ResumeFrom)
	}

	for _, checkpoint := range object.Status.Volumes {
		pv, err := executor.client.CoreV1().
			PersistentVolumes().
			Get(t.Context(), checkpoint.DestinationPV.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		pv.Labels = map[string]string{
			kube.SessionKey:        object.Name,
			kube.ResourceRoleLabel: kube.ResourceRoleActive,
		}
		if _, err := executor.client.CoreV1().
			PersistentVolumes().
			Update(t.Context(), pv, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	object.Finalizers = []string{kube.SessionFinalizer}
	if err := store.client.Update(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := store.client.Delete(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := store.client.Get(
		t.Context(),
		crclient.ObjectKeyFromObject(object),
		object,
	); err != nil {
		t.Fatal(err)
	}

	if err := executor.FinalizeDeleted(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	loaded, err := store.Load(
		t.Context(),
		crclient.ObjectKey{Namespace: object.Namespace, Name: object.Name},
	)
	if err != nil {
		t.Fatalf("deletion removed recovery state before workload convergence: %v", err)
	}

	for _, checkpoint := range loaded.Status.Volumes {
		if checkpoint.Activation.ActivePVC == nil || checkpoint.Activation.ActivatedAt == nil {
			t.Fatal("deletion lost activation recovery identities")
		}
	}

	controller.resumeErr = nil

	if err := executor.FinalizeDeleted(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Load(
		t.Context(),
		crclient.ObjectKey{Namespace: object.Namespace, Name: object.Name},
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("converged workflow remains in store: %v", err)
	}

	if controller.resumed != 3 || controller.paused != 1 || len(engine.requests) != 2 {
		t.Fatalf("deletion repeated cutover: paused=%d resumed=%d copies=%d",
			controller.paused, controller.resumed, len(engine.requests))
	}
}

func TestNamespacedPodMigrationCleanupUsesCutoverIdentityAndExplicitPolicies(t *testing.T) {
	executor, object, _, _ := namespacedPodMigrationFixture(t)
	executor.transfer.copier = &concreteCopyEngine{}
	executor.workloads = &fakeController{}
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

func TestNamespacedPodMigrationCleanupAbortedDeletesStagedDestinationButNeverTheSource(
	t *testing.T,
) {
	executor, object, _, _ := namespacedPodMigrationFixture(t)

	object.Status.Phase = domain.PhaseAborted
	for _, volume := range object.Status.Plan.Volumes {
		object.Status.Volumes = append(object.Status.Volumes,
			v1alpha1.PodMigrationVolumeStatus{
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

	// the source-identity probe reads the fixture's live bound source PVCs;
	// namespaced migrations keep them in the workflow namespace

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

func TestNamespacedPodMigrationDeletionCheckpointFailurePreservesState(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	object.Status.Plan = nil
	object.Status.OriginalPodSnapshotHash = ""

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

func TestNamespacedPodMigrationUnplannedDeletionRemovesStoredObject(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	object.Status.Plan = nil
	object.Status.OriginalPodSnapshotHash = ""

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

func TestNamespacedPodMigrationFailSourceDeletedUsesObjectNamespace(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}

	// The wrapper must resolve the source namespace from the workflow object,
	// not a plan field, for the probe to find the deletion.
	deletePlannedSourcePVCs(t, executor.client, object.Namespace, object.Status.Plan.Volumes)

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
}

func namespacedPausedCheckpointFixture(object *v1alpha1.PodMigration) {
	object.Status.Phase = domain.PhaseFailed
	object.Status.ResumeFrom = domain.PhasePausing
	object.Status.History = []v1alpha1.WorkflowHistoryEntry{
		{Phase: domain.PhasePausing, Message: "pausing workload"},
		{Phase: domain.PhaseFailed, Message: "pause interrupted"},
	}

	for _, volume := range object.Status.Plan.Volumes {
		object.Status.Volumes = append(object.Status.Volumes,
			v1alpha1.PodMigrationVolumeStatus{
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
}

func TestNamespacedPodMigrationDeletionConvergesWhenSourceStorageDeleted(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}

	namespacedPausedCheckpointFixture(object)

	// The source storage was deleted underneath the paused workflow: deletion
	// must converge without re-verifying it (#28) instead of wedging the
	// finalizer.
	deletePlannedSourcePVCs(t, executor.client, object.Namespace, object.Status.Plan.Volumes)

	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	// The fake world holds no source PVCs: deletion must converge without
	// re-verifying them (#28) instead of wedging the finalizer.
	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatalf("deleted source storage must not wedge finalization: %v", err)
	}

	if _, err := store.Load(
		t.Context(),
		crclient.ObjectKey{Namespace: object.Namespace, Name: object.Name},
	); !apierrors.IsNotFound(err) {
		t.Fatalf("converged workflow remains in store: %v", err)
	}
}

func TestNamespacedPodMigrationDeletionConvergesWhenSourceTerminating(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}

	namespacedPausedCheckpointFixture(object)

	// The source deletion was armed underneath the paused workflow and a
	// finalizer keeps it stuck in Terminating: the deletion pass must skip
	// the workload resume the same way it does for a vanished source,
	// instead of rejecting the abort and wedging the workflow finalizer.
	armTerminatingSourcePVC(t, executor, object.Namespace, "a")

	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatalf("terminating source storage must not wedge finalization: %v", err)
	}

	if _, err := store.Load(
		t.Context(),
		crclient.ObjectKey{Namespace: object.Namespace, Name: object.Name},
	); !apierrors.IsNotFound(err) {
		t.Fatalf("converged workflow remains in store: %v", err)
	}
}

func TestNamespacedPodMigrationLiveAbortStillRequiresSourceStorage(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}

	namespacedPausedCheckpointFixture(object)

	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Abort(t.Context(), object); err == nil {
		t.Fatal("live abort must still verify the source storage it restores")
	}
}

func TestNamespacedPodMigrationCleanupReleasesStandalonePodMarker(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}

	namespacedPausedCheckpointFixture(object)
	// Cleanup admission requires a terminal phase; the fixture helper records
	// the pause-failure shape first.
	object.Status.Phase = domain.PhaseAborted

	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := executor.client.CoreV1().Pods(object.Namespace).Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   object.Namespace,
			Name:        "workload",
			Annotations: map[string]string{kube.SessionKey: object.Name},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := executor.Cleanup(t.Context(), object,
		MigrationCleanupOptions{Finalize: true, DeleteSession: true}); err != nil {
		t.Fatal(err)
	}

	pod, err := executor.client.CoreV1().Pods(object.Namespace).Get(
		t.Context(), "workload", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if pod.Annotations[kube.SessionKey] != "" {
		t.Fatalf(
			"finalized workflow left the standalone Pod owned: %q",
			pod.Annotations[kube.SessionKey],
		)
	}
}
