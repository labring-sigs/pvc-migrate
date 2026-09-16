package app

import (
	"errors"
	"reflect"
	"strings"
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

func TestPodMigrationDeletionRetriesWorkloadResumeBeforeReleasingStorage(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
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

	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.FinalizeDeleted(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKey{Name: object.Name})
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
		crclient.ObjectKey{Name: object.Name},
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

func TestPodMigrationDeletionConvergesWhenSourceStorageDeleted(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
	engine := &concreteCopyEngine{}
	executor.transfer.copier = engine
	executor.workloads = &fakeController{}

	object.Status.Phase = domain.PhaseFailed
	object.Status.ResumeFrom = domain.PhasePausing

	object.Status.History = []v1alpha1.WorkflowHistoryEntry{
		{Phase: domain.PhasePausing, Message: "pausing workload"},
		{Phase: domain.PhaseFailed, Message: "pause interrupted"},
	}
	for _, volume := range object.Status.Plan.Volumes {
		object.Status.Volumes = append(object.Status.Volumes,
			v1alpha1.ClusterPodMigrationVolumeStatus{
				Sync: v1alpha1.PodMigrationSyncStatus{Attempts: 1},
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
		crclient.ObjectKey{Name: object.Name},
	); !apierrors.IsNotFound(err) {
		t.Fatalf("converged workflow remains in store: %v", err)
	}
}

func TestPodMigrationLiveAbortStillRequiresSourceStorage(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
	executor.workloads = &fakeController{}

	object.Status.Phase = domain.PhaseFailed
	object.Status.ResumeFrom = domain.PhasePausing
	object.Status.History = []v1alpha1.WorkflowHistoryEntry{
		{Phase: domain.PhasePausing, Message: "pausing workload"},
		{Phase: domain.PhaseFailed, Message: "pause interrupted"},
	}

	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Abort(t.Context(), object); err == nil {
		t.Fatal("live abort must still verify the source storage it restores")
	}
}

func TestPodMigrationCleanupReleasesStandalonePodMarker(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
	executor.workloads = &fakeController{}

	object.Status.Phase = domain.PhaseAborted
	for _, volume := range object.Status.Plan.Volumes {
		object.Status.Volumes = append(object.Status.Volumes,
			v1alpha1.ClusterPodMigrationVolumeStatus{
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

	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := executor.client.CoreV1().Pods("source").Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "source",
			Name:        "workload",
			Annotations: map[string]string{kube.SessionKey: object.Name},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := executor.Cleanup(
		t.Context(),
		object,
		MigrationCleanupOptions{Finalize: true, DeleteSession: true},
	); err != nil {
		t.Fatal(err)
	}

	pod, err := executor.client.CoreV1().Pods("source").Get(
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

func TestPodMigrationCleanupUsesCutoverIdentityAndExplicitPolicies(t *testing.T) {
	executor, object, _, _ := podMigrationExecutorFixture(t)
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
			destination.pvc != *object.Status.Volumes[i].Activation.ActivePVC {
			t.Fatalf("cleanup lost active identity: %+v", destination)
		}
	}
}

func TestPodMigrationCleanupRejectsActiveSourceDeletion(t *testing.T) {
	executor, object, _, _ := podMigrationExecutorFixture(t)
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

func TestPodMigrationDeletionCheckpointFailurePreservesState(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
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

func TestPodMigrationUnplannedDeletionRemovesStoredObject(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
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
		crclient.ObjectKey{Name: object.Name},
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("deleted migration remains in store: %v", err)
	}
}

func TestPodMigrationFailSourceDeletedConvergesToFailed(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
	executor.workloads = &fakeController{}

	object.Status.Phase = domain.PhaseReserved
	for _, volume := range object.Status.Plan.Volumes {
		object.Status.Volumes = append(object.Status.Volumes,
			v1alpha1.ClusterPodMigrationVolumeStatus{
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

	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	// The fixture world holds no source PVCs: the planned source storage was
	// deleted underneath the Reserved phase. The reconciler calls this probe
	// before Run; the workflow must converge to Failed instead of looping.
	if err := executor.FailSourceDeleted(t.Context(), object); err == nil {
		t.Fatal("expected the recorded source-loss failure")
	}

	if object.Status.Phase != domain.PhaseFailed {
		t.Fatalf("phase = %s, want Failed", object.Status.Phase)
	}

	last := object.Status.History[len(object.Status.History)-1]
	if last.Phase != domain.PhaseFailed ||
		!strings.Contains(last.Message, "source PVC no longer exists") {
		t.Fatalf("failure history = %s/%s", last.Phase, last.Message)
	}

	// The probe is idempotent on a Failed workflow: no duplicate history.
	historyLen := len(object.Status.History)
	if err := executor.FailSourceDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(object.Status.History) != historyLen {
		t.Fatal("failed workflow accumulated duplicate source-loss records")
	}
}

func TestPodMigrationFailSourceDeletedNoopWhenSourcePresent(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
	executor.workloads = &fakeController{}

	object.Status.Phase = domain.PhaseReserved
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	// Materialize the planned source storage so the probe finds it present.
	for _, volume := range object.Status.Plan.Volumes {
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: volume.SourcePV.Name, UID: volume.SourcePV.UID},
		}
		if _, err := executor.client.CoreV1().
			PersistentVolumes().
			Create(t.Context(), pv, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "source",
				Name:      volume.SourcePVC.Name,
				UID:       volume.SourcePVC.UID,
			},
		}
		if _, err := executor.client.CoreV1().
			PersistentVolumeClaims("source").
			Create(t.Context(), pvc, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	if err := executor.FailSourceDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseReserved {
		t.Fatalf("phase = %s, want untouched Reserved", object.Status.Phase)
	}
}
