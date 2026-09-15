package app

import (
	"errors"
	"reflect"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestNamespacedPodMigrationCleanupRejectsActiveSourceDeletion(t *testing.T) {
	executor, object, _, _ := namespacedPodMigrationFixture(t)
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
