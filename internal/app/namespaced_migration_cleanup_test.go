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
