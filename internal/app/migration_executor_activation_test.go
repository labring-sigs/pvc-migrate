package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMigrationActivationRecoversResourceChangeBeforeCheckpoint(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixture(t)
	executor.transfer.copier = &concreteCopyEngine{}
	executor.transfer.config.Retries = 1
	switcher := &scriptedSwitcher{client: executor.client}

	executor.switcher = switcher
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.FinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.DeepCopy()
	failure := errors.New("activation checkpoint rejected")

	store.err, store.failAt = failure, store.writes+2
	if err := executor.Activate(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseActivating ||
		object.Status.Volumes[0].Activation.ActivatedAt != nil ||
		object.Status.Volumes[0].Activation.ActivePVC != nil {
		t.Fatalf("failed activation checkpoint leaked: %+v", object.Status)
	}

	if err := executor.Activate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseActivated ||
		!reflect.DeepEqual(switcher.activateCalls, []string{"a", "a", "b"}) {
		t.Fatalf(
			"activation did not recover: phase=%s calls=%v",
			object.Status.Phase,
			switcher.activateCalls,
		)
	}

	if !reflect.DeepEqual(object.Spec, before.Spec) ||
		!reflect.DeepEqual(object.Status.Plan, before.Status.Plan) {
		t.Fatal("activation changed the spec or execution plan")
	}

	writes := store.writes

	if err := executor.Activate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes || len(switcher.activateCalls) != 3 {
		t.Fatal("completed activation repeated resource changes")
	}

	active, err := executor.client.CoreV1().
		PersistentVolumeClaims("source").
		Get(t.Context(), "a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	active.UID = "replaced"
	if _, err := executor.client.CoreV1().
		PersistentVolumeClaims("source").
		Update(t.Context(), active, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := executor.Activate(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("activation accepted replaced identity: %v", err)
	}

	if store.writes != writes {
		t.Fatal("conflicting active identity advanced state")
	}
}

func TestMigrationActivationMovesPVCAcrossNamespaces(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixtureWith(
		t,
		func(object *v1alpha1.ClusterMigration) {
			object.Spec.DestinationNamespace = "landing"
			object.Status.Plan.DestinationNamespace = "landing"
		},
	)
	executor.transfer.copier = &concreteCopyEngine{}
	switcher := &scriptedSwitcher{client: executor.client}
	executor.switcher = switcher

	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.FinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Activate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseActivated {
		t.Fatalf("cross-namespace activation failed: phase=%s", object.Status.Phase)
	}

	active := object.Status.Volumes[0].Activation.ActivePVC
	if active == nil || active.Namespace != "landing" || active.Name != "a" {
		t.Fatalf("active identity not recorded in the landing namespace: %+v", active)
	}

	if _, err := executor.client.CoreV1().
		PersistentVolumeClaims("landing").
		Get(t.Context(), "a", metav1.GetOptions{}); err != nil {
		t.Fatalf("activated PVC missing from landing namespace: %v", err)
	}

	writes := store.writes

	if err := executor.Activate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes || len(switcher.activateCalls) != 2 {
		t.Fatal("revalidated cross-namespace activation repeated resource changes")
	}
}

func TestMigrationActivationRejectsMissingFinalSyncBeforeStorageAccess(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixture(t)
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	object.Status.Phase = domain.PhaseFinalSynced
	executor.client = nil
	writes := store.writes

	if err := executor.Activate(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatal(err)
	}

	if store.writes != writes {
		t.Fatal("missing final sync mutated state")
	}
}
