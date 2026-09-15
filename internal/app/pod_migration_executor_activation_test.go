package app

import (
	"errors"
	"reflect"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodMigrationActivationRecoversResourceChangeBeforeCheckpoint(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
	executor.transfer.copier = &concreteCopyEngine{}
	executor.transfer.config.Retries = 1
	switcher := &scriptedSwitcher{client: executor.client}

	executor.switcher = switcher

	executor.workloads = &fakeController{}
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.PauseAndFinalSync(t.Context(), object); err != nil {
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

func TestPodMigrationActivationRejectsMissingFinalSyncBeforeStorageAccess(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
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
