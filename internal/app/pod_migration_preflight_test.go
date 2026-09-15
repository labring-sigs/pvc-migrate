package app

import (
	"errors"
	"reflect"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestPodMigrationRequestResumeSaveFailurePreservesFailure(t *testing.T) {
	executor, object, store, engine := podMigrationWarmFixture(t)
	controller := &fakeController{}
	executor.workloads = controller

	cause := errors.New("stage interrupted")
	if err := executor.fail(t.Context(), object, cause); !errors.Is(err, cause) {
		t.Fatal(err)
	}

	previous := object.DeepCopy()
	saveErr := errors.New("reactivation save failed")

	store.err, store.failAt = saveErr, store.writes+1
	if err := executor.RequestResume(t.Context(), object); !errors.Is(err, saveErr) {
		t.Fatalf("error = %v", err)
	}

	if !reflect.DeepEqual(previous, object) {
		t.Fatal("failed reactivation changed state")
	}

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseReserved || controller.paused != 0 ||
		len(engine.requests) != 0 {
		t.Fatal("resume request executed work or lost original stage")
	}
}

func TestPodMigrationRequestResumeRetainsCapacityFailure(t *testing.T) {
	executor, object, store, engine := podMigrationWarmFixture(t)
	engine.copy = func(copyengine.Request) error { return errors.New("No space left on device") }

	if err := executor.WarmCopy(t.Context(), object); err == nil {
		t.Fatal("capacity failure ignored")
	}

	before, writes := object.DeepCopy(), store.writes
	if err := executor.RequestResume(t.Context(), object); err == nil {
		t.Fatal("capacity failure was cleared")
	}

	if !reflect.DeepEqual(before, object) || writes != store.writes || len(engine.requests) != 1 {
		t.Fatal("invalid reactivation changed durable state")
	}
}

func TestNamespacedPodMigrationRequestResumeSaveFailurePreservesFailure(t *testing.T) {
	executor, object, store, engine := namespacedPodMigrationWarmFixture(t)
	controller := &fakeController{}
	executor.workloads = controller

	cause := errors.New("stage interrupted")
	if err := executor.fail(t.Context(), object, cause); !errors.Is(err, cause) {
		t.Fatal(err)
	}

	previous := object.DeepCopy()
	saveErr := errors.New("reactivation save failed")

	store.err, store.failAt = saveErr, store.writes+1
	if err := executor.RequestResume(t.Context(), object); !errors.Is(err, saveErr) {
		t.Fatalf("error = %v", err)
	}

	if !reflect.DeepEqual(previous, object) {
		t.Fatal("failed reactivation changed state")
	}

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseReserved || controller.paused != 0 ||
		len(engine.requests) != 0 {
		t.Fatal("resume request executed work or lost original stage")
	}
}

func TestNamespacedPodMigrationRequestResumeRetainsCapacityFailure(t *testing.T) {
	executor, object, store, engine := namespacedPodMigrationWarmFixture(t)
	engine.copy = func(copyengine.Request) error { return errors.New("No space left on device") }

	if err := executor.WarmCopy(t.Context(), object); err == nil {
		t.Fatal("capacity failure ignored")
	}

	before, writes := object.DeepCopy(), store.writes
	if err := executor.RequestResume(t.Context(), object); err == nil {
		t.Fatal("capacity failure was cleared")
	}

	if !reflect.DeepEqual(before, object) || writes != store.writes || len(engine.requests) != 1 {
		t.Fatal("invalid reactivation changed durable state")
	}
}
