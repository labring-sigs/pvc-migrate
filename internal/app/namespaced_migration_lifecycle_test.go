package app

import (
	"errors"
	"reflect"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestNamespacedMigrationRunAndRollbackRecoverFromRejectedCheckpoint(t *testing.T) {
	executor, object, store, _ := namespacedMigrationFixture(t)
	engine := &concreteCopyEngine{}
	executor.transfer.copier = engine
	executor.transfer.config.Retries = 1
	switcher := &scriptedSwitcher{client: executor.client}
	executor.switcher = switcher

	before := object.DeepCopy()
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseCompleted || object.Status.CompletedAt == nil ||
		len(engine.requests) != 2 {
		t.Fatalf("migration did not complete: %+v", object.Status)
	}

	writes := store.writes

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes || len(engine.requests) != 2 {
		t.Fatal("completed migration repeated work")
	}

	failure := errors.New("rollback checkpoint unavailable")

	store.failAt, store.err = store.writes+2, failure
	if err := executor.Rollback(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseRollingBack ||
		object.Status.Volumes[1].Activation.RolledBackAt != nil {
		t.Fatalf("rollback leaked its rejected checkpoint: %+v", object.Status)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Volumes[1].Activation.RolledBackAt != nil {
		t.Fatal("rejected checkpoint was durably recorded")
	}

	if err := executor.Run(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseRolledBack ||
		!reflect.DeepEqual(switcher.rollbackCalls, []string{"b", "b", "a"}) {
		t.Fatalf(
			"rollback recovery lost reverse ordering: phase=%s calls=%v",
			loaded.Status.Phase,
			switcher.rollbackCalls,
		)
	}

	if !reflect.DeepEqual(loaded.Spec, before.Spec) ||
		!reflect.DeepEqual(loaded.Status.Plan, before.Status.Plan) {
		t.Fatal("migration lifecycle changed its input or plan")
	}

	writes = store.writes

	if err := executor.Rollback(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if err := executor.Run(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes || len(switcher.rollbackCalls) != 3 {
		t.Fatal("completed rollback repeated changes")
	}
}

func TestNamespacedMigrationAbortBeforePlanningDoesNotAccessResources(t *testing.T) {
	executor, object, store, reserver := namespacedMigrationFixture(t)

	object.Status.Plan = nil
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	executor.client = nil
	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted || object.Status.CompletedAt == nil ||
		len(reserver.calls) != 0 {
		t.Fatalf("unplanned abort reached resource execution: %+v", object.Status)
	}

	writes := store.writes

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes {
		t.Fatal("aborted migration rewrote state")
	}
}

func TestNamespacedMigrationAbortRejectsActivatedVolumes(t *testing.T) {
	executor, object, store, _ := namespacedMigrationFixture(t)
	executor.transfer.copier = &concreteCopyEngine{}

	executor.switcher = &scriptedSwitcher{client: executor.client}
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	writes := store.writes

	if err := executor.Abort(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatal(err)
	}

	if store.writes != writes {
		t.Fatal("abort changed activated migration")
	}
}

func TestNamespacedMigrationPlanningResumeRestoresStateWhenSaveFails(t *testing.T) {
	executor, object, store, reserver := namespacedMigrationFixture(t)
	object.Status.Plan = nil
	object.Status.Phase = domain.PhaseFailed
	object.Status.ResumeFrom = domain.PhasePlanned
	object.Status.Message = "planning failed"

	object.Status.ErrorCategory = string(domain.ErrorPrecondition)
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.DeepCopy()
	failure := errors.New("resume checkpoint rejected")

	store.err, store.failAt = failure, store.writes+1
	if err := executor.RequestResume(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("failed resume discarded the planning failure")
	}

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhasePlanned || object.Status.Plan != nil ||
		object.Status.ErrorCategory != "" || len(reserver.calls) != 0 {
		t.Fatalf("planning retry changed execution data: %+v", object.Status)
	}
}
