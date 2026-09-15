package app

import (
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestPodMigrationAbortAfterPartialPauseResumesWorkload(t *testing.T) {
	executor, object, _, _ := podMigrationExecutorFixture(t)
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	pauseErr := errors.New("pause convergence interrupted")

	executor.workloads = &checkpointPodController{pauseErr: pauseErr}
	if err := executor.Pause(t.Context(), object); !errors.Is(err, pauseErr) {
		t.Fatalf("error=%v", err)
	}

	installPodAbortSources(t, executor.client, "source", object.Status.Plan.Volumes)

	controller := &fakeController{}

	executor.workloads = controller
	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if controller.resumed != 1 || object.Status.Phase != domain.PhaseAborted {
		t.Fatal("partial pause did not resume")
	}
}

func TestPodMigrationAbortBeforePlanningNeedsNoWorkload(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(
		t,
		func(o *v1alpha1.ClusterPodMigration) { o.Status.Plan = nil },
	)
	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted {
		t.Fatal("unplanned abort failed")
	}

	writes := store.writes

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes {
		t.Fatal("completed abort was persisted again")
	}
}

func TestPodMigrationAbortRejectsCopyWithoutReservation(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	object.Status.Phase = domain.PhaseAborting
	object.Status.Volumes[0].Reserved = false
	object.Status.Volumes[0].DestinationPVC = nil
	object.Status.Volumes[0].DestinationPV = nil
	object.Status.Volumes[0].Sync.Attempts = 1
	executor.client = nil
	writes := store.writes

	if err := executor.Abort(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("error=%v", err)
	}

	if store.writes != writes {
		t.Fatal("malformed checkpoint reached persistence")
	}
}

func TestNamespacedPodMigrationAbortAfterPartialPauseResumesWorkload(t *testing.T) {
	executor, object, _, _ := namespacedPodMigrationFixture(t)
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	pauseErr := errors.New("pause convergence interrupted")

	executor.workloads = &checkpointPodController{pauseErr: pauseErr}
	if err := executor.Pause(t.Context(), object); !errors.Is(err, pauseErr) {
		t.Fatalf("error=%v", err)
	}

	installPodAbortSources(t, executor.client, object.Namespace, object.Status.Plan.Volumes)

	controller := &fakeController{}

	executor.workloads = controller
	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if controller.resumed != 1 || object.Status.Phase != domain.PhaseAborted {
		t.Fatal("partial pause did not resume")
	}
}

func TestNamespacedPodMigrationAbortBeforePlanningNeedsNoWorkload(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(
		t,
		func(o *v1alpha1.PodMigration) { o.Status.Plan = nil },
	)
	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted {
		t.Fatal("unplanned abort failed")
	}

	writes := store.writes

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes {
		t.Fatal("completed abort was persisted again")
	}
}

func TestNamespacedPodMigrationAbortRejectsCopyWithoutReservation(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	object.Status.Phase = domain.PhaseAborting
	object.Status.Volumes[0].Reserved = false
	object.Status.Volumes[0].DestinationPVC = nil
	object.Status.Volumes[0].DestinationPV = nil
	object.Status.Volumes[0].Sync.Attempts = 1
	executor.client = nil
	writes := store.writes

	if err := executor.Abort(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("error=%v", err)
	}

	if store.writes != writes {
		t.Fatal("malformed checkpoint reached persistence")
	}
}
