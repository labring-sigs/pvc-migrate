package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestMigrationFinalSyncResumesOnlyIncompleteVolumes(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixture(t)
	engine := &concreteCopyEngine{}
	executor.transfer.copier = engine
	executor.transfer.config.Retries = 1

	executor.switcher = &fakeSwitcher{}
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.DeepCopy()
	failure := errors.New("transfer interrupted")
	engine.copy = func(request copyengine.Request) error {
		if request.Source.Name == "b" {
			return failure
		}
		return nil
	}

	if err := executor.FinalSync(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseFinalSyncing ||
		object.Status.Volumes[0].Sync.FinalCompletedAt == nil ||
		object.Status.Volumes[1].Sync.FinalCompletedAt != nil {
		t.Fatalf("sync checkpoints were lost: %+v", object.Status)
	}

	if len(engine.requests) != 2 {
		t.Fatalf("requests=%+v", engine.requests)
	}

	for _, request := range engine.requests {
		if request.Mode != copyengine.ModeFinal || request.Source.Namespace != "source" ||
			request.Destination.Namespace != "temporary" {
			t.Fatalf("wrong migration transfer: %+v", request)
		}
	}

	engine.copy = nil

	object.Status.Volumes[0], object.Status.Volumes[1] = object.Status.Volumes[1], object.Status.Volumes[0]
	if err := store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.FinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFinalSynced || len(engine.requests) != 3 ||
		engine.requests[2].Source.Name != "b" ||
		engine.requests[2].Attempt != 2 ||
		len(engine.cleanups) != 1 ||
		engine.cleanups[0].Source.Name != "b" {
		t.Fatalf(
			"retry did not isolate unfinished volume: phase=%s copies=%+v cleanups=%+v",
			object.Status.Phase,
			engine.requests,
			engine.cleanups,
		)
	}

	if !reflect.DeepEqual(object.Spec, before.Spec) ||
		!reflect.DeepEqual(object.Status.Plan, before.Status.Plan) {
		t.Fatal("sync mutated input or plan")
	}
}

func TestMigrationFinalSyncRestartRollsBackOnCheckpointFailure(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixture(t)
	engine := &concreteCopyEngine{}
	executor.transfer.copier = engine
	executor.transfer.config.Retries = 1

	executor.switcher = &fakeSwitcher{}
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.FinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.DeepCopy()
	failure := errors.New("restart checkpoint rejected")

	store.failAt, store.err = store.writes+1, failure
	if err := executor.FinalSync(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(object, before) || len(engine.requests) != 2 {
		t.Fatal("failed restart cleared completed checkpoints or launched a transfer")
	}

	if err := executor.FinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(engine.requests) != 4 || object.Status.Phase != domain.PhaseFinalSynced {
		t.Fatalf("restart failed: %+v", object.Status)
	}
}

func TestMigrationFinalSyncRejectsIncompleteReservation(t *testing.T) {
	executor, object, store, _ := migrationExecutorFixture(t)
	object.Status.Phase = domain.PhaseFinalSyncing

	object.Status.Volumes = []v1alpha1.ClusterMigrationVolumeStatus{{}}
	if err := executor.FinalSync(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatal(err)
	}

	if store.writes != 0 {
		t.Fatal("invalid reservation reached execution")
	}
}
