package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestNamespacedPodMigrationFinalSyncResumesIncompleteVolumes(t *testing.T) {
	executor, object, store, engine := namespacedPodMigrationWarmFixture(t)

	executor.workloads = &fakeController{}
	if err := executor.Pause(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.Status.Plan.DeepCopy()
	cause := errors.New("final copy interrupted")
	engine.copy = func(request copyengine.CopyRequest) error {
		if request.AttemptIdentity.Source.Name == "b" {
			return cause
		}
		return nil
	}

	if err := executor.FinalSync(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseFinalSyncing ||
		object.Status.Volumes[0].Sync.FinalCompletedAt == nil ||
		object.Status.Volumes[1].Sync.FinalCompletedAt != nil {
		t.Fatalf("lost final checkpoint: %+v", object.Status)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	loaded.Status.Volumes[0], loaded.Status.Volumes[1] = loaded.Status.Volumes[1], loaded.Status.Volumes[0]
	if err := store.Save(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	engine.copy = nil

	if err := executor.FinalSync(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseFinalSynced || len(engine.requests) != 3 ||
		engine.requests[2].AttemptIdentity.Source.Name != "b" || engine.requests[2].Attempt != 2 ||
		len(
			engine.cleanups,
		) != 1 || engine.cleanups[0].Source.Name != "b" || engine.cleanups[0].Mode != copyengine.ModeFinal {
		t.Fatalf(
			"incorrect retry copies=%+v cleanup=%+v phase=%s",
			engine.requests,
			engine.cleanups,
			loaded.Status.Phase,
		)
	}

	if !reflect.DeepEqual(before, loaded.Status.Plan) {
		t.Fatal("final sync changed plan")
	}
}

func TestNamespacedPodMigrationFinalSyncCompletionSaveRetry(t *testing.T) {
	executor, object, store, engine := namespacedPodMigrationWarmFixture(t)

	executor.workloads = &fakeController{}
	if err := executor.Pause(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	cause := errors.New("completion save failed")

	store.err, store.failAt = cause, store.writes+6
	if err := executor.FinalSync(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Phase != domain.PhaseFinalSyncing {
		t.Fatal("save failure advanced phase")
	}

	if err := executor.FinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFinalSynced || len(engine.requests) != 2 {
		t.Fatal("retry repeated completed volumes")
	}
}

func TestNamespacedPodMigrationCutoverProbesBeforePause(t *testing.T) {
	executor, object, _, engine := namespacedPodMigrationWarmFixture(t)
	controller := &fakeController{}
	executor.workloads = controller
	cause := errors.New("image probe failed")
	probe := &recordingToolImageProber{
		err:     cause,
		results: []kube.ToolImageProbeResult{},
		onProbe: func(context.Context) {
			if controller.paused != 0 {
				t.Fatal("probe ran after pause")
			}
		},
	}

	executor.config.ToolImageProber = probe
	if err := executor.PauseAndFinalSync(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error = %v", err)
	}

	if controller.paused != 0 || object.Status.Phase != domain.PhaseReserved ||
		len(engine.requests) != 0 {
		t.Fatal("failed probe interrupted workload")
	}

	probe.err = nil
	if err := executor.PauseAndFinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if controller.paused != 1 || object.Status.Phase != domain.PhaseFinalSynced ||
		len(engine.requests) != 2 {
		t.Fatal("cutover did not pause and sync")
	}
}

func TestNamespacedPodMigrationFinalSyncStopsOnFenceLoss(t *testing.T) {
	executor, object, _, engine := namespacedPodMigrationWarmFixture(t)

	executor.workloads = &fakeController{}
	if err := executor.Pause(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	lost := errors.New("lease lost")
	lock := &fakeSessionLock{}
	executor.locker = &fakeSessionLocker{lock: lock}
	engine.copy = func(copyengine.CopyRequest) error { lock.err = lost; return nil }

	if err := executor.FinalSync(t.Context(), object); !errors.Is(err, lost) {
		t.Fatalf("error = %v", err)
	}

	if len(engine.requests) != 1 || object.Status.Volumes[0].Sync.FinalCompletedAt != nil {
		t.Fatal("fence loss advanced checkpoint")
	}
}
