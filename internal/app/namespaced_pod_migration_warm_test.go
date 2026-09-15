package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func namespacedPodMigrationWarmFixture(
	t *testing.T,
) (*PodMigrationExecutor, *v1alpha1.PodMigration, *namespacedPodMigrationCheckpointStore, *concreteCopyEngine) {
	t.Helper()
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	engine := &concreteCopyEngine{}
	executor.transfer.copier = engine
	executor.transfer.config.Retries = 1
	installPodWarmSourcePVs(t, executor.client, object.Status.Plan.Volumes)

	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	return executor, object, store, engine
}

func TestNamespacedPodMigrationWarmCopyResumesIncompleteVolumes(t *testing.T) {
	executor, object, store, engine := namespacedPodMigrationWarmFixture(t)
	before := object.DeepCopy()
	failure := errors.New("copy interrupted")
	engine.copy = func(request copyengine.Request) error {
		if request.Source.Name == "b" {
			return failure
		}
		return nil
	}

	if err := executor.WarmCopy(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseWarmCopying ||
		object.Status.WarmPassesCompleted != 0 ||
		object.Status.Volumes[0].Sync.WarmCompletedAt == nil ||
		object.Status.Volumes[1].Sync.WarmCompletedAt != nil {
		t.Fatalf("lost warm-copy checkpoints: %+v", object.Status)
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

	if err := executor.WarmCopy(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseWarmCopied || loaded.Status.WarmPassesCompleted != 1 ||
		len(
			engine.requests,
		) != 3 || engine.requests[2].Source.Name != "b" || engine.requests[2].Attempt != 2 ||
		len(
			engine.cleanups,
		) != 1 || engine.cleanups[0].Source.Name != "b" || engine.cleanups[0].Mode != copyengine.ModeWarm {
		t.Fatalf(
			"warm-copy recovery repeated completed work: %+v copies=%+v cleanup=%+v",
			loaded.Status,
			engine.requests,
			engine.cleanups,
		)
	}

	for _, request := range engine.requests {
		if request.Mode != copyengine.ModeWarm || request.Source.Namespace != "data" ||
			request.Destination.Namespace != "data" {
			t.Fatalf("incorrect transfer scope: %+v", request)
		}
	}

	if !reflect.DeepEqual(loaded.Spec, before.Spec) ||
		!reflect.DeepEqual(loaded.Status.Plan, before.Status.Plan) {
		t.Fatal("copy changed input or immutable execution plan")
	}
}

func TestNamespacedPodMigrationWarmPassCheckpointFailureDoesNotRepeatCopy(t *testing.T) {
	executor, object, store, engine := namespacedPodMigrationWarmFixture(t)
	failure := errors.New("pass checkpoint unavailable")
	// Begin, attempt A, completion A, attempt B, completion B, then pass completion.
	store.failAt, store.err = store.writes+6, failure
	if err := executor.WarmCopy(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.WarmPassesCompleted != 0 || object.Status.Phase != domain.PhaseWarmCopying {
		t.Fatal("failed pass checkpoint advanced lifecycle")
	}

	if err := executor.WarmCopy(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.WarmPassesCompleted != 1 || object.Status.Phase != domain.PhaseWarmCopied ||
		len(engine.requests) != 2 {
		t.Fatalf(
			"pass checkpoint retry repeated volume copies: %+v requests=%d",
			object.Status,
			len(engine.requests),
		)
	}
}

func TestNamespacedPodMigrationWarmRestartSaveFailurePreservesCompletedPass(t *testing.T) {
	executor, object, store, engine := namespacedPodMigrationWarmFixture(t)
	if err := executor.WarmCopy(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.DeepCopy()
	failure := errors.New("restart checkpoint unavailable")

	store.failAt, store.err = store.writes+1, failure
	if err := executor.WarmCopy(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(object, before) || len(engine.requests) != 2 {
		t.Fatal("failed restart cleared completed checkpoints or started another copy")
	}

	if err := executor.WarmCopy(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.WarmPassesCompleted != 2 || len(engine.requests) != 4 {
		t.Fatal("new pass did not copy all volumes")
	}
}

func TestNamespacedPodMigrationWarmCopyCapacityFailureBlocksRetry(t *testing.T) {
	executor, object, _, engine := namespacedPodMigrationWarmFixture(t)
	engine.copy = func(copyengine.Request) error { return errors.New("No space left on device") }

	if err := executor.WarmCopy(t.Context(), object); err == nil {
		t.Fatal("capacity failure ignored")
	}

	if object.Status.FailureReason != domain.FailureDestinationCapacityExhausted {
		t.Fatal(object.Status.FailureReason)
	}

	if err := executor.WarmCopy(t.Context(), object); err == nil {
		t.Fatal("capacity failure was retried")
	}

	if len(engine.requests) != 1 {
		t.Fatal("capacity failure launched another transfer")
	}
}

func TestNamespacedPodMigrationWarmCopyStopsOnFenceLoss(t *testing.T) {
	executor, object, _, engine := namespacedPodMigrationWarmFixture(t)
	lost := errors.New("lease lost")
	lock := &fakeSessionLock{}
	executor.locker = &fakeSessionLocker{lock: lock}
	engine.copy = func(copyengine.Request) error { lock.err = lost; return nil }

	if err := executor.WarmCopy(t.Context(), object); !errors.Is(err, lost) {
		t.Fatal(err)
	}

	if len(engine.requests) != 1 || object.Status.Volumes[0].Sync.WarmCompletedAt != nil ||
		object.Status.WarmPassesCompleted != 0 {
		t.Fatal("lost fence advanced copy checkpoints")
	}
}
