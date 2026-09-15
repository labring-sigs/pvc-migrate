package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func installPodWarmSourcePVs(
	t *testing.T,
	client kubernetes.Interface,
	volumes []v1alpha1.VolumeSpec,
) {
	t.Helper()

	for _, volume := range volumes {
		if _, err := client.CoreV1().
			PersistentVolumes().
			Create(t.Context(), &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: volume.SourcePV.Name, UID: volume.SourcePV.UID},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func podMigrationWarmFixture(
	t *testing.T,
) (*ClusterPodMigrationExecutor, *v1alpha1.ClusterPodMigration, *podMigrationCheckpointStore, *concreteCopyEngine) {
	t.Helper()
	executor, object, store, _ := podMigrationExecutorFixture(t)
	engine := &concreteCopyEngine{}
	executor.transfer.copier = engine
	executor.transfer.config.Retries = 1
	installPodWarmSourcePVs(t, executor.client, object.Status.Plan.Volumes)

	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	return executor, object, store, engine
}

func TestPodMigrationWarmCopyResumesIncompleteVolumes(t *testing.T) {
	executor, object, store, engine := podMigrationWarmFixture(t)
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
		if request.Mode != copyengine.ModeWarm || request.Source.Namespace != "source" ||
			request.Destination.Namespace != "temporary" {
			t.Fatalf("incorrect transfer scope: %+v", request)
		}
	}

	if !reflect.DeepEqual(loaded.Spec, before.Spec) ||
		!reflect.DeepEqual(loaded.Status.Plan, before.Status.Plan) {
		t.Fatal("copy changed input or immutable execution plan")
	}
}

func TestPodMigrationWarmPassCheckpointFailureDoesNotRepeatCopy(t *testing.T) {
	executor, object, store, engine := podMigrationWarmFixture(t)
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

func TestPodMigrationWarmRestartSaveFailurePreservesCompletedPass(t *testing.T) {
	executor, object, store, engine := podMigrationWarmFixture(t)
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

func TestPodMigrationWarmCopyCapacityFailureBlocksRetry(t *testing.T) {
	executor, object, _, engine := podMigrationWarmFixture(t)
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

func TestPodMigrationWarmFailureAfterCompletedPassStartsNewPass(t *testing.T) {
	executor, object, _, engine := podMigrationWarmFixture(t)
	if err := executor.WarmCopy(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	failure := errors.New("pre-copy cleanup failed")
	if err := executor.fail(t.Context(), object, failure); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.ResumeFrom != domain.PhaseWarmCopied {
		t.Fatal("failed preparation lost the completed-pass checkpoint")
	}

	if err := executor.WarmCopy(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(engine.requests) != 4 || object.Status.WarmPassesCompleted != 2 {
		t.Fatal("retry counted a new pass without copying its volumes")
	}
}

func TestPodMigrationWarmCopyStopsOnFenceLoss(t *testing.T) {
	executor, object, _, engine := podMigrationWarmFixture(t)
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

func TestPodMigrationWarmInventoryErrorsPreserveCategoryAndState(t *testing.T) {
	for _, resource := range []string{"pods", "nodes", "persistentvolumes"} {
		t.Run(resource, func(t *testing.T) {
			executor, object, store, engine := podMigrationWarmFixture(t)
			before, writes := object.DeepCopy(), store.writes
			failure := errors.New("source inventory unavailable")

			client, ok := executor.client.(*fake.Clientset)
			if !ok {
				t.Fatal("fixture requires a fake Kubernetes client")
			}

			client.PrependReactor(
				"*",
				resource,
				func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, failure
				},
			)

			err := executor.ValidateWarmCopy(t.Context(), object)
			if !errors.Is(err, failure) || domain.CategoryOf(err) != domain.ErrorKubernetes {
				t.Fatalf("inventory error lost its Kubernetes category: %v", err)
			}

			if store.writes != writes || len(engine.requests) != 0 ||
				!reflect.DeepEqual(before, object) {
				t.Fatal("failed inventory changed execution state")
			}
		})
	}
}
