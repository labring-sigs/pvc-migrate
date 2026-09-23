package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type migrationCheckpointStore struct {
	kube.WorkflowStore[*v1alpha1.ClusterMigration]
	writes int
	failAt int
	err    error
}

func (s *migrationCheckpointStore) Save(
	ctx context.Context,
	object *v1alpha1.ClusterMigration,
) error {
	s.writes++
	if s.writes == s.failAt {
		return s.err
	}

	return s.WorkflowStore.Save(ctx, object)
}

func migrationExecutorFixture(
	t *testing.T,
) (*ClusterMigrationExecutor, *v1alpha1.ClusterMigration, *migrationCheckpointStore, *concreteReservationReserver) {
	t.Helper()

	return migrationExecutorFixtureWith(t, nil)
}

func migrationExecutorFixtureWith(
	t *testing.T,
	customize func(*v1alpha1.ClusterMigration),
) (*ClusterMigrationExecutor, *v1alpha1.ClusterMigration, *migrationCheckpointStore, *concreteReservationReserver) {
	t.Helper()

	object := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration"},
		Spec: v1alpha1.ClusterMigrationSpec{
			SourceNamespace:    "source",
			TemporaryNamespace: "temporary",
			SessionNamespace:   "sessions",
		},
		Status: v1alpha1.ClusterMigrationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.ClusterMigrationPlan{
				SourceNamespace:      "source",
				TemporaryNamespace:   "temporary",
				SessionNamespace:     "sessions",
				DestinationNamespace: "source",
				ToolImage:            "example/tool:v1",
			},
		},
	}
	for _, name := range []string{"a", "b"} {
		object.Spec.Volumes = append(
			object.Spec.Volumes,
			v1alpha1.VolumeRequest{SourcePVC: v1alpha1.LocalResourceReference{Name: name}},
		)
		object.Status.Plan.Volumes = append(object.Status.Plan.Volumes, v1alpha1.VolumeSpec{
			SourcePVC: v1alpha1.LocalResourceReference{
				Name: name,
				UID:  types.UID("source-" + name),
			},
			SourcePV: v1alpha1.LocalResourceReference{
				Name: "pv-" + name,
				UID:  types.UID("pv-" + name),
			},
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "reserved-" + name},
			SourceCapacity: "1Gi",
			Capacity:       "1Gi",
			StorageClass:   "storage",
			VolumeMode:     corev1.PersistentVolumeFilesystem,
			AccessModes:    []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		})
	}

	if customize != nil {
		customize(object)
	}

	client := fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "temporary"}})
	client.PrependReactor(
		"create",
		"configmaps",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(ktesting.CreateAction)
			if !ok {
				t.Fatalf("unexpected action: %T", action)
			}

			cm, ok := create.GetObject().(*corev1.ConfigMap)
			if !ok {
				t.Fatalf("unexpected created object: %T", create.GetObject())
			}

			cm.UID, cm.ResourceVersion = "workflow-record", "1"

			return false, nil, nil
		},
	)

	backend, err := kube.NewConfigMapWorkflowStore(
		client,
		"sessions",
		func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := backend.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	store := &migrationCheckpointStore{WorkflowStore: backend}
	reserver := &concreteReservationReserver{}
	executor := NewClusterMigrationExecutor(
		client,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"sessions",
		nil,
		MigrationExecutorConfig{},
	)
	executor.now = func() time.Time { return time.Unix(100, 0).UTC() }
	executor.reserver = reserver

	return executor, object, store, reserver
}

func TestMigrationReservationUsesCRDCheckpointsWithoutChangingPlan(t *testing.T) {
	executor, object, store, reserver := migrationExecutorFixture(t)
	before := object.DeepCopy()
	failure := errors.New("provisioner interrupted")
	reserver.reserve = func(name string, _ *v1alpha1.ClusterVolumeReservationStatus) error {
		if name == "b" {
			return failure
		}
		return nil
	}

	if err := executor.Reserve(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseReserving ||
		!object.Status.Volumes[0].Reserved ||
		object.Status.Volumes[1].Reserved ||
		object.Status.Volumes[1].DestinationPVC == nil {
		t.Fatalf("lost partial reservation: %+v", object.Status)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKey{Name: object.Name})
	if err != nil {
		t.Fatal(err)
	}

	if !apiequality.Semantic.DeepEqual(loaded.Status, object.Status) {
		t.Fatalf(
			"partial checkpoint was not persisted: want=%+v got=%+v",
			object.Status,
			loaded.Status,
		)
	}

	reserver.reserve = nil

	loaded.Status.Volumes[0], loaded.Status.Volumes[1] = loaded.Status.Volumes[1], loaded.Status.Volumes[0]
	if err := store.Save(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if err := executor.Reserve(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseReserved ||
		!reflect.DeepEqual(reserver.calls, []string{"a", "b", "b"}) {
		t.Fatalf("completed reservation was repeated: %+v calls=%v", loaded.Status, reserver.calls)
	}

	if !reflect.DeepEqual(loaded.Spec, before.Spec) ||
		!reflect.DeepEqual(loaded.Status.Plan, before.Status.Plan) {
		t.Fatal("reservation mutated the submitted spec or execution plan")
	}

	writes := store.writes

	if err := executor.Reserve(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes {
		t.Fatal("completed reservation rewrote state")
	}
}

func TestMigrationReservationSaveFailureDoesNotPublishResourceIdentities(t *testing.T) {
	executor, object, store, reserver := migrationExecutorFixture(t)
	failure := errors.New("checkpoint unavailable")

	store.err, store.failAt = failure, 2
	if err := executor.Reserve(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Volumes[0].DestinationPVC != nil || object.Status.Volumes[0].Reserved ||
		len(reserver.calls) != 1 {
		t.Fatalf("failed save advanced reservation: %+v", object.Status)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKey{Name: object.Name})
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Volumes[0].DestinationPVC != nil || loaded.Status.Volumes[0].Reserved {
		t.Fatal("failure transition published the rejected checkpoint")
	}

	if err := executor.Reserve(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseReserved ||
		!reflect.DeepEqual(reserver.calls, []string{"a", "a", "b"}) {
		t.Fatalf(
			"retry skipped rejected checkpoint: phase=%s calls=%v",
			loaded.Status.Phase,
			reserver.calls,
		)
	}
}

func TestMigrationReservationRejectsUnrelatedStateBeforeLock(t *testing.T) {
	for _, mutate := range []func(*v1alpha1.ClusterMigration){
		func(o *v1alpha1.ClusterMigration) { o.Status.Phase = domain.PhaseWarmCopying },
		func(o *v1alpha1.ClusterMigration) { o.Status.ResumeFrom = domain.PhasePausing },
		func(o *v1alpha1.ClusterMigration) { o.Status.Plan.TemporaryNamespace = "foreign" },
		func(o *v1alpha1.ClusterMigration) { o.Spec.Volumes[0].SourcePVC.UID = "replaced" },
		func(o *v1alpha1.ClusterMigration) { o.Status.Phase = domain.PhaseReserved },
	} {
		executor, object, store, reserver := migrationExecutorFixture(t)
		mutate(object)

		executor.locker = nil
		if err := executor.Reserve(
			t.Context(),
			object,
		); domain.CategoryOf(
			err,
		) != domain.ErrorValidation {
			t.Fatalf("invalid migration reached execution: %v", err)
		}

		if store.writes != 0 || len(reserver.calls) != 0 {
			t.Fatal("invalid migration mutated resources")
		}
	}
}

func TestMigrationReservationStopsAfterFenceLoss(t *testing.T) {
	executor, object, store, reserver := migrationExecutorFixture(t)
	lock := &fakeSessionLock{}
	executor.locker = &fakeSessionLocker{lock: lock}
	lost := errors.New("lease lost")
	reserver.reserve = func(string, *v1alpha1.ClusterVolumeReservationStatus) error { lock.err = lost; return nil }

	if err := executor.Reserve(t.Context(), object); !errors.Is(err, lost) {
		t.Fatal(err)
	}

	if store.writes != 1 || len(reserver.calls) != 1 || object.Status.Volumes[0].Reserved ||
		object.Status.Volumes[0].DestinationPVC != nil {
		t.Fatalf("lost fence advanced state: %+v", object.Status)
	}
}
