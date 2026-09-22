package app

import (
	"context"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type namespacedMigrationCheckpointStore struct {
	kube.WorkflowStore[*v1alpha1.Migration]
	writes int
	failAt int
	err    error
}

func (s *namespacedMigrationCheckpointStore) Save(
	ctx context.Context,
	object *v1alpha1.Migration,
) error {
	s.writes++
	if s.writes == s.failAt {
		return s.err
	}

	return s.WorkflowStore.Save(ctx, object)
}

func namespacedMigrationFixture(
	t *testing.T,
) (*MigrationExecutor, *v1alpha1.Migration, *namespacedMigrationCheckpointStore, *concreteReservationReserver) {
	t.Helper()
	_, cluster, _, reserver := migrationExecutorFixture(t)
	object := &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "data", UID: "workflow"},
		Spec:       *cluster.Spec.MigrationSpec.DeepCopy(),
		Status: v1alpha1.MigrationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.MigrationPlan{
				Volumes:   cluster.Status.Plan.Volumes,
				ToolImage: "example/tool:v1",
			},
		},
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(object).
		WithObjects(object).
		Build()

	backend, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	object, err = backend.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	store := &namespacedMigrationCheckpointStore{WorkflowStore: backend}
	executor := NewMigrationExecutor(
		fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "data"}}),
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		&concreteCopyEngine{},
		MigrationExecutorConfig{Transfer: VolumeCopyConfig{Retries: 1}},
	)
	executor.reserver = reserver
	executor.switcher = &scriptedSwitcher{client: executor.client}
	executor.now = func() time.Time { return time.Unix(100, 0).UTC() }

	return executor, object, store, reserver
}

func TestNamespacedMigrationRejectsOutOfScopeReservation(t *testing.T) {
	executor, object, store, reserver := namespacedMigrationFixture(t)
	reserver.reserve = func(_ string, status *v1alpha1.ClusterVolumeReservationStatus) error {
		status.DestinationPVC.Namespace = "outside"
		return nil
	}

	if err := executor.Reserve(t.Context(), object); err == nil {
		t.Fatal("out-of-namespace resource was accepted")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Volumes[0].DestinationPVC != nil ||
		object.Status.Volumes[0].DestinationPVC != nil {
		t.Fatal("rejected reference leaked into progress")
	}
}

func TestNamespacedMigrationReservationCheckpointFailureRetries(t *testing.T) {
	executor, object, store, reserver := namespacedMigrationFixture(t)
	failure := errors.New("checkpoint rejected")

	store.failAt, store.err = 2, failure
	if err := executor.Reserve(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Volumes[0].Reserved || object.Status.Volumes[0].DestinationPVC != nil {
		t.Fatal("rejected checkpoint leaked")
	}

	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(reserver.calls) != 3 || object.Status.Phase != domain.PhaseReserved {
		t.Fatalf("reservation did not recover: %+v %v", object.Status, reserver.calls)
	}
}
