package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type namespacedPodMigrationCheckpointStore struct {
	kube.WorkflowStore[*v1alpha1.PodMigration]
	client crclient.Client
	writes int
	failAt int
	err    error
}

func (s *namespacedPodMigrationCheckpointStore) Save(
	ctx context.Context,
	object *v1alpha1.PodMigration,
) error {
	s.writes++
	if s.writes == s.failAt {
		return s.err
	}

	return s.WorkflowStore.Save(ctx, object)
}

func namespacedPodMigrationFixture(
	t *testing.T,
	configure ...func(*v1alpha1.PodMigration),
) (*PodMigrationExecutor, *v1alpha1.PodMigration, *namespacedPodMigrationCheckpointStore, *concreteReservationReserver) {
	t.Helper()

	object := &v1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "data", UID: "workflow"},
		Spec: v1alpha1.PodMigrationSpec{
			Pod: v1alpha1.LocalResourceReference{Name: "workload", UID: "workload-uid"},
		},
		Status: v1alpha1.PodMigrationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.PodMigrationPlan{
				Workload: v1alpha1.WorkloadSpec{
					Adapter: v1alpha1.WorkloadStandalone,
					Pod: &v1alpha1.LocalResourceReference{
						Name: "workload",
						UID:  "workload-uid",
					},
				},
				ToolImage: "example/tool:v1",
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

	for _, apply := range configure {
		apply(object)
	}

	if object.Status.Plan != nil {
		object.Status.OriginalPodSnapshotHash = plannedPodSnapshotFixture(
			t,
			object.Namespace,
			&object.Status.Plan.Workload,
		)
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
		func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	object, err = backend.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	store := &namespacedPodMigrationCheckpointStore{WorkflowStore: backend, client: client}
	executor := NewPodMigrationExecutor(
		fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "data"}}),
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		&concreteCopyEngine{},
		PodMigrationExecutorConfig{
			Storage: MigrationExecutorConfig{Transfer: VolumeCopyConfig{Retries: 1}},
		},
	)
	reserver := &concreteReservationReserver{}
	executor.reserver = reserver
	executor.switcher = &scriptedSwitcher{client: executor.client}
	executor.now = func() time.Time { return time.Unix(100, 0).UTC() }

	return executor, object, store, reserver
}

func plannedPodSnapshotFixture(
	t *testing.T,
	namespace string,
	workload *v1alpha1.WorkloadSpec,
) string {
	t.Helper()

	if workload.Adapter != v1alpha1.WorkloadStandalone || workload.Pod == nil {
		return ""
	}

	if workload.OriginalObject == nil {
		raw, err := json.Marshal(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      workload.Pod.Name,
				UID:       workload.Pod.UID,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		workload.OriginalObject = &apiextensionsv1.JSON{Raw: raw}
	}

	return kube.PodSnapshotHash(workload.OriginalObject.Raw)
}

func TestNamespacedPodMigrationRejectsOutOfScopeReservation(t *testing.T) {
	executor, object, store, reserver := namespacedPodMigrationFixture(t)
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

func TestNamespacedPodMigrationReservationCheckpointFailureRetries(t *testing.T) {
	executor, object, store, reserver := namespacedPodMigrationFixture(t)
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
