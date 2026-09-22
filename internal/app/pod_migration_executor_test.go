package app

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type podMigrationCheckpointStore struct {
	kube.WorkflowStore[*v1alpha1.ClusterPodMigration]
	writes int
	failAt int
	err    error
}

func (s *podMigrationCheckpointStore) Save(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
) error {
	s.writes++
	if s.writes == s.failAt {
		return s.err
	}

	return s.WorkflowStore.Save(ctx, object)
}

func podMigrationExecutorFixture(
	t *testing.T,
	configure ...func(*v1alpha1.ClusterPodMigration),
) (*ClusterPodMigrationExecutor, *v1alpha1.ClusterPodMigration, *podMigrationCheckpointStore, *concreteReservationReserver) {
	t.Helper()

	object := &v1alpha1.ClusterPodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration"},
		Spec: v1alpha1.ClusterPodMigrationSpec{
			PodMigrationSpec: v1alpha1.PodMigrationSpec{
				Pod: v1alpha1.LocalResourceReference{Name: "workload", UID: "workload-uid"},
			},
			SourceNamespace:    "source",
			TemporaryNamespace: "temporary",
			SessionNamespace:   "sessions",
		},
		Status: v1alpha1.ClusterPodMigrationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.ClusterPodMigrationPlan{
				Workload: v1alpha1.WorkloadSpec{
					Adapter: v1alpha1.WorkloadStandalone,
					Pod: &v1alpha1.LocalResourceReference{
						Name: "workload",
						UID:  "workload-uid",
					},
				},
				SourceNamespace:    "source",
				TemporaryNamespace: "temporary",
				SessionNamespace:   "sessions",
				ToolImage:          "example/tool:v1",
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
		object.Status.OriginalPodSnapshotHash = plannedPodSnapshotFixture(t,
			string(object.Status.Plan.SourceNamespace), &object.Status.Plan.Workload)
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
		func() *v1alpha1.ClusterPodMigration { return &v1alpha1.ClusterPodMigration{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := backend.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	store := &podMigrationCheckpointStore{WorkflowStore: backend}
	reserver := &concreteReservationReserver{}
	executor := NewClusterPodMigrationExecutor(
		client,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"sessions",
		nil,
		PodMigrationExecutorConfig{},
	)
	executor.now = func() time.Time { return time.Unix(100, 0).UTC() }
	executor.reserver = reserver

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

func TestPodMigrationReservationUsesCRDCheckpointsWithoutChangingPlan(t *testing.T) {
	executor, object, store, reserver := podMigrationExecutorFixture(t)
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

func TestPodMigrationReservationSaveFailureDoesNotPublishResourceIdentities(t *testing.T) {
	executor, object, store, reserver := podMigrationExecutorFixture(t)
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

func TestPodMigrationReservationRejectsUnrelatedStateBeforeLock(t *testing.T) {
	for _, mutate := range []func(*v1alpha1.ClusterPodMigration){
		func(o *v1alpha1.ClusterPodMigration) { o.Status.Phase = "Unknown" },
		func(o *v1alpha1.ClusterPodMigration) { o.Status.ResumeFrom = "Unknown" },
		func(o *v1alpha1.ClusterPodMigration) { o.Status.Plan.TemporaryNamespace = "foreign" },
		func(o *v1alpha1.ClusterPodMigration) { o.Spec.Volumes[0].SourcePVC.UID = "replaced" },
		func(o *v1alpha1.ClusterPodMigration) { o.Status.Phase = domain.PhaseReserved },
	} {
		executor, object, store, reserver := podMigrationExecutorFixture(t)
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

func TestPodMigrationReservationStopsAfterFenceLoss(t *testing.T) {
	executor, object, store, reserver := podMigrationExecutorFixture(t)
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

func TestPodMigrationReservationInitialSaveFailurePreservesStatus(t *testing.T) {
	executor, object, store, reserver := podMigrationExecutorFixture(t)
	before := object.Status.DeepCopy()
	failure := errors.New("initial checkpoint rejected")

	store.failAt, store.err = 1, failure
	if err := executor.Reserve(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if !apiequality.Semantic.DeepEqual(*before, object.Status) || len(reserver.calls) != 0 {
		t.Fatalf("failed initial checkpoint changed state or storage: %+v", object.Status)
	}
}

func TestPodMigrationReservationPreservesZeroPrecopyAndDiscoveredVolumes(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(
		t,
		func(o *v1alpha1.ClusterPodMigration) { o.Spec.Volumes = nil },
	)
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Spec.PrecopyPasses != 0 || loaded.Status.Plan.PrecopyPasses != 0 ||
		len(loaded.Spec.Volumes) != 0 || len(loaded.Status.Volumes) != 2 {
		t.Fatalf("discovered volume inventory or zero precopy changed: %+v", loaded)
	}
}

func TestPodMigrationRejectsInvalidPlanBeforeResources(t *testing.T) {
	tests := map[string]func(*v1alpha1.ClusterPodMigration){
		"negative precopy":            func(o *v1alpha1.ClusterPodMigration) { o.Spec.PrecopyPasses = -1 },
		"changed precopy":             func(o *v1alpha1.ClusterPodMigration) { o.Status.Plan.PrecopyPasses = 1 },
		"negative completed passes":   func(o *v1alpha1.ClusterPodMigration) { o.Status.WarmPassesCompleted = -1 },
		"replaced Pod":                func(o *v1alpha1.ClusterPodMigration) { o.Status.Plan.Workload.Pod.UID = "replacement" },
		"missing Pod":                 func(o *v1alpha1.ClusterPodMigration) { o.Status.Plan.Workload.Pod = nil },
		"unrelated workload settings": func(o *v1alpha1.ClusterPodMigration) { o.Status.Plan.Workload.Grafana = &v1alpha1.GrafanaSpec{} },
		"unknown workload":            func(o *v1alpha1.ClusterPodMigration) { o.Status.Plan.Workload.Adapter = "Unsupported" },
		"missing controller UID":      func(o *v1alpha1.ClusterPodMigration) { o.Status.Plan.Workload.Adapter = v1alpha1.WorkloadStatefulSet },
		"unknown volume override":     func(o *v1alpha1.ClusterPodMigration) { o.Spec.Volumes[0].SourcePVC.Name = "unknown" },
		"duplicate volume override":   func(o *v1alpha1.ClusterPodMigration) { o.Spec.Volumes[1] = o.Spec.Volumes[0] },
		"foreign workload checkpoint": func(o *v1alpha1.ClusterPodMigration) {
			o.Status.Workload = &v1alpha1.ClusterPodMigrationWorkloadStatus{
				Pod: &v1alpha1.ObjectReference{Name: "pod", UID: "uid", Namespace: "foreign"},
			}
		},
		"orphan workload checkpoint": func(o *v1alpha1.ClusterPodMigration) {
			o.Status.Plan = nil
			o.Status.Workload = &v1alpha1.ClusterPodMigrationWorkloadStatus{}
		},
		"orphan shared mount checkpoint": func(o *v1alpha1.ClusterPodMigration) {
			o.Status.Plan = nil
			o.Status.OpenEBSLVMSharedMounts = []v1alpha1.SharedMountStatus{{}}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			executor, object, store, reserver := podMigrationExecutorFixture(t)
			mutate(object)

			executor.locker = nil
			if err := executor.Reserve(
				t.Context(),
				object,
			); domain.CategoryOf(
				err,
			) != domain.ErrorValidation {
				t.Fatalf("invalid pod migration reached the lock or resources: %v", err)
			}

			if store.writes != 0 || len(reserver.calls) != 0 {
				t.Fatal("invalid plan caused side effects")
			}
		})
	}
}
