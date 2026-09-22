package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type reservationCheckpointStore struct {
	object *v1alpha1.ClusterReservation
	writes int
	failAt int
	err    error
}

func (s *reservationCheckpointStore) Create(
	_ context.Context,
	object *v1alpha1.ClusterReservation,
) error {
	s.object = object.DeepCopy()
	return nil
}

func (s *reservationCheckpointStore) Load(
	context.Context,
	crclient.ObjectKey,
) (*v1alpha1.ClusterReservation, error) {
	return s.object.DeepCopy(), nil
}

func (s *reservationCheckpointStore) List(
	context.Context,
	string,
) ([]*v1alpha1.ClusterReservation, error) {
	return []*v1alpha1.ClusterReservation{s.object.DeepCopy()}, nil
}

func (s *reservationCheckpointStore) Save(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.writes++
	if s.writes == s.failAt {
		return s.err
	}

	s.object = object.DeepCopy()

	return nil
}

func (s *reservationCheckpointStore) Delete(context.Context, *v1alpha1.ClusterReservation) error {
	s.object = nil
	return nil
}

type concreteReservationReserver struct {
	calls    []string
	reserve  func(string, *v1alpha1.ClusterVolumeReservationStatus) error
	validate func(*corev1.PersistentVolumeClaim, v1alpha1.ClusterVolumeReservationStatus) error
}

func (r *concreteReservationReserver) ReserveVolume(
	_ context.Context, _ kube.ReservationRequest,
	source, _ v1alpha1.ObjectReference, _ string,
	desired *corev1.PersistentVolumeClaim, status *v1alpha1.ClusterVolumeReservationStatus,
) error {
	r.calls = append(r.calls, source.Name)

	status.DestinationPVC = &v1alpha1.ObjectReference{
		Namespace: desired.Namespace,
		Name:      desired.Name,
		UID:       "destination-pvc",
	}
	if r.reserve != nil {
		if err := r.reserve(source.Name, status); err != nil {
			return err
		}
	}

	status.DestinationPV = &v1alpha1.ObjectReference{
		Name: "destination-" + source.Name,
		UID:  "destination-pv",
	}
	status.DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	status.Reserved = true

	return nil
}

func (r *concreteReservationReserver) ValidateVolumeReservation(
	_ context.Context, _ kube.ReservationRequest,
	_, _ v1alpha1.ObjectReference, _ string,
	desired *corev1.PersistentVolumeClaim, status v1alpha1.ClusterVolumeReservationStatus,
) error {
	if r.validate != nil {
		return r.validate(desired, status)
	}

	return nil
}

func reservationExecutorFixture(
	t *testing.T,
) (*ClusterReservationExecutor, *v1alpha1.ClusterReservation, *reservationCheckpointStore, *concreteReservationReserver) {
	t.Helper()

	object := &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{Name: "reserve", UID: "workflow", ResourceVersion: "1"},
		Spec: v1alpha1.ClusterReservationSpec{
			SourceNamespace:      "source",
			DestinationNamespace: "destination",
			SessionNamespace:     "sessions",
		},
		Status: v1alpha1.ClusterReservationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.ClusterReservationPlan{
				SourceNamespace:      "source",
				DestinationNamespace: "destination",
				SessionNamespace:     "sessions",
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

	store := &reservationCheckpointStore{object: object.DeepCopy()}
	reserver := &concreteReservationReserver{}
	executor := NewClusterReservationExecutor(
		fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "destination"}}),
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"sessions",
		ReservationExecutorConfig{},
	)
	executor.reserver = reserver

	return executor, object, store, reserver
}

func TestReservationExecutorRecoversPartialVolumeWithoutReplanning(t *testing.T) {
	executor, object, store, reserver := reservationExecutorFixture(t)
	before := object.DeepCopy()
	failure := errors.New("provisioner unavailable")
	reserver.reserve = func(name string, _ *v1alpha1.ClusterVolumeReservationStatus) error {
		if name == "b" {
			return failure
		}
		return nil
	}

	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseReserving ||
		!object.Status.Volumes[0].Reserved ||
		object.Status.Volumes[1].Reserved ||
		object.Status.Volumes[1].DestinationPVC == nil {
		t.Fatalf("lost partial reservation: %+v", object.Status)
	}

	if !reflect.DeepEqual(store.object, object) {
		t.Fatal("partial checkpoint was not persisted")
	}

	reserver.reserve = nil
	// Status is keyed by source PVC, independently of plan order.
	object.Status.Volumes[0], object.Status.Volumes[1] = object.Status.Volumes[1], object.Status.Volumes[0]

	store.object = object.DeepCopy()
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseReserved ||
		!reflect.DeepEqual(reserver.calls, []string{"a", "b", "b"}) {
		t.Fatalf(
			"resume repeated completed work: phase=%s calls=%v",
			object.Status.Phase,
			reserver.calls,
		)
	}

	if !reflect.DeepEqual(object.Spec, before.Spec) ||
		!reflect.DeepEqual(object.Status.Plan, before.Status.Plan) {
		t.Fatal("execution mutated input or plan")
	}

	writes := store.writes

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes || len(reserver.calls) != 3 {
		t.Fatal("completed reservation repeated execution")
	}
}

func TestReservationExecutorRestoresCheckpointAfterSaveFailure(t *testing.T) {
	executor, object, store, reserver := reservationExecutorFixture(t)
	failure := errors.New("checkpoint unavailable")

	store.err, store.failAt = failure, 2
	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseReserving ||
		object.Status.Volumes[0].DestinationPVC != nil ||
		!reflect.DeepEqual(store.object, object) ||
		len(reserver.calls) != 1 {
		t.Fatalf("failed save advanced state: %+v", object.Status)
	}

	store.failAt = 0

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseReserved {
		t.Fatalf("phase=%s", object.Status.Phase)
	}
}

func TestReservationExecutorValidationOwnsItsReferences(t *testing.T) {
	executor, object, store, reserver := reservationExecutorFixture(t)
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.DeepCopy()
	writes := store.writes
	failure := errors.New("validation rejected")
	reserver.validate = func(pvc *corev1.PersistentVolumeClaim, status v1alpha1.ClusterVolumeReservationStatus) error {
		pvc.Spec.AccessModes[0] = corev1.ReadOnlyMany
		*pvc.Spec.StorageClassName = "changed"
		status.DestinationPVC.Name = "changed"
		status.DestinationPV.Name = "changed"

		return failure
	}

	if err := executor.Validate(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(object, before) || store.writes != writes {
		t.Fatal("validation mutated the workflow")
	}
}

func TestReservationExecutorRejectsForeignStateBeforeLock(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*v1alpha1.ClusterReservation)
	}{
		{"foreign phase", func(o *v1alpha1.ClusterReservation) { o.Status.Phase = domain.PhasePausing }},
		{"changed source namespace", func(o *v1alpha1.ClusterReservation) { o.Status.Plan.SourceNamespace = "elsewhere" }},
		{"changed source identity", func(o *v1alpha1.ClusterReservation) { o.Spec.Volumes[0].SourcePVC.UID = "replaced" }},
		{"missing checkpoint", func(o *v1alpha1.ClusterReservation) { o.Status.Phase = domain.PhaseReserved }},
		{"invalid capacity", func(o *v1alpha1.ClusterReservation) { o.Status.Plan.Volumes[0].SourceCapacity = "invalid" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor, object, store, reserver := reservationExecutorFixture(t)
			test.mutate(object)

			executor.locker = nil
			if err := executor.Run(
				t.Context(),
				object,
			); domain.CategoryOf(
				err,
			) != domain.ErrorValidation {
				t.Fatalf("error=%v", err)
			}

			if store.writes != 0 || len(reserver.calls) != 0 {
				t.Fatal("invalid reservation reached execution")
			}
		})
	}
}

func TestReservationExecutorStopsAfterFenceLoss(t *testing.T) {
	executor, object, store, reserver := reservationExecutorFixture(t)
	lock := &fakeSessionLock{}
	executor.locker = &fakeSessionLocker{lock: lock}
	lost := errors.New("lease lost")
	reserver.reserve = func(_ string, _ *v1alpha1.ClusterVolumeReservationStatus) error { lock.err = lost; return nil }

	if err := executor.Run(t.Context(), object); !errors.Is(err, lost) {
		t.Fatal(err)
	}

	if store.writes != 1 || len(reserver.calls) != 1 || object.Status.Volumes[0].Reserved ||
		object.Status.Volumes[0].DestinationPVC != nil {
		t.Fatalf("lost fence advanced checkpoint: %+v", object.Status)
	}
}
