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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func namespacedReservationFixture(
	t *testing.T,
	intercept interceptor.Funcs,
) (*ReservationExecutor, *v1alpha1.Reservation, *concreteReservationReserver) {
	t.Helper()
	_, cluster, _, reserver := reservationExecutorFixture(t)
	object := &v1alpha1.Reservation{
		ObjectMeta: metav1.ObjectMeta{Name: "reserve", Namespace: "data", UID: "workflow"},
		Spec:       *cluster.Spec.ReservationSpec.DeepCopy(),
		Status: v1alpha1.ReservationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan:           cluster.Status.Plan.ReservationPlan.DeepCopy(),
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
		WithInterceptorFuncs(intercept).
		Build()

	store, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	object, err = store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	executor := NewReservationExecutor(
		fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "data"}}),
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		ReservationExecutorConfig{},
	)
	executor.reserver = reserver

	return executor, object, reserver
}

func TestNamespacedReservationRecoversPartialProvisioning(t *testing.T) {
	executor, object, reserver := namespacedReservationFixture(t, interceptor.Funcs{})
	before := object.Spec.DeepCopy()
	failure := errors.New("provisioner unavailable")
	reserver.reserve = func(name string, checkpoint *v1alpha1.ClusterVolumeReservationStatus) error {
		if checkpoint.DestinationPVC.Namespace != object.Namespace {
			t.Fatal("provisioning escaped workflow namespace")
		}

		if name == "b" {
			return failure
		}

		return nil
	}

	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Phase != domain.PhaseFailed || !object.Status.Volumes[0].Reserved ||
		object.Status.Volumes[1].DestinationPVC == nil || object.Status.Volumes[1].Reserved {
		t.Fatalf("partial resources were not checkpointed: %+v", object.Status)
	}

	reserver.reserve = nil
	reserver.calls = nil

	object.Status.Volumes[0], object.Status.Volumes[1] = object.Status.Volumes[1], object.Status.Volumes[0]
	if err := executor.store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseReserved ||
		!reflect.DeepEqual(reserver.calls, []string{"b"}) ||
		!reflect.DeepEqual(object.Spec, *before) {
		t.Fatalf(
			"resume did not preserve source identities and input: %+v; %v",
			object.Status,
			reserver.calls,
		)
	}
}

func TestNamespacedReservationCheckpointFailureRollsBackMemory(t *testing.T) {
	writes := 0
	failure := errors.New("status unavailable")

	executor, object, reserver := namespacedReservationFixture(t, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, client crclient.Client, subresource string, object crclient.Object, options ...crclient.SubResourceUpdateOption) error {
			writes++
			if writes == 2 {
				return failure
			}

			return client.SubResource(subresource).Update(ctx, object, options...)
		},
	})
	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	stored, err := executor.store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(object.Status.Volumes, stored.Status.Volumes) ||
		object.Status.Phase != stored.Status.Phase ||
		object.Status.Volumes[0].Reserved ||
		object.Status.Volumes[0].DestinationPVC != nil {
		t.Fatal("failed write retained uncommitted volume identity")
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(reserver.calls, []string{"a", "a", "b"}) {
		t.Fatalf("retry skipped uncheckpointed resource: %v", reserver.calls)
	}
}

func TestNamespacedReservationRejectsForeignResourceCheckpoint(t *testing.T) {
	executor, object, reserver := namespacedReservationFixture(t, interceptor.Funcs{})
	reserver.reserve = func(_ string, checkpoint *v1alpha1.ClusterVolumeReservationStatus) error {
		checkpoint.DestinationPVC.Namespace = "foreign"
		return nil
	}

	if err := executor.Run(t.Context(), object); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("foreign namespace accepted: %v", err)
	}

	if object.Status.Volumes[0].DestinationPVC != nil || object.Status.Volumes[0].Reserved {
		t.Fatal("foreign resource entered the local checkpoint")
	}
}

func TestNamespacedReservationCleanupRetainsMetadataWhenLeaseCleanupFails(t *testing.T) {
	executor, object, _ := namespacedReservationFixture(t, interceptor.Funcs{})
	object.Status.Plan = nil

	object.Status.Phase = domain.PhaseAborted
	if err := executor.store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	failure := errors.New("lease deletion failed")
	lock := &renameDeletionLock{deleteErr: failure}

	executor.locker = &fakeSessionLocker{lock: lock}
	if err := executor.Cleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{Finalize: true, DeleteSession: true},
	); !errors.Is(
		err,
		failure,
	) {
		t.Fatalf("error = %v", err)
	}

	if _, err := executor.store.Load(
		t.Context(),
		crclient.ObjectKeyFromObject(object),
	); err != nil {
		t.Fatalf("metadata was removed before lease cleanup succeeded: %v", err)
	}

	lock.deleteErr = nil

	if err := executor.Cleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{Finalize: true, DeleteSession: true},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := executor.store.Load(
		t.Context(),
		crclient.ObjectKeyFromObject(object),
	); !apierrors.IsNotFound(err) {
		t.Fatalf("metadata remained after lease cleanup recovered: %v", err)
	}
}
