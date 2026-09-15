package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func namespacedCopyFixture(
	t *testing.T,
	intercept interceptor.Funcs,
) (*CopyExecutor, *v1alpha1.Copy, *concreteCopyEngine) {
	t.Helper()
	_, cluster, _, reserver := reservationExecutorFixture(t)
	object := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{Name: "reserve", Namespace: "data", UID: "workflow"},
		Spec:       v1alpha1.CopySpec{Volumes: cluster.Spec.Volumes},
		Status: v1alpha1.CopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.CopyPlan{
				Volumes:        cluster.Status.Plan.Volumes,
				ToolImage:      "example/tool:v1",
				Strategies:     []string{domain.StrategyMount},
				VerifyChecksum: true,
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
		WithInterceptorFuncs(intercept).
		Build()

	store, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	object, err = store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	engine := &concreteCopyEngine{}
	executor := NewCopyExecutor(
		fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "data"}}),
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		engine, CopyExecutorConfig{Transfer: VolumeCopyConfig{Retries: 1}},
	)
	executor.reserver = reserver

	return executor, object, engine
}

func TestNamespacedCopyResumesByIdentityWithinNamespace(t *testing.T) {
	executor, object, engine := namespacedCopyFixture(t, interceptor.Funcs{})
	before := object.DeepCopy()
	failure := errors.New("transport interrupted")
	engine.copy = func(request copyengine.Request) error {
		if request.Source.Namespace != object.Namespace ||
			request.Destination.Namespace != object.Namespace {
			t.Fatal("copy escaped metadata.namespace")
		}

		if request.Source.Name == "b" {
			return failure
		}

		return nil
	}

	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.Volumes[0].Sync.WarmCompletedAt == nil ||
		object.Status.Volumes[1].Sync.WarmCompletedAt != nil {
		t.Fatal("partial progress was lost")
	}

	object.Status.Volumes[0], object.Status.Volumes[1] = object.Status.Volumes[1], object.Status.Volumes[0]
	if err := executor.store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	engine.copy = nil

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseWarmCopied || len(engine.requests) != 3 ||
		engine.requests[2].Source.Name != "b" ||
		engine.requests[2].Attempt != 2 ||
		len(engine.cleanups) != 1 {
		t.Fatal("resume replayed completed work or reused an attempt")
	}

	if !reflect.DeepEqual(object.Spec, before.Spec) ||
		!reflect.DeepEqual(object.Status.Plan, before.Status.Plan) ||
		!engine.requests[0].VerifyChecksum {
		t.Fatal("copy changed input or lost checksum verification")
	}

	if err := executor.Run(t.Context(), object); err != nil || len(engine.requests) != 3 {
		t.Fatal("reconciliation repeated completed work")
	}

	if err := executor.RequestCopyPass(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(engine.requests) != 5 || engine.requests[3].Attempt != 2 ||
		engine.requests[4].Attempt != 3 {
		t.Fatal("explicit repeated pass lost attempt identities")
	}
}

func TestNamespacedCopyFailedAttemptSaveNeverStartsTransfer(t *testing.T) {
	failure := errors.New("status unavailable")
	reject := true

	executor, object, engine := namespacedCopyFixture(t, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, client crclient.Client, subresource string, object crclient.Object, options ...crclient.SubResourceUpdateOption) error {
			current, ok := object.(*v1alpha1.Copy)
			if !ok {
				t.Fatal("status write used a different workflow type")
			}

			if reject && len(current.Status.Volumes) != 0 &&
				current.Status.Volumes[0].Sync.Attempts != 0 {
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

	if len(engine.requests) != 0 || object.Status.Volumes[0].Sync.Attempts != 0 ||
		stored.Status.Volumes[0].Sync.Attempts != 0 {
		t.Fatal("uncommitted attempt reached engine or persisted")
	}

	reject = false

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(engine.requests) != 2 || engine.requests[0].Attempt != 1 {
		t.Fatal("retry lost first attempt")
	}
}

func TestNamespacedCopyRejectsForeignProvisioningCheckpoint(t *testing.T) {
	executor, object, engine := namespacedCopyFixture(t, interceptor.Funcs{})

	reserver, ok := executor.reserver.(*concreteReservationReserver)
	if !ok {
		t.Fatal("fixture did not install the reservation double")
	}

	reserver.reserve = func(_ string, checkpoint *v1alpha1.ClusterVolumeReservationStatus) error {
		checkpoint.DestinationPVC.Namespace = "foreign"
		return nil
	}

	if err := executor.Run(t.Context(), object); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Volumes[0].DestinationPVC != nil || len(engine.requests) != 0 {
		t.Fatal("foreign identity persisted or reached transfer")
	}
}
