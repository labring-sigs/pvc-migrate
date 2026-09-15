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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func namespacedReservationCleanupFixture(
	t *testing.T,
	intercept interceptor.Funcs,
) (*ReservationExecutor, *v1alpha1.Reservation, *fake.Clientset) {
	t.Helper()
	_, cluster, _, sourceClient := reservationCleanupFixture(t)

	object := &v1alpha1.Reservation{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Name, Namespace: "data", UID: cluster.UID},
		Spec:       *cluster.Spec.ReservationSpec.DeepCopy(),
		Status: v1alpha1.ReservationStatus{
			WorkflowStatus: *cluster.Status.WorkflowStatus.DeepCopy(),
			Plan:           cluster.Status.Plan.ReservationPlan.DeepCopy(),
		},
	}
	for _, volume := range cluster.Status.Volumes {
		checkpoint := *volume.ClusterVolumeReservationStatus.DeepCopy()
		checkpoint.DestinationPVC.Namespace = "data"

		local, err := localReservationCheckpoint(checkpoint, "data")
		if err != nil {
			t.Fatal(err)
		}

		object.Status.Volumes = append(
			object.Status.Volumes,
			v1alpha1.ReservationVolumeStatus{VolumeReservationStatus: local},
		)
	}

	object.Status.Phase = domain.PhaseAborted
	object.Status.Volumes[0].DestinationPV = nil
	object.Status.Volumes[0].Reserved = false

	var resources []runtime.Object
	for _, namespace := range []string{"source", "destination"} {
		claims, err := sourceClient.CoreV1().
			PersistentVolumeClaims(namespace).
			List(t.Context(), metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}

		for _, claim := range claims.Items {
			claim.Namespace = "data"
			resources = append(resources, claim.DeepCopy())
		}
	}

	volumes, err := sourceClient.CoreV1().
		PersistentVolumes().
		List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes.Items {
		volume.Spec.ClaimRef.Namespace = "data"
		resources = append(resources, volume.DeepCopy())
	}

	client := fake.NewClientset(resources...)

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	api := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(object).
		WithObjects(object).WithInterceptorFuncs(intercept).Build()

	store, err := kube.NewCRDWorkflowStore(
		api,
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
		client,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		ReservationExecutorConfig{},
	)

	return executor, object, client
}

func TestNamespacedReservationCleanupRecoveryAndFinalization(t *testing.T) {
	executor, object, client := namespacedReservationCleanupFixture(t, interceptor.Funcs{})
	before := object.DeepCopy()

	options := ReservationCleanupOptions{Finalize: true}
	if err := executor.ValidateCleanup(t.Context(), object, options); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("cleanup preview mutated the workflow")
	}

	assertReservationCleanupReadOnly(t, client)

	if err := executor.Cleanup(t.Context(), object, options); err != nil {
		t.Fatal(err)
	}

	stored, err := executor.store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil || stored.Status.Volumes[0].DestinationPV == nil {
		t.Fatalf("recovered PV not persisted: %v", err)
	}

	if err := executor.Cleanup(t.Context(), object, options); err != nil {
		t.Fatalf("finalization retry failed: %v", err)
	}

	for _, volume := range object.Status.Plan.Volumes {
		claim, err := client.CoreV1().
			PersistentVolumeClaims(object.Namespace).
			Get(t.Context(), volume.SourcePVC.Name, metav1.GetOptions{})
		if err != nil || claim.UID != volume.SourcePVC.UID ||
			claim.Annotations[kube.SessionKey] != "" {
			t.Fatalf("source PVC was not retained and released: %+v; %v", claim, err)
		}

		pv, err := client.CoreV1().
			PersistentVolumes().
			Get(t.Context(), volume.SourcePV.Name, metav1.GetOptions{})
		if err != nil ||
			pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
			t.Fatalf("source PV policy was not restored: %+v; %v", pv, err)
		}
	}
}

func TestNamespacedReservationCleanupCheckpointFailurePreventsResourceWrites(t *testing.T) {
	failure := errors.New("status write failed")
	executor, object, client := namespacedReservationCleanupFixture(t, interceptor.Funcs{
		SubResourceUpdate: func(context.Context, crclient.Client, string, crclient.Object, ...crclient.SubResourceUpdateOption) error {
			return failure
		},
	})

	before := object.DeepCopy()
	if err := executor.Cleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{Finalize: true},
	); !errors.Is(
		err,
		failure,
	) {
		t.Fatalf("error = %v", err)
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("failed checkpoint leaked recovery state")
	}

	assertReservationCleanupReadOnly(t, client)
}
