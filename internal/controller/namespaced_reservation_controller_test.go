package controller

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestNamespacedReservationControllerOwnsPlanningResumeAndDeletion(t *testing.T) {
	object := &v1alpha1.Reservation{
		ObjectMeta: metav1.ObjectMeta{Name: "reserve", Namespace: "tenant", UID: "workflow"},
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
	r := NewWorkflowReconciler().WithSupportedKinds([]domain.ControllerKind{domain.ControllerKindReservation})
	locker := &moveControllerLocker{}
	plans := 0

	options := ManagerOptions{
		KubernetesClient: fake.NewClientset(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant"}},
		),
		NamespacedReservationPlanner: func(_ context.Context, request *v1alpha1.Reservation, image string) (*domain.TransferPlan, error) {
			if request.Namespace != "tenant" || image != "trusted/tool:v1" {
				t.Fatal("planner lost namespace or administrator image")
			}

			plans++

			return nil, errors.New("source unavailable")
		},
	}
	if err := r.configureTransferControllers(
		client,
		options,
		locker,
		"trusted/tool:v1",
		nil,
	); err != nil {
		t.Fatal(err)
	}

	r.namespacedReservation.checkCollision = func(_ context.Context, name string, namespaces []string) error {
		if name != object.Name || len(namespaces) != 1 || namespaces[0] != object.Namespace {
			t.Fatal("collision check lost the namespaced boundary")
		}
		return nil
	}
	entry := &kindWorkflowReconciler{parent: r, kind: domain.ControllerKindReservation}

	request := reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(object)}
	for range 2 {
		if _, err := entry.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}

	if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
		t.Fatal(err)
	}

	if plans != 1 || object.Status.Phase != domain.PhaseFailed {
		t.Fatal("failed discovery was not suspended")
	}

	executor := r.namespacedReservation.executor()
	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := r.reconcileInventory(t.Context(), client); err == nil || plans != 2 {
		t.Fatalf("one-shot did not retry explicit resume: %v; %d", err, plans)
	}

	if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := client.Delete(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := r.reconcileInventory(t.Context(), client); err != nil {
		t.Fatal(err)
	}

	if err := client.Get(t.Context(), request.NamespacedName, object); !apierrors.IsNotFound(err) {
		t.Fatalf("one-shot did not finalize deletion: %v", err)
	}

	for _, namespace := range locker.namespaces {
		if namespace != "tenant" {
			t.Fatalf("lease escaped namespaced workflow: %v", locker.namespaces)
		}
	}
}
