package controller

import (
	"context"
	"errors"
	"sync"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func newRenameReconcileFixture(
	t *testing.T,
) (*RenameReconciler, crclient.Client, reconcile.Request) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	object := &v1alpha1.Rename{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "rename",
			Namespace:  "data",
			UID:        "workflow",
			Generation: 1,
		},
		Spec: v1alpha1.RenameSpec{
			SourcePVC:      v1alpha1.LocalResourceReference{Name: "source"},
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "target"},
		},
	}
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(object).
		WithObjects(object).
		Build()

	store, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	r := &RenameReconciler{
		store:  store,
		client: fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "data"}}),
		locker: &runnerSessionStore{lock: &runnerSessionLock{}}, active: &sync.Map{},
		checkCollision: func(context.Context, string, string) error { return nil },
	}

	return r, client, reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(object)}
}

func TestRenameReconcilePlanningFailureRequiresExplicitResume(t *testing.T) {
	r, _, request := newRenameReconcileFixture(t)
	plans := 0

	r.planner = func(context.Context, *v1alpha1.Rename, string) (*domain.PVCIdentityReport, error) {
		plans++
		return nil, errors.New("source is busy")
	}

	entry := &kindWorkflowReconciler{
		parent: &WorkflowReconciler{rename: r}, kind: domain.ControllerKindRename,
	}
	for range 2 {
		if _, err := entry.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}

	object, err := r.store.Load(t.Context(), request.NamespacedName)
	if err != nil {
		t.Fatal(err)
	}

	if plans != 1 || object.Status.Phase != domain.PhaseFailed || len(object.Finalizers) != 1 {
		t.Fatalf("planning failure not protected and suspended: plans=%d; %+v", plans, object)
	}

	executor := app.NewRenameExecutor(r.client, r.store, r.locker, object.Namespace)
	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	if plans != 2 {
		t.Fatal("explicit resume did not retry planning")
	}
}

func TestRenameReconcilePlanningSchedulesExecution(t *testing.T) {
	r, _, request := newRenameReconcileFixture(t)
	r.planner = func(ctx context.Context, object *v1alpha1.Rename, namespace string) (*domain.PVCIdentityReport, error) {
		stored, err := r.store.Load(ctx, request.NamespacedName)
		if err != nil || len(stored.Finalizers) != 1 || namespace != object.Namespace {
			t.Fatal("planner ran without protection or in the wrong namespace")
		}

		object.Status.Plan = &v1alpha1.RenamePlan{PVCIdentityFields: v1alpha1.PVCIdentityFields{
			SourcePVC:      v1alpha1.LocalResourceReference{Name: "source", UID: "source-uid"},
			SourcePV:       v1alpha1.LocalResourceReference{Name: "pv", UID: "pv-uid"},
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "target"},
			SourceTemplate: v1alpha1.PVCSourceTemplate{
				ReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			},
		}}

		return &domain.PVCIdentityReport{PlanSummary: domain.PlanSummary{Ready: true}}, nil
	}

	result, err := r.Reconcile(t.Context(), request)
	if err != nil || result.RequeueAfter == 0 {
		t.Fatalf("committed plan was not scheduled: %+v; %v", result, err)
	}

	stored, err := r.store.Load(t.Context(), request.NamespacedName)
	if err != nil || stored.Status.Plan == nil || stored.Status.ObservedGeneration != 1 {
		t.Fatalf("plan was not committed: %+v; %v", stored, err)
	}

	// The source disappears before execution. The shared executor must record
	// the failure without discarding the identities needed for recovery.
	if _, err := r.Reconcile(t.Context(), request); err == nil {
		t.Fatal("missing source PVC was accepted")
	}

	stored, err = r.store.Load(t.Context(), request.NamespacedName)
	if err != nil || stored.Status.Phase != domain.PhaseFailed ||
		stored.Status.Plan.SourcePVC.UID != "source-uid" {
		t.Fatalf("executor lost failed plan: %+v; %v", stored, err)
	}
}

func TestRenameReconcileUnplannedDeletionUsesConcreteExecutor(t *testing.T) {
	r, client, request := newRenameReconcileFixture(t)

	object, err := r.store.Load(t.Context(), request.NamespacedName)
	if err != nil {
		t.Fatal(err)
	}

	if err := r.store.EnsureProtection(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := client.Delete(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	if _, err := r.store.Load(t.Context(), request.NamespacedName); !apierrors.IsNotFound(err) {
		t.Fatalf("deletion protection was not released: %v", err)
	}
}
