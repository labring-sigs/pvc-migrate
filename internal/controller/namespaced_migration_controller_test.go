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

func namespacedMigrationControllerFixture(
	t *testing.T,
	object *v1alpha1.Migration,
) (*WorkflowReconciler, crclient.WithWatch) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.Migration{}).
		WithObjects(object).
		Build()
	r := NewWorkflowReconciler().WithSupportedKinds([]domain.ControllerKind{domain.ControllerKindMigration})

	options := ManagerOptions{
		KubernetesClient: fake.NewClientset(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		),
		NamespacedMigrationPlanner: func(context.Context, *v1alpha1.Migration, string) (*domain.TransferPlan, error) {
			return nil, errors.New("source unavailable")
		},
	}
	if err := r.configureTransferControllers(
		client,
		options,
		&moveControllerLocker{},
		"trusted/tool:v1",
		nil,
	); err != nil {
		t.Fatal(err)
	}

	r.namespacedMigration.checkCollision = func(context.Context, string, []string) error { return nil }

	return r, client
}

func TestNamespacedMigrationControllerPlanningRetryUsesCRD(t *testing.T) {
	object := &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "migration",
			Namespace:  "system",
			UID:        "workflow",
			Generation: 1,
		},
		Spec: v1alpha1.MigrationSpec{},
	}
	r, client := namespacedMigrationControllerFixture(t, object)
	plans := 0
	r.namespacedMigration.planner = func(_ context.Context, object *v1alpha1.Migration, image string) (*domain.TransferPlan, error) {
		plans++

		if image != "trusted/tool:v1" {
			t.Fatalf("untrusted image: %s", image)
		}

		object.Status.Plan = &v1alpha1.MigrationPlan{TargetNode: "invalid"}

		return nil, errors.New("source unavailable")
	}
	entry := &kindWorkflowReconciler{parent: r, kind: domain.ControllerKindMigration}

	request := reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(object)}
	for range 2 {
		if _, err := entry.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}

	loaded, err := r.namespacedMigration.store.Load(t.Context(), request.NamespacedName)
	if err != nil {
		t.Fatal(err)
	}

	if plans != 1 || loaded.Status.Phase != domain.PhaseFailed || loaded.Status.Plan != nil {
		t.Fatalf("failed planning leaked or retried: %d %+v", plans, loaded.Status)
	}

	if err := r.namespacedMigration.executor().RequestResume(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if _, err := entry.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	if plans != 2 {
		t.Fatalf("explicit resume did not retry: %d", plans)
	}

	if err := client.Get(t.Context(), request.NamespacedName, loaded); err != nil {
		t.Fatal(err)
	}

	loaded.Generation++

	loaded.Spec.TargetNode = "corrected"
	if err := client.Update(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if _, err := entry.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	if plans != 3 {
		t.Fatalf("corrected spec did not retry: %d", plans)
	}
}

func TestNamespacedMigrationControllerPersistsConcretePlan(t *testing.T) {
	object := &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "system", UID: "workflow"},
		Spec:       v1alpha1.MigrationSpec{},
	}
	r, _ := namespacedMigrationControllerFixture(t, object)
	r.namespacedMigration.planner = func(_ context.Context, object *v1alpha1.Migration, _ string) (*domain.TransferPlan, error) {
		object.Status.Plan = &v1alpha1.MigrationPlan{
			TargetNode: "target",
		}

		return &domain.TransferPlan{Ready: true}, nil
	}
	entry := &kindWorkflowReconciler{parent: r, kind: domain.ControllerKindMigration}
	request := reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(object)}

	result, err := entry.Reconcile(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := r.namespacedMigration.store.Load(t.Context(), request.NamespacedName)
	if err != nil {
		t.Fatal(err)
	}

	if result.RequeueAfter == 0 || loaded.Status.Plan == nil ||
		loaded.Status.Phase != domain.PhasePlanned {
		t.Fatalf("plan not persisted: %+v", loaded.Status)
	}
}

func TestNamespacedMigrationControllerDeletesUnplannedCRDWithoutLegacyService(t *testing.T) {
	object := &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "system", UID: "workflow"},
		Spec:       v1alpha1.MigrationSpec{},
	}

	r, client := namespacedMigrationControllerFixture(t, object)
	if err := r.namespacedMigration.store.EnsureProtection(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := client.Delete(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	entry := &kindWorkflowReconciler{parent: r, kind: domain.ControllerKindMigration}

	request := reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(object)}
	if _, err := entry.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	if _, err := r.namespacedMigration.store.Load(
		t.Context(),
		request.NamespacedName,
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("CRD still exists: %v", err)
	}
}
