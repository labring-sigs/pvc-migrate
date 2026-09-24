package controller

import (
	"context"
	"errors"
	"slices"
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

func migrationControllerFixture(
	t *testing.T,
	object *v1alpha1.ClusterMigration,
) (*WorkflowReconciler, crclient.WithWatch) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.ClusterMigration{}).
		WithObjects(object).
		Build()
	r := NewWorkflowReconciler().WithSupportedKinds([]domain.ControllerKind{domain.ControllerKindClusterMigration})

	options := ManagerOptions{
		KubernetesClient: fake.NewClientset(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		),
		MigrationPlanner: func(context.Context, *v1alpha1.ClusterMigration, string) (*domain.TransferPlan, error) {
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

	r.migration.checkCollision = func(context.Context, string, []string) error { return nil }

	return r, client
}

func TestMigrationControllerPlanningRetryUsesCRD(t *testing.T) {
	object := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", UID: "workflow", Generation: 1},
		Spec:       v1alpha1.ClusterMigrationSpec{SourceNamespace: "system"},
	}
	r, client := migrationControllerFixture(t, object)
	plans := 0
	r.migration.planner = func(_ context.Context, object *v1alpha1.ClusterMigration, image string) (*domain.TransferPlan, error) {
		plans++

		if image != "trusted/tool:v1" {
			t.Fatalf("untrusted image: %s", image)
		}

		object.Status.Plan = &v1alpha1.ClusterMigrationPlan{SourceNamespace: "invalid"}

		return nil, errors.New("source unavailable")
	}
	entry := &kindWorkflowReconciler{parent: r, kind: domain.ControllerKindClusterMigration}

	request := reconcile.Request{NamespacedName: crclient.ObjectKey{Name: object.Name}}
	for range 2 {
		if _, err := entry.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}

	loaded, err := r.migration.store.Load(t.Context(), request.NamespacedName)
	if err != nil {
		t.Fatal(err)
	}

	if plans != 1 || loaded.Status.Phase != domain.PhaseFailed || loaded.Status.Plan != nil {
		t.Fatalf("failed planning leaked or retried: %d %+v", plans, loaded.Status)
	}

	if err := r.migration.executor(loaded).RequestResume(t.Context(), loaded); err != nil {
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

	loaded.Spec.TemporaryNamespace = "corrected"
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

func TestMigrationControllerPersistsConcretePlan(t *testing.T) {
	object := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", UID: "workflow"},
		Spec:       v1alpha1.ClusterMigrationSpec{SourceNamespace: "system"},
	}
	r, _ := migrationControllerFixture(t, object)
	r.migration.planner = func(_ context.Context, object *v1alpha1.ClusterMigration, _ string) (*domain.TransferPlan, error) {
		object.Status.Plan = &v1alpha1.ClusterMigrationPlan{
			SourceNamespace:      "system",
			TemporaryNamespace:   "system",
			DestinationNamespace: "system",
			SessionNamespace:     "system",
		}

		return &domain.TransferPlan{Ready: true}, nil
	}
	entry := &kindWorkflowReconciler{parent: r, kind: domain.ControllerKindClusterMigration}
	request := reconcile.Request{NamespacedName: crclient.ObjectKey{Name: object.Name}}

	result, err := entry.Reconcile(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := r.migration.store.Load(t.Context(), request.NamespacedName)
	if err != nil {
		t.Fatal(err)
	}

	if result.RequeueAfter == 0 || loaded.Status.Plan == nil ||
		loaded.Status.Phase != domain.PhasePlanned {
		t.Fatalf("plan not persisted: %+v", loaded.Status)
	}
}

func TestMigrationControllerDeletesUnplannedCRDWithoutLegacyService(t *testing.T) {
	object := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration", UID: "workflow"},
		Spec:       v1alpha1.ClusterMigrationSpec{SourceNamespace: "system"},
	}

	r, client := migrationControllerFixture(t, object)
	if err := r.migration.store.EnsureProtection(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := client.Delete(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	entry := &kindWorkflowReconciler{parent: r, kind: domain.ControllerKindClusterMigration}

	request := reconcile.Request{NamespacedName: crclient.ObjectKey{Name: object.Name}}
	if _, err := entry.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	if _, err := r.migration.store.Load(
		t.Context(),
		request.NamespacedName,
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("CRD still exists: %v", err)
	}
}

func TestMigrationWorkflowNamespacesIncludeDestination(t *testing.T) {
	spec := v1alpha1.ClusterMigrationSpec{
		SourceNamespace:      "source",
		DestinationNamespace: "landing",
		TemporaryNamespace:   "temporary",
		SessionNamespace:     "sessions",
	}

	assertContains := func(namespaces []string) {
		t.Helper()

		if !slices.Contains(namespaces, "landing") {
			t.Fatalf("destination namespace missing from collision scope: %v", namespaces)
		}
	}

	assertContains(migrationWorkflowNamespaces(spec, nil))

	assertContains(migrationWorkflowNamespaces(
		v1alpha1.ClusterMigrationSpec{SourceNamespace: "source"},
		&v1alpha1.ClusterMigrationPlan{
			SourceNamespace:      "source",
			DestinationNamespace: "landing",
			TemporaryNamespace:   "source",
			SessionNamespace:     "source",
		},
	))
}
