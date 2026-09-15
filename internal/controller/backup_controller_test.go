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

func TestBackupControllerOwnsPlanningResumeAndDeletion(t *testing.T) {
	object := &v1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: "tenant", UID: "workflow"},
		Spec: v1alpha1.BackupSpec{
			SourcePVC:     v1alpha1.LocalResourceReference{Name: "source"},
			Name:          "daily",
			RepositoryRef: v1alpha1.LocalObjectReference{Name: "archive"},
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
		Build()
	r := NewWorkflowReconciler().WithSupportedKinds([]domain.ControllerKind{domain.ControllerKindBackup}).
		WithTrustedToolImage("trusted/tool:v1")
	locker := &moveControllerLocker{}
	plans := 0

	options := ManagerOptions{
		KubernetesClient: fake.NewClientset(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant"}},
		),
		BackupPlanner: func(_ context.Context, request *v1alpha1.Backup, image string) error {
			if request.Namespace != "tenant" || image != "trusted/tool:v1" {
				t.Fatal("planner lost namespace or trusted image")
			}

			plans++

			return errors.New("source unavailable")
		},
	}
	if err := r.configureBackupController(client, options, locker); err != nil {
		t.Fatal(err)
	}

	r.backup.checkCollision = func(_ context.Context, namespace, name string) error {
		if name != object.Name || namespace != object.Namespace {
			t.Fatal("collision check escaped operation identity")
		}
		return nil
	}
	entry := &kindWorkflowReconciler{parent: r, kind: domain.ControllerKindBackup}

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
		t.Fatal("failed planning did not suspend")
	}

	object.Spec.Name = "corrected"
	object.Generation++

	if err := client.Update(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if _, err := entry.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}

	if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
		t.Fatal(err)
	}

	if plans != 2 || object.Status.ObservedGeneration != object.Generation {
		t.Fatalf(
			"corrected intent was not observed exactly once: plans=%d status=%+v",
			plans,
			object.Status,
		)
	}

	executor := r.backup.executor(object.Namespace)
	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := r.reconcileInventory(t.Context(), client); err == nil || plans != 3 {
		t.Fatalf("one-shot did not retry requested resume: %v, plans=%d", err, plans)
	}

	if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
		t.Fatal(err)
	}

	if err := client.Delete(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := r.reconcileInventory(t.Context(), client); err != nil {
		t.Fatal(err)
	}

	if err := client.Get(t.Context(), request.NamespacedName, object); !apierrors.IsNotFound(err) {
		t.Fatalf("deletion did not converge: %v", err)
	}

	for _, namespace := range locker.namespaces {
		if namespace != "tenant" {
			t.Fatalf("lease escaped workflow namespace: %s", namespace)
		}
	}
}
