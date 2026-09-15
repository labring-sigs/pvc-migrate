package kube

import (
	"context"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	crclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func mustTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}

	return scheme
}

func TestCheckWorkflowIdentityCollisionValidation(t *testing.T) {
	client := crclientfake.NewClientBuilder().WithScheme(mustTestScheme()).Build()
	kubeClient := kubernetesfake.NewSimpleClientset()

	if err := CheckWorkflowIdentityCollision(
		context.Background(), nil, kubeClient, nil, "id",
		domain.ControllerKindCopy, []string{"ns"}, true,
	); err == nil {
		t.Error("nil client should error")
	}

	if err := CheckWorkflowIdentityCollision(
		context.Background(), client, kubeClient, nil, "",
		domain.ControllerKindCopy, []string{"ns"}, true,
	); err == nil {
		t.Error("empty id should error")
	}

	if err := CheckWorkflowIdentityCollision(
		context.Background(), client, kubeClient, nil, "id",
		"", []string{"ns"}, true,
	); err == nil {
		t.Error("empty kind should error")
	}

	if err := CheckWorkflowIdentityCollision(
		context.Background(), client, kubeClient, nil, "id",
		domain.ControllerKindCopy, nil, true,
	); err == nil {
		t.Error("empty namespaces should error")
	}

	if err := CheckWorkflowIdentityCollision(
		context.Background(), client, kubeClient, nil, "id",
		domain.ControllerKindCopy, []string{"ns", ""}, true,
	); err == nil {
		t.Error("implicit namespace should error")
	}
}

func TestCheckWorkflowIdentityCollisionDetectsSameNameDifferentKind(t *testing.T) {
	scheme := mustTestScheme()
	client := crclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&v1alpha1.Migration{
			ObjectMeta: nsObjectMeta("shared-id", "tenant"),
		}).
		Build()
	kubeClient := kubernetesfake.NewSimpleClientset()

	err := CheckWorkflowIdentityCollision(
		context.Background(), client, kubeClient, nil, "shared-id",
		domain.ControllerKindCopy, []string{"tenant"}, true,
	)
	if err == nil {
		t.Fatal("same-name Migration must collide with a new Copy")
	}

	if !contains(err.Error(), "shared-id") {
		t.Errorf("error should name the colliding id: %v", err)
	}
}

func TestCheckWorkflowIdentityCollisionAllowsSameKindAbsence(t *testing.T) {
	client := crclientfake.NewClientBuilder().WithScheme(mustTestScheme()).Build()
	kubeClient := kubernetesfake.NewSimpleClientset()

	if err := CheckWorkflowIdentityCollision(
		context.Background(), client, kubeClient, nil, "fresh-id",
		domain.ControllerKindCopy, []string{"tenant"}, true,
	); err != nil {
		t.Fatalf("free identity should pass, got %v", err)
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}

func nsObjectMeta(name, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: namespace}
}
