package kube

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNewWorkflowStoreForBackend(t *testing.T) {
	scheme := k8sruntime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	clients := &Clients{
		Kubernetes: kubefake.NewSimpleClientset(&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: "sessions"},
		}),
		Runtime: crfake.NewClientBuilder().WithScheme(scheme).Build(),
	}

	cmStore, err := NewWorkflowStoreForBackend(clients, BackendConfigMap, "sessions",
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} })
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := any(cmStore).(*ConfigMapWorkflowStore[*v1alpha1.Copy]); !ok {
		t.Fatalf("configmap backend produced %T", cmStore)
	}

	crdStore, err := NewWorkflowStoreForBackend(clients, BackendCRD, "sessions",
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} })
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := any(crdStore).(*CRDWorkflowStore[*v1alpha1.Copy]); !ok {
		t.Fatalf("crd backend produced %T", crdStore)
	}

	if _, err := NewWorkflowStoreForBackend[*v1alpha1.Copy](nil, BackendConfigMap, "sessions",
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} }); err == nil {
		t.Fatal("nil clients must be rejected")
	}
}
