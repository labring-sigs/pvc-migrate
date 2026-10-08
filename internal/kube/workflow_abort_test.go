package kube

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func abortRequestTestStore(
	t *testing.T,
	object *v1alpha1.Copy,
) (*CRDWorkflowStore[*v1alpha1.Copy], crclient.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(object).
		WithObjects(object).
		Build()

	store, err := NewCRDWorkflowStore(
		client,
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	return store, client
}

func TestSetAbortRequestRecordsAndConsumes(t *testing.T) {
	object := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "copy",
			Namespace:   "tenant",
			UID:         "workflow",
			Annotations: map[string]string{"unrelated": "keep"},
		},
	}
	store, client := abortRequestTestStore(t, object)

	if err := store.SetAbortRequest(t.Context(), object, "2026-10-08T00:00:00Z"); err != nil {
		t.Fatal(err)
	}

	persisted := &v1alpha1.Copy{}
	if err := client.Get(
		t.Context(),
		crclient.ObjectKeyFromObject(object),
		persisted,
	); err != nil {
		t.Fatal(err)
	}

	if persisted.Annotations[WorkflowAbortRequestedAnnotation] != "2026-10-08T00:00:00Z" {
		t.Fatalf("abort request was not recorded: %v", persisted.Annotations)
	}

	if persisted.Annotations["unrelated"] != "keep" {
		t.Fatal("abort request dropped unrelated annotations")
	}

	if err := store.SetAbortRequest(t.Context(), object, ""); err != nil {
		t.Fatal(err)
	}

	if err := client.Get(
		t.Context(),
		crclient.ObjectKeyFromObject(object),
		persisted,
	); err != nil {
		t.Fatal(err)
	}

	if WorkflowAbortRequested(persisted) {
		t.Fatal("abort request was not consumed")
	}

	if persisted.Annotations["unrelated"] != "keep" {
		t.Fatal("consuming dropped unrelated annotations")
	}
}

func TestSetAbortRequestRejectsDeletingWorkflow(t *testing.T) {
	deleting := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "copy",
			Namespace:         "tenant",
			UID:               "workflow",
			DeletionTimestamp: &metav1.Time{},
			Finalizers:        []string{"migrate.sealos.io/session"},
		},
	}
	store, _ := abortRequestTestStore(t, deleting)

	if err := store.SetAbortRequest(t.Context(), deleting, "2026-10-08T00:00:00Z"); err == nil {
		t.Fatal("abort request was recorded on a deleting workflow")
	}
}
