package kube

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestCRDWorkflowOwnerFinderResolvesClusterWorkflow(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(
		runtime.NewScheme(),
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "migrate.sealos.io/v1alpha1",
			"kind":       "ClusterCopy",
			"metadata":   map[string]any{"name": "copy-1"},
			"spec":       map[string]any{"sessionNamespace": "tenant-a"},
			"status":     map[string]any{"phase": "Reserved", "resumeFrom": ""},
		}},
	)
	finder := NewCRDWorkflowOwnerFinder(client)

	owner, err := finder.Find(context.Background(), "copy-1", "tenant-a")
	if err != nil {
		t.Fatal(err)
	}

	wantResource, ok := domain.ControllerResourceForKind(domain.ControllerKindClusterCopy)
	if !ok || owner == nil || owner.Resource != wantResource {
		t.Fatalf("owner=%#v", owner)
	}

	if owner.SessionNamespace != "tenant-a" || owner.Phase != "Reserved" ||
		owner.Backend != SessionBackendCRD {
		t.Fatalf("owner=%#v", owner)
	}
}

func TestCRDWorkflowOwnerFinderDoesNotReadConfigMaps(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	finder := NewCRDWorkflowOwnerFinder(client)

	owner, err := finder.Find(context.Background(), "configmap-only", "tenant-a")
	if err != nil {
		t.Fatal(err)
	}

	if owner != nil {
		t.Fatalf("owner=%#v", owner)
	}
}
