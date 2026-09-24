package kube

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
)

// TestLoadConfigMapWorkflowDecodesRemovedKinds keeps upgrade recovery working:
// a session record written by an older binary may carry a kind this build
// removed, and ownership lookups must still summarize it instead of failing.
func TestLoadConfigMapWorkflowDecodesRemovedKinds(t *testing.T) {
	client := fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "pvc-migrate-system",
			Name:      SessionConfigMapName("legacy"),
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     "legacy",
			},
		},
		Data: map[string]string{
			SessionDataKey: `{"kind":"ClusterPodMigration","apiVersion":"migrate.sealos.io/v1alpha1",` +
				`"metadata":{"name":"legacy"},"spec":{},"status":{"phase":"Completed"}}`,
		},
	})

	object, err := LoadConfigMapWorkflow(
		t.Context(),
		client,
		"pvc-migrate-system",
		"legacy",
	)
	if err != nil {
		t.Fatalf("removed-kind record failed to load: %v", err)
	}

	generic, ok := object.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("record decoded as %T instead of unstructured", object)
	}

	if generic.GetKind() != "ClusterPodMigration" || generic.GetName() != "legacy" {
		t.Fatalf("record summary lost identity: %s/%s", generic.GetKind(), generic.GetName())
	}

	owner := configMapWorkflowOwner(
		generic,
		"legacy",
		"pvc-migrate-system",
		SessionBackendConfigMap,
	)
	if owner.ID != "legacy" || owner.Phase != "Completed" {
		t.Fatalf("ownership summary incomplete: %+v", owner)
	}
}
