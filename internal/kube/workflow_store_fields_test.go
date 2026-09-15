package kube

import (
	"encoding/json"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestWorkflowStorageRejectsUnrelatedOperationFields(t *testing.T) {
	for _, field := range []string{"online", "precopyPasses", "switchoverCandidate", "openebsLvmEnableShared"} {
		t.Run(field, func(t *testing.T) {
			assertStoredFieldRejected(
				t,
				"Migration",
				field,
				func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
			)
			assertStoredFieldRejected(
				t,
				"Reservation",
				field,
				func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
			)
		})
	}

	assertStoredFieldRejected(
		t,
		"Migration",
		"pod",
		func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
	)
}

func assertStoredFieldRejected[T crclient.Object](
	t *testing.T,
	kind, field string,
	factory func() T,
) {
	t.Helper()

	data, err := json.Marshal(map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       kind,
		"metadata":   map[string]string{"name": "isolation", "namespace": "app"},
		"spec":       map[string]any{field: nil},
	})
	if err != nil {
		t.Fatal(err)
	}

	client := fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: SessionConfigMapName(
				"isolation",
			),
			Namespace: "sessions",
			Labels:    sessionLabels("isolation"),
		},
		Data: map[string]string{SessionDataKey: string(data)},
	})

	store, err := NewConfigMapWorkflowStore(client, "sessions", factory)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Load(t.Context(), crclient.ObjectKey{Name: "isolation", Namespace: "app"})
	if err == nil || !strings.Contains(err.Error(), `unknown field "`+field+`"`) {
		t.Fatalf("%s accepted unrelated field %s: %v", kind, field, err)
	}
}
