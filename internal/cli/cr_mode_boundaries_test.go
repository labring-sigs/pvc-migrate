package cli

import (
	"bytes"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestCRCopyCreateRejectsConfigMapSessionGraduation pins the mode boundary:
// a cr submission graduates controller-owned reservations only. When no
// Reservation CR exists, the command must refuse instead of falling back to
// ConfigMap session records, which the session copy command owns.
func TestCRCopyCreateRejectsConfigMapSessionGraduation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	var out, diagnostics bytes.Buffer

	root := NewRoot(Options{
		Out: &out, ErrOut: &diagnostics,
		runtimeFactory: func(state *rootState) (*commandRuntime, error) {
			return &commandRuntime{
				clients: &kube.Clients{
					Runtime:    crfake.NewClientBuilder().WithScheme(scheme).Build(),
					Kubernetes: kubernetesfake.NewClientset(),
				},
				printer:           printerFor(state),
				waitForController: false,
			}, nil
		},
	})
	root.SetArgs([]string{
		"cr", "copy", "create",
		"--session", "missing-reservation",
		"-n", "app",
	})

	err := root.ExecuteContext(t.Context())
	if err == nil {
		t.Fatal("graduation without a Reservation CR must fail")
	}

	message := err.Error() + " " + diagnostics.String()
	if !strings.Contains(message, "graduated by the session copy command") {
		t.Fatalf(
			"error must redirect ConfigMap sessions to the session command: %v; %s",
			err,
			diagnostics.String(),
		)
	}

	if !strings.Contains(message, "missing-reservation") || !strings.Contains(message, "app") {
		t.Fatalf("error must name the reservation and namespace: %v; %s", err, diagnostics.String())
	}
}
