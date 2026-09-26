package cli

import (
	"context"
	"strings"
	"testing"
)

// TestPodMigrationPlanAcceptsRunFlags pins plan/run flag parity for the
// reprovision escape hatch: the shared convergence advice tells the operator
// to use --force-reprovision, and `migrate-pod plan` must accept the same
// flag the run command does instead of failing on an unknown name.
func TestPodMigrationPlanAcceptsRunFlags(t *testing.T) {
	root := NewRoot(Options{Version: "test", In: strings.NewReader("")})
	root.SetArgs([]string{
		"migrate-pod", "plan",
		"--pod", "app-0",
		"--force-reprovision",
		"--kubeconfig=/nonexistent",
	})

	err := root.ExecuteContext(context.Background())
	if err == nil {
		t.Fatal("expected the run to stop on the unreachable kubeconfig")
	}

	if strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("plan rejected a run flag: %v", err)
	}
}
