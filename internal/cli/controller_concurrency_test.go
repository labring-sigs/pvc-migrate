package cli

import (
	"context"
	"strings"
	"testing"
)

// TestControllerMaxConcurrentReconcilesFlagContract pins the concurrency flag
// surface: the default stays serial, --once refuses it because the one-shot
// run reconciles inline, and zero or negative workers are rejected before the
// manager starts.
func TestControllerMaxConcurrentReconcilesFlagContract(t *testing.T) {
	oneShot := NewRoot(Options{Version: "test", In: strings.NewReader("")})
	oneShot.SetArgs([]string{
		"controller", "--once", "--max-concurrent-reconciles=2",
		"--kubeconfig=/nonexistent",
	})

	err := oneShot.ExecuteContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not available with --once") {
		t.Fatalf("--once error=%v, want the --max-concurrent-reconciles refusal", err)
	}

	zero := NewRoot(Options{Version: "test", In: strings.NewReader("")})
	zero.SetArgs([]string{
		"controller", "--max-concurrent-reconciles=0", "--kubeconfig=/nonexistent",
	})

	zeroErr := zero.ExecuteContext(context.Background())
	if zeroErr == nil || !strings.Contains(zeroErr.Error(), "at least one reconcile worker") {
		t.Fatalf("zero error=%v, want the worker floor refusal", zeroErr)
	}

	serial := NewRoot(Options{Version: "test", In: strings.NewReader("")})
	serial.SetArgs([]string{"controller", "--kubeconfig=/nonexistent"})

	// The serial default must pass flag validation and fail later on the
	// unreachable kubeconfig, not on the concurrency flag.
	serialErr := serial.ExecuteContext(context.Background())
	if serialErr == nil || strings.Contains(serialErr.Error(), "concurrent") {
		t.Fatalf("default error=%v, want a kubeconfig failure without concurrency", serialErr)
	}
}

// The one-shot run binds no probes and derives its own namespace, so the two
// remaining daemon-only flags are refused instead of silently not applying.
func TestControllerOnceRefusesDaemonOnlyFlags(t *testing.T) {
	for _, flag := range []string{"--health-probe-bind-address=:9999", "--controller-namespace=ops"} {
		oneShot := NewRoot(Options{Version: "test", In: strings.NewReader("")})
		oneShot.SetArgs([]string{"controller", "--once", flag, "--kubeconfig=/nonexistent"})

		err := oneShot.ExecuteContext(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not available with --once") {
			t.Fatalf("%s: error=%v, want the --once refusal", flag, err)
		}
	}
}
