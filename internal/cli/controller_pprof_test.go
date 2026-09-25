package cli

import (
	"context"
	"strings"
	"testing"
)

// TestControllerPprofPortFlagContract pins the profiling flag surface: the
// port stays optional and defaults to disabled, --once refuses it because the
// one-shot run never serves, and out-of-range ports are rejected before the
// manager starts.
func TestControllerPprofPortFlagContract(t *testing.T) {
	oneShot := NewRoot(Options{Version: "test", In: strings.NewReader("")})
	oneShot.SetArgs([]string{
		"controller", "--once", "--pprof-port=6060", "--kubeconfig=/nonexistent",
	})

	err := oneShot.ExecuteContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not available with --once") {
		t.Fatalf("--once error=%v, want the --pprof-port refusal", err)
	}

	outOfRange := NewRoot(Options{Version: "test", In: strings.NewReader("")})
	outOfRange.SetArgs([]string{
		"controller", "--pprof-port=65536", "--kubeconfig=/nonexistent",
	})

	rangeErr := outOfRange.ExecuteContext(context.Background())
	if rangeErr == nil || !strings.Contains(rangeErr.Error(), "must be between 0 and 65535") {
		t.Fatalf("range error=%v, want the port range refusal", rangeErr)
	}

	disabled := NewRoot(Options{Version: "test", In: strings.NewReader("")})
	disabled.SetArgs([]string{"controller", "--pprof-port=0", "--kubeconfig=/nonexistent"})

	// The disabled default must pass flag validation and fail later on the
	// unreachable kubeconfig, not on the profiling flag.
	disabledErr := disabled.ExecuteContext(context.Background())
	if disabledErr == nil || strings.Contains(disabledErr.Error(), "pprof") {
		t.Fatalf("disabled error=%v, want a kubeconfig failure without pprof", disabledErr)
	}
}
