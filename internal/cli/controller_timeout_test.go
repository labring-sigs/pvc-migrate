package cli

import (
	"context"
	"strings"
	"testing"
)

// TestControllerDaemonRefusesExplicitTimeout pins that the daemon never
// accepts --timeout: it bounds a single command run, the daemon runs until
// its process signal, and an accepted-but-unconsumed flag would also tighten
// the --copy-timeout validation against a deadline nothing enforces. The
// one-shot mode keeps the flag.
func TestControllerDaemonRefusesExplicitTimeout(t *testing.T) {
	root := NewRoot(Options{Version: "test", In: strings.NewReader("")})

	daemon := root
	daemon.SetArgs([]string{"controller", "--timeout=5m", "--kubeconfig=/nonexistent"})

	err := daemon.ExecuteContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "consumed by --once") {
		t.Fatalf("daemon error=%v, want --timeout refusal", err)
	}

	oneShot := NewRoot(Options{Version: "test", In: strings.NewReader("")})
	oneShot.SetArgs([]string{
		"controller", "--once", "--timeout=1ms", "--kubeconfig=/nonexistent",
	})

	oneShotErr := oneShot.ExecuteContext(context.Background())
	if oneShotErr == nil || strings.Contains(oneShotErr.Error(), "consumed by --once") {
		t.Fatalf("--once error=%v, must not be refused on --timeout", oneShotErr)
	}
}
