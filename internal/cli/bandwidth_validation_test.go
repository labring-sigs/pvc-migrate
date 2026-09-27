package cli

import (
	"context"
	"strings"
	"testing"
)

// Cross-cluster transfers execute rsync in this process, so a malformed rate
// must fail admission there exactly as it already does for copy and migrate.
func TestClusterFamiliesValidateBandwidthLimitAtAdmission(t *testing.T) {
	for root, sub := range map[string]string{
		"cluster-copy":    "cross",
		"cluster-migrate": "plan",
	} {
		command := NewRoot(Options{Version: "test", In: strings.NewReader("")})
		command.SetArgs([]string{
			root, sub, "--copy-bandwidth-limit=10x", "--kubeconfig=/nonexistent",
		})

		err := command.ExecuteContext(context.Background())
		if err == nil || !strings.Contains(err.Error(), "must be a rate") {
			t.Fatalf("%s: error=%v, want the bandwidth admission refusal", root, err)
		}
	}
}
