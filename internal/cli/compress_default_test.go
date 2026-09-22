package cli

import (
	"testing"

	"github.com/spf13/cobra"
)

// TestCrossClusterCompressDefault pins the compression policy default:
// compression is opt-in elsewhere, but an unset flag on a cross-cluster
// command means enabled — the WAN hop pays for itself.
func TestCrossClusterCompressDefault(t *testing.T) {
	root := NewRoot(Options{Version: "test"})

	// Unset flag: default on for cross-cluster.
	if got := crossClusterCompressDefault(root.Commands()[0], false); !got {
		t.Fatal("unset flag must default to compression on")
	}

	// The helper never sees a nil command in production, but a nil-safe
	// default keeps early error paths on the safe side.
	if got := crossClusterCompressDefault(nil, false); !got {
		t.Fatal("nil command must default to compression on")
	}

	// An explicitly set flag is respected in both directions.
	withExplicit := &cobra.Command{}
	withExplicit.Flags().Bool("compress", false, "")

	if err := withExplicit.Flags().Set("compress", "false"); err != nil {
		t.Fatal(err)
	}

	if got := crossClusterCompressDefault(withExplicit, false); got {
		t.Fatal("explicit --compress=false must disable compression")
	}

	if err := withExplicit.Flags().Set("compress", "true"); err != nil {
		t.Fatal(err)
	}

	if got := crossClusterCompressDefault(withExplicit, true); !got {
		t.Fatal("explicit --compress must enable compression")
	}
}

// TestGlobalCompressFlagDefaultsOff pins the CLI flag surface: compression
// starts disabled for single-cluster operations and the bandwidth limit
// starts unlimited.
func TestGlobalCompressFlagDefaultsOff(t *testing.T) {
	root := NewRoot(Options{Version: "test"})

	compress := root.PersistentFlags().Lookup("compress")
	if compress == nil {
		t.Fatal("--compress flag missing")
	}

	if compress.DefValue != "false" {
		t.Fatalf("--compress default=%q, want false (off)", compress.DefValue)
	}

	if root.PersistentFlags().Lookup("no-compress") != nil {
		t.Fatal("--no-compress must be replaced by --compress")
	}

	bandwidth := root.PersistentFlags().Lookup("copy-bandwidth-limit")
	if bandwidth == nil {
		t.Fatal("--copy-bandwidth-limit flag missing")
	}

	if bandwidth.DefValue != "" {
		t.Fatalf("--copy-bandwidth-limit default=%q, want empty (unlimited)", bandwidth.DefValue)
	}
}
