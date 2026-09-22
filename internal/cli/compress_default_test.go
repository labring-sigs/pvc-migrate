package cli

import (
	"strings"
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

// TestValidateCopyBandwidthRejectsRcloneOperations pins that the rsync rate
// limit is refused for backup and restore instead of being accepted and
// silently ignored: those operations transfer through rclone.
func TestValidateCopyBandwidthRejectsRcloneOperations(t *testing.T) {
	root := NewRoot(Options{Version: "test"})
	state := &rootState{}
	state.global.copyBandwidth = "10m"

	backup := findSubCommandT(t, root, "backup")
	if err := state.validateCopyBandwidth(backup); err == nil ||
		!strings.Contains(err.Error(), "backup and restore use rclone") {
		t.Fatalf("backup error=%v", err)
	}

	restore := findSubCommandT(t, root, "restore")
	if err := state.validateCopyBandwidth(restore); err == nil {
		t.Fatal("restore accepted the rsync-only limit")
	}

	copyCmd := findSubCommandT(t, root, "copy")
	if err := state.validateCopyBandwidth(copyCmd); err != nil {
		t.Fatalf("copy rejected a valid limit: %v", err)
	}

	state.global.copyBandwidth = "wat"
	if err := state.validateCopyBandwidth(copyCmd); err == nil ||
		!strings.Contains(err.Error(), "10m") {
		t.Fatalf("malformed limit error=%v", err)
	}

	state.global.copyBandwidth = ""
	if err := state.validateCopyBandwidth(copyCmd); err != nil {
		t.Fatalf("empty limit error=%v", err)
	}
}

func findSubCommandT(t *testing.T, root *cobra.Command, path ...string) *cobra.Command {
	t.Helper()

	current := root
	for _, segment := range path {
		next, _, err := current.Find([]string{segment})
		if err != nil || next == nil || next == current || next.Name() != segment {
			t.Fatalf("command %v not found under %q", path, current.Name())
		}

		current = next
	}

	return current
}
