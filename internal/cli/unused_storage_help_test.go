package cli

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func findSubCommand(t *testing.T, root *cobra.Command, path ...string) *cobra.Command {
	t.Helper()

	current := root
	for _, segment := range path {
		next, _, err := current.Find([]string{segment})
		if err != nil || next == nil || next == current || next.Name() != segment {
			t.Fatalf("command %v not found under %q (err=%v)", path, current.Name(), err)
		}

		current = next
	}

	return current
}

func flagUsage(t *testing.T, command *cobra.Command, name string) string {
	t.Helper()

	flag := command.Flags().Lookup(name)
	if flag == nil {
		t.Fatalf("command %q has no --%s flag", command.CommandPath(), name)
	}

	return flag.Usage
}

// TestUnusedStoragePolicyHelpIsOperationSpecific pins the per-operation
// meaning of --unused-storage-policy: the flag decides the fate of different
// storage for copy, migration, and reservation workflows, so one generic
// description invites wrong expectations about what Delete removes.
func TestUnusedStoragePolicyHelpIsOperationSpecific(t *testing.T) {
	root := NewRoot(Options{Version: "test"})

	cases := []struct {
		path    []string
		mustSay []string
	}{
		{
			path:    []string{"copy"},
			mustSay: []string{"undelivered destination", "aborted before completing"},
		},
		{
			path:    []string{"copy", "cleanup"},
			mustSay: []string{"undelivered destination", "aborted before completing"},
		},
		{
			path:    []string{"cluster-copy", "cross"},
			mustSay: []string{"undelivered destination"},
		},
		{
			path:    []string{"migrate"},
			mustSay: []string{"old source PV", "rollback or abort"},
		},
		{
			path:    []string{"migrate", "cleanup"},
			mustSay: []string{"old source PV", "rollback or abort"},
		},
		{
			path:    []string{"migrate-pod"},
			mustSay: []string{"old source PV", "rollback or abort"},
		},
		{
			path:    []string{"migrate-pod", "cleanup"},
			mustSay: []string{"old source PV", "rollback or abort"},
		},
		{
			path:    []string{"reserve"},
			mustSay: []string{"never promoted to a copy"},
		},
		{
			path:    []string{"reserve", "cleanup"},
			mustSay: []string{"never promoted to a copy"},
		},
		{
			path:    []string{"cluster-reserve", "cross"},
			mustSay: []string{"never promoted to a copy"},
		},
	}

	usages := make([]string, 0, len(cases))

	for _, testCase := range cases {
		t.Run(strings.Join(testCase.path, " "), func(t *testing.T) {
			usage := flagUsage(
				t,
				findSubCommand(t, root, testCase.path...),
				"unused-storage-policy",
			)

			for _, phrase := range testCase.mustSay {
				if !strings.Contains(usage, phrase) {
					t.Fatalf(
						"command %q --unused-storage-policy usage=%q, want it to mention %q",
						strings.Join(testCase.path, " "),
						usage,
						phrase,
					)
				}
			}

			usages = append(usages, usage)
		})
	}

	copyText := flagUsage(t, findSubCommand(t, root, "copy"), "unused-storage-policy")
	migrateText := flagUsage(t, findSubCommand(t, root, "migrate"), "unused-storage-policy")
	reserveText := flagUsage(t, findSubCommand(t, root, "reserve"), "unused-storage-policy")

	if copyText == migrateText || migrateText == reserveText || copyText == reserveText {
		t.Fatal("--unused-storage-policy help text collapsed back to one shared description")
	}
}

// TestRepositoryCleanupHelpKeepsRecoveryPoints documents that backup and
// restore cleanup never touch the data the user depends on: published
// recovery points survive backup cleanup and a completed restore keeps its
// destination PVC.
func TestRepositoryCleanupHelpKeepsRecoveryPoints(t *testing.T) {
	root := NewRoot(Options{Version: "test"})

	backupFinalize := flagUsage(t, findSubCommand(t, root, "backup", "cleanup"), "finalize")
	if !strings.Contains(backupFinalize, "never deleted") {
		t.Fatalf(
			"backup cleanup --finalize usage=%q, want recovery-point retention",
			backupFinalize,
		)
	}

	restoreFinalize := flagUsage(t, findSubCommand(t, root, "restore", "cleanup"), "finalize")
	if !strings.Contains(restoreFinalize, "always kept") {
		t.Fatalf("restore cleanup --finalize usage=%q, want destination retention", restoreFinalize)
	}
}
