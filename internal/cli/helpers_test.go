package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

type cliFailingWriter struct {
	err error
}

func (w cliFailingWriter) Write([]byte) (int, error) { return 0, w.err }

type cliBlockingReader struct {
	started chan struct{}
	release chan struct{}
}

func (r cliBlockingReader) Read([]byte) (int, error) {
	close(r.started)
	<-r.release
	return 0, io.EOF
}

func TestRootCommandSurfaceAndGlobalDefaults(t *testing.T) {
	root := NewRoot(Options{Version: "test"})
	t.Run("command surface and global flags", func(t *testing.T) {
		testRootCommandSurface(t, root)
	})
	t.Run("dry-run command placement", func(t *testing.T) {
		testRootDryRunPlacement(t, root)
	})
	t.Run("operation-specific flags", func(t *testing.T) {
		testRootOperationFlags(t, root)
	})
}

func TestOperationCommandSurfacesStayIndependent(t *testing.T) {
	root := NewRoot(Options{Version: "test"})

	checks := []struct {
		path    []string
		present []string
		absent  []string
	}{
		{path: []string{"reserve"}, absent: []string{"online", "source-node"}},
		{path: []string{"reserve", "plan"}, absent: []string{"online", "source-node"}},
		{path: []string{"copy"}, present: []string{"online", "source-node", "pod"}},
		{path: []string{"copy", "plan"}, present: []string{"online", "source-node", "pod"}},
		{
			path:    []string{"rename"},
			present: []string{"destination-pvc"},
			absent:  []string{"destination-namespace"},
		},
		{
			path:    []string{"rename", "plan"},
			present: []string{"destination-pvc"},
			absent: []string{
				"destination-namespace",
				"online",
				"source-node",
				"pod",
				"destination-capacity",
				"source-path",
				"target-node",
			},
		},
		{
			path:    []string{"move"},
			present: []string{"destination-pvc", "destination-namespace"},
			absent: []string{
				"online",
				"source-node",
				"pod",
				"destination-capacity",
				"source-path",
				"target-node",
			},
		},
		{
			path:    []string{"move", "plan"},
			present: []string{"destination-pvc", "destination-namespace"},
			absent: []string{
				"online",
				"source-node",
				"pod",
				"destination-capacity",
				"source-path",
				"target-node",
			},
		},
		{path: []string{"backup"}, present: []string{"online", "openebs-lvm-enable-shared"}},
		{
			path:    []string{"backup", "plan"},
			present: []string{"online", "openebs-lvm-enable-shared"},
		},
		{
			path:   []string{"reserve", "cross-cluster", "run"},
			absent: []string{"online", "verify-checksum", "delete-extraneous"},
		},
		{
			path:   []string{"reserve", "cross-cluster", "plan"},
			absent: []string{"online", "verify-checksum", "delete-extraneous"},
		},
		{
			path: []string{"reserve", "cross-cluster", "status"},
			absent: []string{
				"online",
				"verify-checksum",
				"delete-extraneous",
				"source-pvc",
				"destination-pvc",
			},
		},
		{
			path: []string{"reserve", "cross-cluster", "resume"},
			absent: []string{
				"online",
				"verify-checksum",
				"delete-extraneous",
				"source-pvc",
				"destination-pvc",
			},
		},
		{
			path: []string{"reserve", "cross-cluster", "cleanup"},
			absent: []string{
				"online",
				"verify-checksum",
				"delete-extraneous",
				"source-pvc",
				"destination-pvc",
			},
		},
		{
			path:    []string{"copy", "cross-cluster", "run"},
			present: []string{"online", "verify-checksum", "delete-extraneous"},
		},
		{
			path: []string{"copy", "cross-cluster", "status"},
			absent: []string{
				"source-pvc",
				"destination-pvc",
				"destination-capacity",
				"online",
				"verify-checksum",
				"delete-extraneous",
			},
		},
		{
			path: []string{"copy", "cross-cluster", "resume"},
			absent: []string{
				"source-pvc",
				"destination-pvc",
				"destination-capacity",
				"online",
				"verify-checksum",
				"delete-extraneous",
			},
		},
		{
			path: []string{"copy", "cross-cluster", "cleanup"},
			absent: []string{
				"source-pvc",
				"destination-pvc",
				"destination-capacity",
				"online",
				"verify-checksum",
				"delete-extraneous",
			},
		},
	}
	for _, check := range checks {
		command, _, err := root.Find(check.path)
		if err != nil {
			t.Fatalf("Find(%v): %v", check.path, err)
		}

		for _, name := range check.present {
			if command.Flags().Lookup(name) == nil {
				t.Fatalf("%v missing --%s", check.path, name)
			}
		}

		for _, name := range check.absent {
			if command.Flags().Lookup(name) != nil {
				t.Fatalf("%v unexpectedly exposes --%s", check.path, name)
			}
		}
	}
}

func testRootCommandSurface(t *testing.T, root *cobra.Command) {
	t.Helper()

	wantCommands := []string{
		"backup",
		"completion",
		"copy",
		"migrate",
		"migrate-pod",
		"move",
		"rename",
		"reserve",
		"restore",
		"recovery",
		"version",
	}
	for _, name := range wantCommands {
		command, _, err := root.Find([]string{name})
		if err != nil || command == root || command.Name() != name {
			t.Fatalf("Find(%q) command=%v error=%v", name, command, err)
		}
	}

	if mv, _, err := root.Find([]string{"mv"}); err == nil && mv != root {
		t.Fatalf("removed mv alias resolved to command %v", mv)
	}

	if session, _, err := root.Find([]string{"session"}); err == nil && session != root {
		t.Fatalf("removed session command resolved to %v", session)
	}

	for _, name := range []string{"final-sync", "activate"} {
		if command, _, err := root.Find([]string{name}); err == nil && command != root {
			t.Fatalf("removed top-level %s command resolved to %v", name, command)
		}
	}

	if command, _, err := root.Find([]string{"live-backup"}); err == nil && command != root {
		t.Fatalf("removed live-backup command resolved to %v", command)
	}

	for name, want := range map[string]string{
		"session-namespace":    "pvc-migrate-system",
		"controller-namespace": "pvc-migrate-system",
		"timeout":              "30m0s",
		"retries":              "3",
		"retry-backoff":        "2s",
		"helm-timeout":         "10m0s",
		"output":               "table",
		"log-format":           "text",
		"log-level":            "info",
		"color":                "auto",
		"stream-tool-logs":     "true",
		"wait":                 "true",
		"no-compress":          "false",
		"yes":                  "false",
		"tool-image":           "ghcr.io/labring-sigs/pvc-migrate:test",
	} {
		flag := root.PersistentFlags().Lookup(name)
		if flag == nil || flag.DefValue != want {
			t.Fatalf("flag --%s default=%v, want %q", name, flag, want)
		}
	}

	if root.PersistentFlags().Lookup("dry-run") != nil {
		t.Fatal("root exposes mutation-only --dry-run")
	}

	if root.PersistentFlags().Lookup("stream-helper-logs") != nil {
		t.Fatal("root exposes the removed --stream-helper-logs flag")
	}
}

func testRootDryRunPlacement(t *testing.T, root *cobra.Command) {
	t.Helper()

	mutationPaths := [][]string{
		{"backup"},
		{"copy"},
		{"migrate"},
		{"migrate", "resume"},
		{"migrate", "abort"},
		{"migrate", "rollback"},
		{"migrate", "cleanup"},
		{"migrate-pod"},
		{"migrate-pod", "resume"},
		{"migrate-pod", "abort"},
		{"migrate-pod", "rollback"},
		{"migrate-pod", "cleanup"},
		{"move"},
		{"rename"},
		{"reserve"},
		{"restore"},
		{"reserve", "resume"},
		{"reserve", "abort"},
		{"reserve", "cleanup"},
		{"copy", "resume"},
		{"copy", "abort"},
		{"copy", "cleanup"},
		{"rename", "resume"},
		{"rename", "abort"},
		{"rename", "rollback"},
		{"rename", "cleanup"},
		{"move", "resume"},
		{"move", "abort"},
		{"move", "rollback"},
		{"move", "cleanup"},
		{"backup", "resume"},
		{"backup", "abort"},
		{"backup", "cleanup"},
		{"recovery", "cleanup-orphan"},
	}
	for _, path := range mutationPaths {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("Find(%v): %v", path, err)
		}

		flag := command.Flags().Lookup("dry-run")
		if flag == nil || flag.DefValue != "true" {
			t.Fatalf("%v --dry-run default=%v, want true", path, flag)
		}
	}

	planPaths := [][]string{
		{"backup", "plan"},
		{"copy", "plan"},
		{"migrate", "plan"},
		{"migrate-pod", "plan"},
		{"move", "plan"},
		{"rename", "plan"},
		{"reserve", "plan"},
		{"restore", "plan"},
	}
	for _, path := range planPaths {
		command, _, err := root.Find(path)
		if err != nil || command.Name() != "plan" {
			t.Fatalf("Find(%v) command=%v error=%v", path, command, err)
		}

		if command.Flags().Lookup("dry-run") != nil {
			t.Fatalf("read-only %v exposes --dry-run", path)
		}
	}
}

func testRootOperationFlags(t *testing.T, root *cobra.Command) {
	t.Helper()

	testCopyBackupRestoreFlags(t, root)
	testCapacityAndPathFlagPlacement(t, root)
	testCutoverFlagPlacement(t, root)
	testCleanupFlagPlacement(t, root)
	testWorkflowCommandSurface(t, root)
}

func testCleanupFlagPlacement(t *testing.T, root *cobra.Command) {
	t.Helper()

	for _, workflow := range []string{"migrate", "migrate-pod", "reserve", "copy"} {
		command, _, err := root.Find([]string{workflow, "cleanup"})
		if err != nil {
			t.Fatalf("Find(%s cleanup): %v", workflow, err)
		}

		for _, name := range []string{"delete-temporary", "delete-rollback-pv", "finalize", "delete-session"} {
			if command.Flags().Lookup(name) == nil {
				t.Fatalf("%s cleanup is missing --%s", workflow, name)
			}
		}
	}

	for _, workflow := range []string{"backup", "rename", "move"} {
		command, _, err := root.Find([]string{workflow, "cleanup"})
		if err != nil {
			t.Fatalf("Find(%s cleanup): %v", workflow, err)
		}

		for _, name := range []string{"finalize", "delete-session"} {
			if command.Flags().Lookup(name) == nil {
				t.Fatalf("%s cleanup is missing --%s", workflow, name)
			}
		}

		for _, name := range []string{"delete-temporary", "delete-rollback-pv"} {
			if command.Flags().Lookup(name) != nil {
				t.Fatalf("%s cleanup unexpectedly exposes --%s", workflow, name)
			}
		}
	}
}

func testCopyBackupRestoreFlags(t *testing.T, root *cobra.Command) {
	t.Helper()

	copyCommand, _, err := root.Find([]string{"copy"})
	if err != nil {
		t.Fatal(err)
	}

	if online := copyCommand.Flags().Lookup("online"); online == nil || online.DefValue != "false" {
		t.Fatalf("copy --online default=%v, want false", online)
	}

	backupCommand, _, err := root.Find([]string{"backup"})
	if err != nil {
		t.Fatal(err)
	}

	if backupCommand.Flags().Lookup("allow-mounted") != nil {
		t.Fatal("backup exposes removed --allow-mounted alias")
	}

	restoreCommand, _, err := root.Find([]string{"restore"})
	if err != nil {
		t.Fatal(err)
	}

	if restoreCommand.Flags().Lookup("online") != nil {
		t.Fatal("restore exposes backup-only --online flag")
	}

	if restoreCommand.Flags().Lookup("allow-mounted") == nil {
		t.Fatal("restore is missing --allow-mounted")
	}

	backupID := backupCommand.Flags().Lookup("id")
	restoreID := restoreCommand.Flags().Lookup("id")

	if backupID == nil || !strings.Contains(backupID.Usage, "Session ID") {
		t.Fatalf("backup --id usage=%v", backupID)
	}

	if restoreID == nil || !strings.Contains(restoreID.Usage, "Restore session ID") {
		t.Fatalf("restore --id usage=%v", restoreID)
	}

	if label, value := transferResultIdentity(
		false,
		"backup-1",
	); label != "session" ||
		value != "backup-1" {
		t.Fatalf("backup result identity=%q/%q", label, value)
	}

	if label, value := transferResultIdentity(
		true,
		"restore-1",
	); label != "operation-id" ||
		value != "restore-1" {
		t.Fatalf("restore result identity=%q/%q", label, value)
	}

	if label, value := transferResultIdentity(true, ""); label != "operation-id" || value != "-" {
		t.Fatalf("anonymous restore result identity=%q/%q", label, value)
	}

	for _, name := range []string{"capacity-awareness", "destination-storage-class", "source-node", "target-node", "pod"} {
		if copyCommand.Flags().Lookup(name) == nil {
			t.Fatalf("copy is missing --%s", name)
		}
	}
}

func testCapacityAndPathFlagPlacement(t *testing.T, root *cobra.Command) {
	t.Helper()

	for _, path := range [][]string{{"reserve"}, {"reserve", "plan"}, {"copy"}, {"copy", "plan"}, {"migrate"}, {"migrate", "plan"}} {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("Find(%v): %v", path, err)
		}

		for _, name := range []string{"destination-capacity", "allow-volume-shrink", "source-path", "destination-path"} {
			if command.Flags().Lookup(name) == nil {
				t.Fatalf("%v is missing --%s", path, name)
			}
		}
	}

	for _, path := range [][]string{{"migrate-pod"}, {"migrate-pod", "plan"}} {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("Find(%v): %v", path, err)
		}

		for _, name := range []string{"destination-capacity", "allow-volume-shrink", "source-path", "destination-path"} {
			if command.Flags().Lookup(name) == nil {
				t.Fatalf("%v is missing --%s", path, name)
			}
		}

		for _, name := range []string{"source-pvc", "destination-namespace", "destination-pvc"} {
			if command.Flags().Lookup(name) != nil {
				t.Fatalf("%v unexpectedly exposes --%s", path, name)
			}
		}
	}

	for _, path := range [][]string{{"rename"}, {"rename", "plan"}, {"move"}, {"move", "plan"}, {"backup"}, {"restore"}} {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("Find(%v): %v", path, err)
		}

		for _, name := range []string{"destination-capacity", "allow-volume-shrink", "source-path", "destination-path"} {
			if path[0] == "restore" && name == "destination-capacity" {
				continue
			}

			if command.Flags().Lookup(name) != nil {
				t.Fatalf("%v unexpectedly exposes --%s", path, name)
			}
		}
	}

	restore, _, err := root.Find([]string{"restore"})
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"create-pvc", "destination-storage-class", "destination-access-mode", "destination-capacity", "target-node"} {
		if restore.Flags().Lookup(name) == nil {
			t.Fatalf("restore is missing --%s", name)
		}
	}
}

func testCutoverFlagPlacement(t *testing.T, root *cobra.Command) {
	t.Helper()

	copyCommand, _, err := root.Find([]string{"copy"})
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"kubeblocks-candidate", "allow-leader-downtime", "force-reprovision", "precopy-passes", "openebs-lvm-enable-shared"} {
		if copyCommand.Flags().Lookup(name) != nil {
			t.Fatalf("copy exposes cutover-only --%s", name)
		}
	}

	migrateCommand, _, err := root.Find([]string{"migrate"})
	if err != nil {
		t.Fatal(err)
	}

	if migrateCommand.Flags().Lookup("force-reprovision") != nil {
		t.Fatal("migrate exposes migrate-pod-only --force-reprovision")
	}

	for _, path := range [][]string{{"migrate-pod"}, {"migrate-pod", "plan"}} {
		command, _, err := root.Find(path)

		flag := command.Flags().Lookup("precopy-passes")
		if err != nil || flag == nil || flag.DefValue != "1" {
			t.Fatalf("%v precopy-passes flag=%v error=%v", path, flag, err)
		}

		if flag := command.Flags().
			Lookup("openebs-lvm-enable-shared"); flag == nil ||
			flag.DefValue != "false" {
			t.Fatalf("%v openebs-lvm-enable-shared flag=%v", path, flag)
		}
	}

	for _, path := range [][]string{{"migrate"}, {"migrate", "plan"}} {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatal(err)
		}

		for _, name := range []string{"pod", "precopy-passes", "openebs-lvm-enable-shared", "kubeblocks-candidate", "allow-leader-downtime"} {
			if command.Flags().Lookup(name) != nil {
				t.Fatalf("%v unexpectedly exposes --%s", path, name)
			}
		}
	}

	for _, path := range [][]string{{"migrate-pod"}, {"migrate-pod", "plan"}} {
		command, _, err := root.Find(path)
		if err != nil || command.Flags().Lookup("force-reprovision") == nil {
			t.Fatalf(
				"%v force-reprovision flag=%v error=%v",
				path,
				command.Flags().Lookup("force-reprovision"),
				err,
			)
		}
	}
}

func testWorkflowCommandSurface(t *testing.T, root *cobra.Command) {
	t.Helper()

	for _, workflow := range []string{"migrate", "migrate-pod", "reserve", "copy", "backup", "rename", "move"} {
		status, _, err := root.Find([]string{workflow, "status"})
		if err != nil || status.Flags().Lookup("dry-run") != nil {
			t.Fatalf(
				"%s status dry-run flag=%v error=%v",
				workflow,
				status.Flags().Lookup("dry-run"),
				err,
			)
		}
	}

	if recovery, _, err := root.Find(
		[]string{"recovery", "cleanup-orphan"},
	); err != nil ||
		recovery.Name() != "cleanup-orphan" {
		t.Fatalf("recovery cleanup-orphan command=%v error=%v", recovery, err)
	}

	restore, _, err := root.Find([]string{"restore"})
	if err != nil {
		t.Fatal(err)
	}

	deleteExtraneous := restore.Flags().Lookup("delete-extraneous")
	if deleteExtraneous == nil || deleteExtraneous.DefValue != "false" {
		t.Fatalf("restore delete-extraneous default=%v, want false", deleteExtraneous)
	}
}

func TestMigrationFlagDefaultsAndPlanOptions(t *testing.T) {
	state := &rootState{global: globals{sessionNamespace: "sessions"}}
	flags := &podMigrationFlags{}
	command := &cobra.Command{Use: "test"}
	flags.bind(command)
	flags.bindForceReprovision(command)
	testMigrationFlagDefaults(t, command)

	configureMigrationFlags(flags)
	options := migrationPlanOptions(t, state, flags)

	testMigrationPlanIdentity(t, options)
	testMigrationPlanBehavior(t, options)
	testMigrationPlanSliceOwnership(t, flags, options)
}

func migrationPlanOptions(
	t *testing.T,
	state *rootState,
	flags *podMigrationFlags,
) planner.PodMigrationOptions {
	t.Helper()

	options, err := flags.planOptions(state, true)
	if err != nil {
		t.Fatal(err)
	}

	return options
}

func testMigrationFlagDefaults(t *testing.T, command *cobra.Command) {
	t.Helper()

	for name, want := range map[string]string{
		"capacity-awareness":        "auto",
		"destination-capacity":      "[]",
		"allow-volume-shrink":       "false",
		"skip-source-usage-check":   "false",
		"source-namespace":          "default",
		"temporary-namespace":       "pvc-migrate-system",
		"target-node":               "auto",
		"strategy":                  "[auto]",
		"verify-checksum":           "true",
		"delete-extraneous":         "true",
		"precopy-passes":            "1",
		"openebs-lvm-enable-shared": "false",
		"force-reprovision":         "false",
	} {
		flag := command.Flags().Lookup(name)
		if flag == nil || flag.DefValue != want {
			t.Fatalf("flag --%s default=%v, want %q", name, flag, want)
		}
	}
}

func configureMigrationFlags(flags *podMigrationFlags) {
	flags.sessionID = "mig-fixed"
	flags.sourceNamespace = "source"
	flags.temporaryNamespace = "staging"
	flags.destinationCapacities = []string{"3Gi", "4Gi"}
	flags.sourcePaths = []string{"data=data/current", "logs=logs/current"}
	flags.destinationPaths = []string{"data=.", "logs=restored/logs"}
	flags.allowVolumeShrink = true
	flags.podName = "db-2"
	flags.sourceNode = "node-a"
	flags.targetNode = "node-b"
	flags.destinationClass = "fast"
	flags.capacityAwareness = "require"
	flags.strategies = []string{"local"}
	flags.verifyChecksum = true
	flags.deleteExtraneous = true
	flags.switchoverCandidate = "db-1"
	flags.allowLeaderDowntime = true
	flags.forceReprovision = true
	flags.openEBSLVMEnableShared = true
}

func testMigrationPlanIdentity(t *testing.T, options planner.PodMigrationOptions) {
	t.Helper()

	if options.SessionID != "mig-fixed" || options.SourceNamespace != "source" ||
		options.TemporaryNamespace != "staging" ||
		options.StagingNamespace != "staging" ||
		options.SessionNamespace != "sessions" {
		t.Fatalf("namespaces and identity = %#v", options)
	}
}

func testMigrationPlanBehavior(t *testing.T, options planner.PodMigrationOptions) {
	t.Helper()

	if options.PodName != "db-2" ||
		options.SourceNode != "node-a" ||
		options.TargetNode != "node-b" ||
		options.DestinationClass != "fast" ||
		options.CapacityAwareness != domain.CapacityAwarenessRequire ||
		options.SwitchoverCandidate != "db-1" ||
		!options.AllowLeaderDowntime ||
		!options.AllowVolumeShrink ||
		!options.ForceReprovision ||
		!options.VerifyChecksum ||
		!options.DeleteExtraneous ||
		!options.OpenEBSLVMEnableShared ||
		options.PrecopyPasses != 1 {
		t.Fatalf("options = %#v", options)
	}
}

func testMigrationPlanSliceOwnership(
	t *testing.T,
	flags *podMigrationFlags,
	options planner.PodMigrationOptions,
) {
	t.Helper()

	flags.destinationCapacities[0] = "mutated"
	flags.sourcePaths[0] = "mutated"
	flags.destinationPaths[0] = "mutated"
	flags.strategies[0] = "mutated"

	if options.DestinationCapacities[0] != "3Gi" ||
		options.SourcePaths[0] != "data=data/current" ||
		options.DestinationPaths[0] != "data=." ||
		options.Strategies[0] != "local" {
		t.Fatalf("plan options alias flag slices: %#v", options)
	}
}

func TestTransferPathFlagsRejectUnsafeAndExistingSessionChanges(t *testing.T) {
	for _, test := range []struct {
		name             string
		sourcePaths      []string
		destinationPaths []string
		existing         bool
	}{
		{name: "existing session", sourcePaths: []string{"data=partial"}, existing: true},
		{name: "absolute", sourcePaths: []string{"data=/etc"}},
		{name: "traversal", destinationPaths: []string{"data=../outside"}},
		{name: "empty mapping", sourcePaths: []string{"data="}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateTransferPathFlags(
				domain.OperationCopy,
				test.existing,
				test.sourcePaths,
				test.destinationPaths,
			); domain.CategoryOf(
				err,
			) != domain.ErrorValidation {
				t.Fatalf("error=%v category=%s", err, domain.CategoryOf(err))
			}
		})
	}

	if err := validateTransferPathFlags(
		domain.OperationCopy,
		false,
		[]string{"data=tenant/current"},
		[]string{"data=."},
	); err != nil {
		t.Fatal(err)
	}
}

func TestPlanOptionsDirectAndGeneratedIdentity(t *testing.T) {
	state := &rootState{global: globals{sessionNamespace: "sessions"}}
	flags := &copyFlags{
		sourceNamespace: "app", temporaryNamespace: "staging", destinationNamespace: "archive",
	}

	options, err := flags.planOptions(state, false)
	if err != nil {
		t.Fatal(err)
	}

	if flags.sessionID == "" || options.SessionID != flags.sessionID ||
		!strings.HasPrefix(options.SessionID, "mig-") {
		t.Fatalf("generated session ID flags=%q options=%q", flags.sessionID, options.SessionID)
	}

	if options.DestinationNamespace != "archive" || options.StagingNamespace != "archive" ||
		options.TemporaryNamespace != "archive" {
		t.Fatalf("direct namespaces = %#v", options)
	}
}

func TestOfflinePlanOptionsPreserveSourcePVCSelection(t *testing.T) {
	state := &rootState{global: globals{sessionNamespace: "sessions"}}
	flags := &offlineMigrationFlags{
		sourceNamespace:       "app",
		temporaryNamespace:    "staging",
		sourcePVCs:            []string{"data", "logs"},
		destinationPVCs:       []string{"data-copy", "logs-copy"},
		destinationCapacities: []string{"3Gi"},
	}

	options, err := flags.planOptions(state, true)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(options.SourcePVCs, flags.sourcePVCs) {
		t.Fatalf("source PVCs = %#v, want %#v", options.SourcePVCs, flags.sourcePVCs)
	}

	if !reflect.DeepEqual(options.DestinationPVCs, flags.destinationPVCs) {
		t.Fatalf("destination PVCs = %#v, want %#v", options.DestinationPVCs, flags.destinationPVCs)
	}
}

func TestParseLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"INFO":    slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
	}
	for input, want := range cases {
		got, err := parseLogLevel(input)
		if err != nil || got != want {
			t.Fatalf("parseLogLevel(%q) = %v, %v; want %v", input, got, err, want)
		}
	}

	if _, err := parseLogLevel("trace"); domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("invalid log level error=%v category=%q", err, domain.CategoryOf(err))
	}
}

func TestParseColorMode(t *testing.T) {
	for _, input := range []string{"", "auto", "always", "never"} {
		mode, err := parseColorMode(input)
		if err != nil {
			t.Fatalf("parseColorMode(%q): %v", input, err)
		}

		if input == "" && mode != colorAuto || input != "" && mode != input {
			t.Fatalf("parseColorMode(%q)=%q", input, mode)
		}
	}

	if _, err := parseColorMode("rainbow"); domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("invalid color mode error=%v category=%q", err, domain.CategoryOf(err))
	}
}

func TestParseExecutionMode(t *testing.T) {
	tests := []struct {
		input string
		want  executionMode
	}{
		{input: "auto", want: executionModeAuto},
		{input: " SESSION ", want: executionModeSession},
		{input: "Controller", want: executionModeController},
	}
	for _, test := range tests {
		got, err := parseExecutionMode(test.input)
		if err != nil {
			t.Fatalf("parseExecutionMode(%q): %v", test.input, err)
		}

		if got != test.want {
			t.Fatalf("parseExecutionMode(%q)=%q, want %q", test.input, got, test.want)
		}
	}

	if _, err := parseExecutionMode("direct"); domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("invalid execution mode error=%v category=%q", err, domain.CategoryOf(err))
	}
}

func TestRuntimeFreeCommandsRejectInvalidColorMode(t *testing.T) {
	for _, args := range [][]string{{"--color", "rainbow", "version"}, {"--color", "rainbow", "completion", "bash"}} {
		stdout, err := executeCLI(t, args...)
		if domain.CategoryOf(err) != domain.ErrorValidation ||
			!strings.Contains(err.Error(), "color mode") {
			t.Fatalf("args=%v error=%v category=%q", args, err, domain.CategoryOf(err))
		}

		if stdout != "" {
			t.Fatalf("args=%v produced output: %q", args, stdout)
		}
	}
}

func TestLoggerForFormatsAndValidation(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			var output bytes.Buffer

			state := &rootState{
				options: Options{ErrOut: &output},
				global:  globals{logFormat: format, logLevel: "debug"},
			}

			logger, err := loggerFor(state)
			if err != nil {
				t.Fatal(err)
			}

			logger.Debug("configured", "format", format)

			if !strings.Contains(output.String(), "configured") ||
				!strings.Contains(output.String(), format) {
				t.Fatalf("log output = %q", output.String())
			}
		})
	}

	state := &rootState{
		options: Options{ErrOut: io.Discard},
		global:  globals{logFormat: "console", logLevel: "info"},
	}
	if _, err := loggerFor(state); domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("invalid format error=%v category=%q", err, domain.CategoryOf(err))
	}

	state.global.logFormat = "text"

	state.global.logLevel = "trace"
	if _, err := loggerFor(state); domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("invalid level error=%v category=%q", err, domain.CategoryOf(err))
	}
}

func TestKubernetesLogsFollowCLIFormat(t *testing.T) {
	state := klog.CaptureState()
	defer state.Restore()

	var output bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&output, nil))
	configureKubernetesLogger(logger)
	klog.ErrorS(errors.New("watch ended"), "reflector failed", "resource", "pods")

	text := output.String()
	for _, want := range []string{`"msg":"reflector failed"`, `"component":"kubernetes"`, `"resource":"pods"`, `"err":"watch ended"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("Kubernetes JSON log lacks %q: %s", want, text)
		}
	}
}

func TestKubernetesLoggerSuppressesExpectedWatchCancellation(t *testing.T) {
	state := klog.CaptureState()
	defer state.Restore()

	var output bytes.Buffer
	configureKubernetesLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	klog.ErrorS(context.Canceled, "Failed to watch", "resource", "pods")

	if output.Len() != 0 {
		t.Fatalf("expected canceled watch log to be suppressed, got %q", output.String())
	}
}

func TestKubernetesLoggerKeepsUnexpectedWatchErrors(t *testing.T) {
	state := klog.CaptureState()
	defer state.Restore()

	var output bytes.Buffer
	configureKubernetesLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	klog.ErrorS(errors.New("forbidden"), "Failed to watch", "resource", "pods")

	text := output.String()
	for _, want := range []string{`"msg":"Failed to watch"`, `"component":"kubernetes"`, `"resource":"pods"`, `"err":"forbidden"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("unexpected watch error log lacks %q: %s", want, text)
		}
	}
}

func TestKubernetesLoggerKeepsCancellationForOtherMessages(t *testing.T) {
	state := klog.CaptureState()
	defer state.Restore()

	var output bytes.Buffer
	configureKubernetesLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	klog.ErrorS(context.Canceled, "Failed to list", "resource", "pods")

	if output.Len() == 0 {
		t.Fatal("expected cancellation for a non-watch message to be logged")
	}
}

func TestRootContextTimeoutModes(t *testing.T) {
	state := &rootState{global: globals{timeout: 5 * time.Second}}

	ctx, cancel := state.context(context.Background())
	defer cancel()

	deadline, ok := ctx.Deadline()

	remaining := time.Until(deadline)
	if !ok || remaining < 4*time.Second || remaining > 5*time.Second {
		t.Fatalf("deadline=%v ok=%t", deadline, ok)
	}

	state.global.timeout = 0

	ctx, cancel = state.context(context.Background())
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("zero timeout created deadline")
	}

	cancel()

	select {
	case <-ctx.Done():
	default:
		t.Fatal("cancel did not close context")
	}
}

func TestConfirmationPaths(t *testing.T) {
	t.Run("assume yes", func(t *testing.T) {
		state := &rootState{global: globals{assumeYes: true}}
		if err := state.confirm(context.Background(), &cobra.Command{}, "data"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("canceled assume yes", func(t *testing.T) {
		state := &rootState{global: globals{assumeYes: true}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := state.confirm(ctx, &cobra.Command{}, "data")
		if domain.CategoryOf(err) != domain.ErrorTimeout || !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
		}
	})
	t.Run("empty identity", func(t *testing.T) {
		state := &rootState{}
		if err := state.confirm(
			context.Background(),
			&cobra.Command{},
			"",
		); domain.CategoryOf(
			err,
		) != domain.ErrorValidation {
			t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
		}
	})
	t.Run("matching input", func(t *testing.T) {
		var prompt bytes.Buffer

		command := &cobra.Command{}
		command.SetIn(strings.NewReader("data\n"))
		command.SetErr(&prompt)

		if err := (&rootState{}).confirm(context.Background(), command, "data"); err != nil {
			t.Fatal(err)
		}

		if prompt.String() != "Type data to approve: " {
			t.Fatalf("prompt=%q", prompt.String())
		}
	})
	t.Run("mismatch", func(t *testing.T) {
		command := &cobra.Command{}
		command.SetIn(strings.NewReader("other\n"))
		command.SetErr(io.Discard)

		if err := (&rootState{}).confirm(
			context.Background(),
			command,
			"data",
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
		}
	})
	t.Run("missing input", func(t *testing.T) {
		command := &cobra.Command{}
		command.SetIn(strings.NewReader(""))
		command.SetErr(io.Discard)

		err := (&rootState{}).confirm(context.Background(), command, "data")
		if domain.CategoryOf(err) != domain.ErrorPrecondition || !errors.Is(err, io.EOF) {
			t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
		}
	})
	t.Run("prompt write failure", func(t *testing.T) {
		wantErr := errors.New("closed")
		command := &cobra.Command{}
		command.SetErr(cliFailingWriter{err: wantErr})

		if err := (&rootState{}).confirm(
			context.Background(),
			command,
			"data",
		); !errors.Is(
			err,
			wantErr,
		) {
			t.Fatalf("error=%v want=%v", err, wantErr)
		}
	})
	t.Run("canceled while waiting for input", func(t *testing.T) {
		reader := cliBlockingReader{started: make(chan struct{}), release: make(chan struct{})}
		defer close(reader.release)

		command := &cobra.Command{}
		command.SetIn(reader)
		command.SetErr(io.Discard)

		ctx, cancel := context.WithCancel(context.Background())

		result := make(chan error, 1)
		go func() { result <- (&rootState{}).confirm(ctx, command, "data") }()

		select {
		case <-reader.started:
		case <-time.After(time.Second):
			t.Fatal("approval did not start reading input")
		}

		cancel()

		var err error
		select {
		case err = <-result:
		case <-time.After(time.Second):
			t.Fatal("approval did not stop after cancellation")
		}

		if domain.CategoryOf(err) != domain.ErrorTimeout || !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
		}
	})
	t.Run("canceled input never approves", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		command := &cobra.Command{}
		command.SetIn(strings.NewReader("data\n"))
		command.SetErr(io.Discard)

		err := (&rootState{}).confirm(ctx, command, "data")
		if domain.CategoryOf(err) != domain.ErrorTimeout || !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
		}
	})
}

func TestResumeApprovalCoversPVCRebindStages(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseRenaming, domain.PhaseMoving} {
		if !requiresResumeApproval(phase) {
			t.Fatalf("phase %s does not require resume approval", phase)
		}
	}

	if requiresResumeApproval(domain.PhasePlanned) {
		t.Fatal("planned phase unexpectedly requires resume approval")
	}

	if !requiresOperationResumeApproval(domain.OperationRename, domain.PhasePlanned) ||
		!requiresOperationResumeApproval(domain.OperationMove, domain.PhasePlanned) {
		t.Fatal("planned PVC identity phases require approval")
	}

	if requiresOperationResumeApproval(domain.OperationMigrate, domain.PhasePlanned) {
		t.Fatal("planned orchestrated phases do not require identity approval")
	}
}

func TestApprovalIdentityPrecedence(t *testing.T) {
	offlineCases := []struct {
		flags offlineMigrationFlags
		want  string
	}{
		{
			flags: offlineMigrationFlags{sourcePVCs: []string{"data", "logs"}, sessionID: "mig"},
			want:  "data",
		},
		{flags: offlineMigrationFlags{sessionID: "mig"}, want: "mig"},
		{flags: offlineMigrationFlags{}, want: ""},
	}
	for _, tc := range offlineCases {
		if got := offlineApprovalIdentity(&tc.flags); got != tc.want {
			t.Fatalf("offlineApprovalIdentity(%#v) = %q, want %q", tc.flags, got, tc.want)
		}
	}

	podCases := []struct {
		flags podMigrationFlags
		want  string
	}{
		{flags: podMigrationFlags{podName: "db-0", sessionID: "mig"}, want: "db-0"},
		{flags: podMigrationFlags{sessionID: "mig"}, want: "mig"},
		{flags: podMigrationFlags{}, want: ""},
	}
	for _, tc := range podCases {
		if got := podApprovalIdentity(&tc.flags); got != tc.want {
			t.Fatalf("podApprovalIdentity(%#v) = %q, want %q", tc.flags, got, tc.want)
		}
	}
}

func TestReadinessHelpers(t *testing.T) {
	if err := requireReady(&domain.MigrationPlan{Ready: true}); err != nil {
		t.Fatal(err)
	}

	err := requireReady(&domain.MigrationPlan{Ready: false})
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("requireReady error=%v category=%q", err, domain.CategoryOf(err))
	}

	var printed bytes.Buffer

	runtime := &commandRuntime{printer: output.Printer{Writer: &printed, Format: output.JSON}}
	plan := &domain.MigrationPlan{SessionID: "mig", Ready: false, Checks: []domain.Check{
		{
			Name:     "controller-adapter",
			Severity: domain.SeverityError,
			Message:  "discover workload: controller has no safe pause adapter",
		},
	}}

	var guidance bytes.Buffer

	err = requireReadyWithOutput(runtime, plan, &guidance)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(printed.String(), "mig") ||
		!strings.Contains(
			guidance.String(),
			"use a supported workload adapter or the controller's native maintenance procedure",
		) {
		t.Fatalf("error=%v output=%q guidance=%q", err, printed.String(), guidance.String())
	}

	runtime.printer.Writer = cliFailingWriter{err: io.ErrClosedPipe}
	if err := requireReadyWithOutput(runtime, plan, io.Discard); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("print failure=%v", err)
	}

	printed.Reset()

	runtime.printer.Writer = &printed
	if err := requireReadyWithOutput(
		runtime,
		&domain.MigrationPlan{Ready: true},
		io.Discard,
	); err != nil ||
		printed.Len() != 0 {
		t.Fatalf("ready error=%v output=%q", err, printed.String())
	}
}

func TestPlanFailureGuidanceSuggestsActionForControllerAndStorageChecks(t *testing.T) {
	var output bytes.Buffer

	plan := &domain.MigrationPlan{
		Checks: []domain.Check{
			{
				Name:     "controller-adapter",
				Severity: domain.SeverityError,
				Message:  "discover workload: controller has no safe pause adapter",
			},
			{
				Name:     "storage-capacity",
				Severity: domain.SeverityError,
				Message:  "capacity is insufficient",
			},
		},
	}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	for _, want := range []string{
		"use a supported workload adapter or the controller's native maintenance procedure",
		"require no HorizontalPodAutoscaler",
		"choose a compatible StorageClass or target node",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}
}

func TestPlanFailureGuidancePrioritizesStatefulSetAction(t *testing.T) {
	var output bytes.Buffer

	plan := &domain.MigrationPlan{Checks: []domain.Check{
		{
			Name:     "controller-adapter",
			Severity: domain.SeverityError,
			Message:  "discover workload: StatefulSet app/db PVC retention whenScaled is Delete; Retain is required",
		},
	}}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}

	if text := output.String(); !strings.Contains(
		text,
		"persistentVolumeClaimRetentionPolicy.whenScaled=Retain",
	) {
		t.Fatalf("guidance=%q", text)
	}
}

func TestPlanFailureGuidanceExplainsKubeBlocksCandidateScope(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name: "InstanceSet secondary",
			message: "discover KubeBlocks: --kubeblocks-candidate applies only when " +
				"the selected InstanceSet Pod has a leader role; Pod db/cluster-db-1 has role secondary",
			want: "remove --kubeblocks-candidate when migrating a non-leader InstanceSet Pod",
		},
		{
			name: "legacy component",
			message: "discover KubeBlocks: --kubeblocks-candidate is supported only for " +
				"InstanceSet-backed KubeBlocks components",
			want: "remove --kubeblocks-candidate for a legacy KubeBlocks component",
		},
		{
			name: "Redis primary",
			message: "discover KubeBlocks: automatic switchover for selected instance redis-0 " +
				"is unavailable: the KubeBlocks Redis addon does not provide a Switchover action",
			want: "remove --kubeblocks-candidate and rerun with --allow-leader-downtime",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer

			plan := &domain.MigrationPlan{Checks: []domain.Check{{
				Name:     "controller-adapter",
				Severity: domain.SeverityError,
				Message:  test.message,
			}}}

			if err := writePlanFailureGuidance(&output, plan); err != nil {
				t.Fatal(err)
			}

			if text := output.String(); !strings.Contains(text, test.want) ||
				strings.Contains(text, "use a supported workload adapter") {
				t.Fatalf("guidance=%q", text)
			}
		})
	}
}

func TestBucketFlagValidationMatrix(t *testing.T) {
	cases := []struct {
		name    string
		flags   bucketFlags
		pvcFlag string
		want    string
	}{
		{
			name:    "source PVC",
			flags:   bucketFlags{name: "daily", backend: "s3", bucket: "b"},
			pvcFlag: "source-pvc",
			want:    "--source-pvc",
		},
		{
			name:    "backup name",
			flags:   bucketFlags{pvc: "data", backend: "s3", bucket: "b"},
			pvcFlag: "source-pvc",
			want:    "--name",
		},
		{
			name:    "backend",
			flags:   bucketFlags{pvc: "data", name: "daily", bucket: "b"},
			pvcFlag: "source-pvc",
			want:    "--backend and --bucket",
		},
		{
			name:    "bucket",
			flags:   bucketFlags{pvc: "data", name: "daily", backend: "s3"},
			pvcFlag: "source-pvc",
			want:    "--backend and --bucket",
		},
		{
			name:    "azure backend",
			flags:   bucketFlags{pvc: "data", name: "daily", backend: "azure", bucket: "b"},
			pvcFlag: "source-pvc",
			want:    "unsupported backend",
		},
		{
			name:    "gcs backend",
			flags:   bucketFlags{pvc: "data", name: "daily", backend: "gcs", bucket: "b"},
			pvcFlag: "source-pvc",
			want:    "unsupported backend",
		},
		{
			name: "repository namespace without repository",
			flags: bucketFlags{
				pvc:                       "data",
				name:                      "daily",
				backend:                   "s3",
				backupRepositoryNamespace: "backups",
			},
			pvcFlag: "source-pvc",
			want:    "requires --backup-repository",
		},
		{
			name: "invalid repository namespace",
			flags: bucketFlags{
				pvc:                       "data",
				name:                      "daily",
				backend:                   "s3",
				backupRepository:          "shared",
				backupRepositoryNamespace: "INVALID_NS",
			},
			pvcFlag: "source-pvc",
			want:    "invalid --backup-repository-namespace",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBucketFlags(&tc.flags, tc.pvcFlag)
			if domain.CategoryOf(err) != domain.ErrorValidation ||
				!strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
			}
		})
	}

	valid := bucketFlags{pvc: "data", name: "daily", backend: "s3", bucket: "backups"}
	if err := validateBucketFlags(&valid, "source-pvc"); err != nil {
		t.Fatal(err)
	}

	repository := bucketFlags{
		pvc:              "data",
		name:             "daily",
		backend:          "s3",
		backupRepository: "shared-s3",
	}
	if err := validateBucketFlags(&repository, "source-pvc"); err != nil {
		t.Fatalf("repository-backed flags should not require bucket: %v", err)
	}

	if err := validateControllerRepositoryFlags(&repository); err != nil {
		t.Fatalf("repository-backed defaults should be accepted: %v", err)
	}

	repository.backupRepositoryNamespace = "backups"
	if err := validateBucketFlags(&repository, "source-pvc"); err != nil {
		t.Fatalf("cross-namespace repository reference should be accepted: %v", err)
	}

	repository.prefixExplicit = true
	if err := validateControllerRepositoryFlags(
		&repository,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "BackupRepository") {
		t.Fatalf(
			"explicit repository prefix override error=%v category=%q",
			err,
			domain.CategoryOf(err),
		)
	}
}

func TestCommandsRejectInvalidInputBeforeClusterAccess(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		category domain.ErrorCategory
		text     string
	}{
		{
			name:     "pod",
			args:     []string{"migrate-pod"},
			category: domain.ErrorValidation,
			text:     "--pod",
		},
		{
			name:     "pod plan",
			args:     []string{"migrate-pod", "plan"},
			category: domain.ErrorValidation,
			text:     "--pod",
		},
		{
			name:     "move namespace",
			args:     []string{"move", "--source-pvc", "data"},
			category: domain.ErrorValidation,
			text:     "--destination-namespace",
		},
		{
			name:     "rename destination",
			args:     []string{"rename", "--source-pvc", "data"},
			category: domain.ErrorValidation,
			text:     "--destination-pvc",
		},
		{
			name:     "negative precopy passes",
			args:     []string{"migrate-pod", "--pod", "db-0", "--precopy-passes", "-1"},
			category: domain.ErrorValidation,
			text:     "cannot be negative",
		},
		{
			name:     "negative plan precopy passes",
			args:     []string{"migrate-pod", "plan", "--pod", "db-0", "--precopy-passes", "-1"},
			category: domain.ErrorValidation,
			text:     "cannot be negative",
		},
		{
			name:     "shrink opt-in without capacity",
			args:     []string{"migrate", "--allow-volume-shrink"},
			category: domain.ErrorValidation,
			text:     "requires --destination-capacity",
		},
		{
			name:     "usage skip without shrink opt-in",
			args:     []string{"migrate", "--skip-source-usage-check"},
			category: domain.ErrorValidation,
			text:     "requires --allow-volume-shrink",
		},
		{
			name:     "invalid destination capacity",
			args:     []string{"copy", "--destination-capacity", "invalid"},
			category: domain.ErrorValidation,
			text:     "is invalid",
		},
		{
			name:     "zero destination capacity",
			args:     []string{"reserve", "--destination-capacity", "0"},
			category: domain.ErrorValidation,
			text:     "must be positive",
		},
		{
			name:     "existing session capacity override",
			args:     []string{"copy", "--session", "existing", "--destination-capacity", "2Gi"},
			category: domain.ErrorValidation,
			text:     "cannot modify an existing session",
		},
		{
			name: "existing reserve session shrink override",
			args: []string{
				"reserve",
				"plan",
				"--session",
				"existing",
				"--destination-capacity",
				"1Gi",
				"--allow-volume-shrink",
			},
			category: domain.ErrorValidation,
			text:     "cannot modify an existing session",
		},
		{
			name:     "existing session usage check override",
			args:     []string{"copy", "--session", "existing", "--skip-source-usage-check"},
			category: domain.ErrorValidation,
			text:     "cannot modify an existing session",
		},
		{
			name:     "zero retries",
			args:     []string{"migrate", "plan", "--retries", "0"},
			category: domain.ErrorValidation,
			text:     "--retries",
		},
		{
			name:     "negative retry backoff",
			args:     []string{"migrate", "plan", "--retry-backoff", "-1s"},
			category: domain.ErrorValidation,
			text:     "--retry-backoff",
		},
		{
			name:     "zero Helm timeout",
			args:     []string{"migrate", "plan", "--helm-timeout", "0"},
			category: domain.ErrorValidation,
			text:     "--helm-timeout",
		},
		{
			name:     "empty session namespace",
			args:     []string{"migrate", "plan", "--session-namespace", ""},
			category: domain.ErrorValidation,
			text:     "--session-namespace",
		},
		{
			name: "backup PVC",
			args: []string{
				"backup",
				"--dry-run",
				"--name",
				"daily",
				"--backend",
				"s3",
				"--bucket",
				"b",
			},
			category: domain.ErrorValidation,
			text:     "--source-pvc",
		},
		{
			name: "restore PVC",
			args: []string{
				"restore",
				"--dry-run",
				"--name",
				"daily",
				"--backend",
				"s3",
				"--bucket",
				"b",
			},
			category: domain.ErrorValidation,
			text:     "--destination-pvc",
		},
		{
			name: "Azure backend",
			args: []string{
				"backup",
				"--dry-run",
				"--source-pvc",
				"data",
				"--name",
				"daily",
				"--backend",
				"azure",
				"--bucket",
				"b",
			},
			category: domain.ErrorValidation,
			text:     "unsupported backend",
		},
		{
			name:     "output",
			args:     []string{"migrate", "plan", "--output", "toml"},
			category: domain.ErrorValidation,
			text:     "output format",
		},
		{
			name:     "log format",
			args:     []string{"migrate", "plan", "--log-format", "console"},
			category: domain.ErrorValidation,
			text:     "log format",
		},
		{
			name:     "log level",
			args:     []string{"migrate", "plan", "--log-level", "trace"},
			category: domain.ErrorValidation,
			text:     "log level",
		},
		{
			name:     "color mode",
			args:     []string{"migrate", "plan", "--color", "rainbow"},
			category: domain.ErrorValidation,
			text:     "color mode",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := executeCLI(t, tc.args...)
			if domain.CategoryOf(err) != tc.category || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
			}
		})
	}
}

func TestCommandArgumentContracts(t *testing.T) {
	cases := [][]string{
		{"migrate", "plan", "extra"},
		{"reserve", "extra"},
		{"copy", "extra"},
		{"migrate", "extra"},
		{"migrate-pod", "extra"},
		{"rename", "extra"},
		{"move", "extra"},
		{"backup", "extra"},
		{"restore", "extra"},
		{"migrate", "status", "one", "two"},
		{"migrate", "resume"},
		{"migrate", "resume", "one", "two"},
		{"migrate", "abort"},
		{"migrate", "rollback"},
		{"migrate", "cleanup"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			_, err := executeCLI(t, args...)
			if err == nil {
				t.Fatalf("command accepted invalid arguments: %v", args)
			}
		})
	}
}

func TestCompletionAllShellsAndInvalidInput(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		t.Run(shell, func(t *testing.T) {
			stdout, err := executeCLI(t, "completion", shell)
			if err != nil {
				t.Fatal(err)
			}

			if len(stdout) < 100 || !strings.Contains(strings.ToLower(stdout), "pvc-migrate") {
				t.Fatalf("completion output too small or missing command: %q", stdout)
			}
		})
	}

	_, err := executeCLI(t, "completion", "xonsh")
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("invalid shell error=%v category=%q", err, domain.CategoryOf(err))
	}

	_, err = executeCLI(t, "completion")
	if err == nil {
		t.Fatal("missing completion argument accepted")
	}
}

func TestVersionRejectsArgumentsAndNilOptionsAreUsable(t *testing.T) {
	command := NewRoot(Options{Version: "v0"})
	command.SetArgs([]string{"version"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	_, err := executeCLI(t, "version", "extra")
	if err == nil {
		t.Fatal("version accepted positional argument")
	}
}
