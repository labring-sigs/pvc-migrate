package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/output"
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
	wantCommands := []string{"activate", "backup", "completion", "copy", "final-sync", "live-backup", "migrate", "migrate-pod", "move", "rename", "reserve", "restore", "session", "version"}
	for _, name := range wantCommands {
		command, _, err := root.Find([]string{name})
		if err != nil || command == root || command.Name() != name {
			t.Fatalf("Find(%q) command=%v error=%v", name, command, err)
		}
	}
	if mv, _, err := root.Find([]string{"mv"}); err == nil && mv != root {
		t.Fatalf("removed mv alias resolved to command %v", mv)
	}
	liveBackup, _, err := root.Find([]string{"live-backup"})
	if err != nil || liveBackup.Name() != "live-backup" {
		t.Fatalf("live-backup alias command=%v error=%v", liveBackup, err)
	}
	for name, want := range map[string]string{
		"session-namespace": "pvc-migrate-system",
		"timeout":           "30m0s",
		"retries":           "3",
		"retry-backoff":     "2s",
		"helm-timeout":      "10m0s",
		"output":            "table",
		"log-format":        "text",
		"log-level":         "info",
		"color":             "auto",
		"stream-tool-logs":  "true",
		"no-compress":       "false",
		"yes":               "false",
		"tool-image":        "ghcr.io/labring-sigs/pvc-migrate:test",
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
	mutationPaths := [][]string{
		{"activate"}, {"backup"}, {"copy"}, {"final-sync"}, {"live-backup"}, {"migrate"}, {"migrate-pod"},
		{"move"}, {"rename"}, {"reserve"}, {"restore"}, {"session", "abort"},
		{"session", "cleanup"}, {"session", "resume"}, {"session", "rollback"},
		{"session", "cleanup-orphan"},
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
		{"activate", "plan"}, {"backup", "plan"}, {"copy", "plan"}, {"final-sync", "plan"}, {"live-backup", "plan"},
		{"migrate", "plan"}, {"migrate-pod", "plan"}, {"move", "plan"}, {"rename", "plan"},
		{"reserve", "plan"}, {"restore", "plan"}, {"session", "abort", "plan"},
		{"session", "cleanup", "plan"}, {"session", "resume", "plan"}, {"session", "rollback", "plan"},
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
	for _, name := range []string{"capacity-awareness", "destination-storage-class", "source-node", "target-node", "pod"} {
		if copyCommand.Flags().Lookup(name) == nil {
			t.Fatalf("copy is missing --%s", name)
		}
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
	for _, path := range [][]string{{"migrate"}, {"migrate", "plan"}, {"migrate-pod"}, {"migrate-pod", "plan"}} {
		command, _, err := root.Find(path)
		flag := command.Flags().Lookup("precopy-passes")
		if err != nil || flag == nil || flag.DefValue != "1" {
			t.Fatalf("%v precopy-passes flag=%v error=%v", path, flag, err)
		}
		if flag := command.Flags().Lookup("openebs-lvm-enable-shared"); flag == nil || flag.DefValue != "false" {
			t.Fatalf("%v openebs-lvm-enable-shared flag=%v", path, flag)
		}
	}
	for _, path := range [][]string{{"migrate-pod"}, {"migrate-pod", "plan"}} {
		command, _, err := root.Find(path)
		if err != nil || command.Flags().Lookup("force-reprovision") == nil {
			t.Fatalf("%v force-reprovision flag=%v error=%v", path, command.Flags().Lookup("force-reprovision"), err)
		}
	}

	session, _, err := root.Find([]string{"session"})
	if err != nil {
		t.Fatal(err)
	}
	status, _, err := root.Find([]string{"session", "status"})
	if err != nil || status.Flags().Lookup("dry-run") != nil {
		t.Fatalf("session status dry-run flag=%v error=%v", status.Flags().Lookup("dry-run"), err)
	}
	for _, name := range []string{"abort", "cleanup", "cleanup-orphan", "resume", "rollback", "status"} {
		command, _, err := session.Find([]string{name})
		if err != nil || command == session || command.Name() != name {
			t.Fatalf("session Find(%q) command=%v error=%v", name, command, err)
		}
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
	flags := &migrationFlags{}
	command := &cobra.Command{Use: "test"}
	flags.bind(command, true, true, true, true)
	flags.bindForceReprovision(command)

	for name, want := range map[string]string{
		"capacity-awareness":        "auto",
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

	flags.sessionID = "mig-fixed"
	flags.sourceNamespace = "source"
	flags.temporaryNamespace = "staging"
	flags.sourcePVCs = []string{"data", "logs"}
	flags.destinationPVCs = []string{"data-new", "logs-new"}
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
	options, err := flags.planOptions(state, domain.OperationMigratePod, true)
	if err != nil {
		t.Fatal(err)
	}
	if options.SessionID != "mig-fixed" || options.SourceNamespace != "source" || options.DestinationNamespace != "source" || options.TemporaryNamespace != "staging" || options.StagingNamespace != "staging" || options.SessionNamespace != "sessions" {
		t.Fatalf("namespaces and identity = %#v", options)
	}
	if options.Operation != domain.OperationMigratePod || options.PodName != "db-2" || options.SourceNode != "node-a" || options.TargetNode != "node-b" || options.DestinationClass != "fast" || options.CapacityAwareness != domain.CapacityAwarenessRequire || options.SwitchoverCandidate != "db-1" || !options.AllowLeaderDowntime || !options.ForceReprovision || !options.VerifyChecksum || !options.DeleteExtraneous || !options.OpenEBSLVMEnableShared || options.PrecopyPasses != 1 {
		t.Fatalf("options = %#v", options)
	}
	flags.sourcePVCs[0] = "mutated"
	flags.destinationPVCs[0] = "mutated"
	flags.strategies[0] = "mutated"
	if options.SourcePVCs[0] != "data" || options.DestinationPVCs[0] != "data-new" || options.Strategies[0] != "local" {
		t.Fatalf("plan options alias flag slices: %#v", options)
	}
}

func TestPlanOptionsDirectAndGeneratedIdentity(t *testing.T) {
	state := &rootState{global: globals{sessionNamespace: "sessions"}}
	flags := &migrationFlags{sourceNamespace: "app", destinationNamespace: "archive", temporaryNamespace: "staging"}
	options, err := flags.planOptions(state, domain.OperationCopy, false)
	if err != nil {
		t.Fatal(err)
	}
	if flags.sessionID == "" || options.SessionID != flags.sessionID || !strings.HasPrefix(options.SessionID, "mig-") {
		t.Fatalf("generated session ID flags=%q options=%q", flags.sessionID, options.SessionID)
	}
	if options.DestinationNamespace != "archive" || options.StagingNamespace != "archive" || options.TemporaryNamespace != "archive" {
		t.Fatalf("direct namespaces = %#v", options)
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

func TestRuntimeFreeCommandsRejectInvalidColorMode(t *testing.T) {
	for _, args := range [][]string{{"--color", "rainbow", "version"}, {"--color", "rainbow", "completion", "bash"}} {
		stdout, _, err := executeCLI(t, "", args...)
		if domain.CategoryOf(err) != domain.ErrorValidation || !strings.Contains(err.Error(), "color mode") {
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
			state := &rootState{options: Options{ErrOut: &output}, global: globals{logFormat: format, logLevel: "debug"}}
			logger, err := loggerFor(state)
			if err != nil {
				t.Fatal(err)
			}
			logger.Debug("configured", "format", format)
			if !strings.Contains(output.String(), "configured") || !strings.Contains(output.String(), format) {
				t.Fatalf("log output = %q", output.String())
			}
		})
	}
	state := &rootState{options: Options{ErrOut: io.Discard}, global: globals{logFormat: "console", logLevel: "info"}}
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
		if err := state.confirm(context.Background(), &cobra.Command{}, ""); domain.CategoryOf(err) != domain.ErrorValidation {
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
		if err := (&rootState{}).confirm(context.Background(), command, "data"); domain.CategoryOf(err) != domain.ErrorPrecondition {
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
		if err := (&rootState{}).confirm(context.Background(), command, "data"); !errors.Is(err, wantErr) {
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
	if !requiresOperationResumeApproval(domain.OperationRename, domain.PhasePlanned) || !requiresOperationResumeApproval(domain.OperationMove, domain.PhasePlanned) {
		t.Fatal("planned PVC identity phases require approval")
	}
	if requiresOperationResumeApproval(domain.OperationMigrate, domain.PhasePlanned) {
		t.Fatal("planned orchestrated phases do not require identity approval")
	}
}

func TestApprovalIdentityPrecedence(t *testing.T) {
	cases := []struct {
		flags migrationFlags
		want  string
	}{
		{flags: migrationFlags{podName: "db-0", sourcePVCs: []string{"data"}, sessionID: "mig"}, want: "db-0"},
		{flags: migrationFlags{sourcePVCs: []string{"data", "logs"}, sessionID: "mig"}, want: "data"},
		{flags: migrationFlags{sessionID: "mig"}, want: "mig"},
		{flags: migrationFlags{}, want: ""},
	}
	for _, tc := range cases {
		if got := approvalIdentity(&tc.flags); got != tc.want {
			t.Fatalf("approvalIdentity(%#v) = %q, want %q", tc.flags, got, tc.want)
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
	plan := &domain.MigrationPlan{SessionID: "mig", Ready: false, Checks: []domain.Check{{
		Name: "controller-adapter", Severity: domain.SeverityError, Message: "discover workload: controller has no safe pause adapter",
	}}}
	var guidance bytes.Buffer
	err = requireReadyWithOutput(runtime, plan, &guidance)
	if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(printed.String(), "mig") ||
		!strings.Contains(guidance.String(), "use the controller's native maintenance or pause procedure") {
		t.Fatalf("error=%v output=%q guidance=%q", err, printed.String(), guidance.String())
	}
	runtime.printer.Writer = cliFailingWriter{err: io.ErrClosedPipe}
	if err := requireReadyWithOutput(runtime, plan, io.Discard); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("print failure=%v", err)
	}
	printed.Reset()
	runtime.printer.Writer = &printed
	if err := requireReadyWithOutput(runtime, &domain.MigrationPlan{Ready: true}, io.Discard); err != nil || printed.Len() != 0 {
		t.Fatalf("ready error=%v output=%q", err, printed.String())
	}
}

func TestPlanFailureGuidanceSuggestsActionForControllerAndStorageChecks(t *testing.T) {
	var output bytes.Buffer
	plan := &domain.MigrationPlan{
		Checks: []domain.Check{
			{Name: "controller-adapter", Severity: domain.SeverityError, Message: "discover workload: controller has no safe pause adapter"},
			{Name: "storage-capacity", Severity: domain.SeverityError, Message: "capacity is insufficient"},
		},
	}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{
		"use the controller's native maintenance or pause procedure",
		"choose a compatible StorageClass or target node",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}
}

func TestPlanFailureGuidancePrioritizesStatefulSetAction(t *testing.T) {
	var output bytes.Buffer
	plan := &domain.MigrationPlan{Checks: []domain.Check{{
		Name:     "controller-adapter",
		Severity: domain.SeverityError,
		Message:  "discover workload: StatefulSet app/db PVC retention whenScaled is Delete; Retain is required",
	}}}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}
	if text := output.String(); !strings.Contains(text, "persistentVolumeClaimRetentionPolicy.whenScaled=Retain") {
		t.Fatalf("guidance=%q", text)
	}
}

func TestBucketFlagValidationMatrix(t *testing.T) {
	cases := []struct {
		name    string
		flags   bucketFlags
		pvcFlag string
		want    string
	}{
		{name: "source PVC", flags: bucketFlags{name: "daily", backend: "s3", bucket: "b"}, pvcFlag: "source-pvc", want: "--source-pvc"},
		{name: "backup name", flags: bucketFlags{pvc: "data", backend: "s3", bucket: "b"}, pvcFlag: "source-pvc", want: "--name"},
		{name: "backend", flags: bucketFlags{pvc: "data", name: "daily", bucket: "b"}, pvcFlag: "source-pvc", want: "--backend and --bucket"},
		{name: "bucket", flags: bucketFlags{pvc: "data", name: "daily", backend: "s3"}, pvcFlag: "source-pvc", want: "--backend and --bucket"},
		{name: "azure backend", flags: bucketFlags{pvc: "data", name: "daily", backend: "azure", bucket: "b"}, pvcFlag: "source-pvc", want: "unsupported backend"},
		{name: "gcs backend", flags: bucketFlags{pvc: "data", name: "daily", backend: "gcs", bucket: "b"}, pvcFlag: "source-pvc", want: "unsupported backend"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBucketFlags(&tc.flags, tc.pvcFlag)
			if domain.CategoryOf(err) != domain.ErrorValidation || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
			}
		})
	}
	valid := bucketFlags{pvc: "data", name: "daily", backend: "s3", bucket: "backups"}
	if err := validateBucketFlags(&valid, "source-pvc"); err != nil {
		t.Fatal(err)
	}
}

func TestCommandsRejectInvalidInputBeforeClusterAccess(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		category domain.ErrorCategory
		text     string
	}{
		{name: "final sync session", args: []string{"final-sync"}, category: domain.ErrorValidation, text: "--session"},
		{name: "activate session", args: []string{"activate"}, category: domain.ErrorValidation, text: "--session"},
		{name: "pod", args: []string{"migrate-pod"}, category: domain.ErrorValidation, text: "--pod"},
		{name: "pod plan", args: []string{"migrate-pod", "plan"}, category: domain.ErrorValidation, text: "--pod"},
		{name: "move namespace", args: []string{"move", "--source-pvc", "data"}, category: domain.ErrorValidation, text: "--destination-namespace"},
		{name: "rename destination", args: []string{"rename", "--source-pvc", "data"}, category: domain.ErrorValidation, text: "--destination-pvc"},
		{name: "orchestrated namespace", args: []string{"migrate", "--source-namespace", "app", "--destination-namespace", "other"}, category: domain.ErrorPrecondition, text: "source namespace"},
		{name: "negative precopy passes", args: []string{"migrate", "--precopy-passes", "-1"}, category: domain.ErrorValidation, text: "cannot be negative"},
		{name: "negative plan precopy passes", args: []string{"migrate", "plan", "--precopy-passes", "-1"}, category: domain.ErrorValidation, text: "cannot be negative"},
		{name: "zero retries", args: []string{"migrate", "plan", "--retries", "0"}, category: domain.ErrorValidation, text: "--retries"},
		{name: "negative retry backoff", args: []string{"migrate", "plan", "--retry-backoff", "-1s"}, category: domain.ErrorValidation, text: "--retry-backoff"},
		{name: "zero Helm timeout", args: []string{"migrate", "plan", "--helm-timeout", "0"}, category: domain.ErrorValidation, text: "--helm-timeout"},
		{name: "empty session namespace", args: []string{"migrate", "plan", "--session-namespace", ""}, category: domain.ErrorValidation, text: "--session-namespace"},
		{name: "backup PVC", args: []string{"backup", "--dry-run", "--name", "daily", "--backend", "s3", "--bucket", "b"}, category: domain.ErrorValidation, text: "--source-pvc"},
		{name: "restore PVC", args: []string{"restore", "--dry-run", "--name", "daily", "--backend", "s3", "--bucket", "b"}, category: domain.ErrorValidation, text: "--destination-pvc"},
		{name: "Azure backend", args: []string{"backup", "--dry-run", "--source-pvc", "data", "--name", "daily", "--backend", "azure", "--bucket", "b"}, category: domain.ErrorValidation, text: "unsupported backend"},
		{name: "output", args: []string{"migrate", "plan", "--output", "toml"}, category: domain.ErrorValidation, text: "output format"},
		{name: "log format", args: []string{"migrate", "plan", "--log-format", "console"}, category: domain.ErrorValidation, text: "log format"},
		{name: "log level", args: []string{"migrate", "plan", "--log-level", "trace"}, category: domain.ErrorValidation, text: "log level"},
		{name: "color mode", args: []string{"migrate", "plan", "--color", "rainbow"}, category: domain.ErrorValidation, text: "color mode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := executeCLI(t, "", tc.args...)
			if domain.CategoryOf(err) != tc.category || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("error=%v category=%q", err, domain.CategoryOf(err))
			}
		})
	}
}

func TestCommandArgumentContracts(t *testing.T) {
	cases := [][]string{
		{"migrate", "plan", "extra"},
		{"final-sync", "plan", "extra"},
		{"activate", "plan", "extra"},
		{"reserve", "extra"},
		{"copy", "extra"},
		{"final-sync", "extra"},
		{"activate", "extra"},
		{"migrate", "extra"},
		{"migrate-pod", "extra"},
		{"rename", "extra"},
		{"move", "extra"},
		{"backup", "extra"},
		{"restore", "extra"},
		{"session", "status", "one", "two"},
		{"session", "resume"},
		{"session", "resume", "one", "two"},
		{"session", "abort"},
		{"session", "rollback"},
		{"session", "cleanup"},
		{"session", "resume", "plan"},
		{"session", "abort", "plan"},
		{"session", "rollback", "plan"},
		{"session", "cleanup", "plan"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			_, _, err := executeCLI(t, "", args...)
			if err == nil {
				t.Fatalf("command accepted invalid arguments: %v", args)
			}
		})
	}
}

func TestCompletionAllShellsAndInvalidInput(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		t.Run(shell, func(t *testing.T) {
			stdout, _, err := executeCLI(t, "", "completion", shell)
			if err != nil {
				t.Fatal(err)
			}
			if len(stdout) < 100 || !strings.Contains(strings.ToLower(stdout), "pvc-migrate") {
				t.Fatalf("completion output too small or missing command: %q", stdout)
			}
		})
	}
	_, _, err := executeCLI(t, "", "completion", "xonsh")
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("invalid shell error=%v category=%q", err, domain.CategoryOf(err))
	}
	_, _, err = executeCLI(t, "", "completion")
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
	_, _, err := executeCLI(t, "", "version", "extra")
	if err == nil {
		t.Fatal("version accepted positional argument")
	}
}
