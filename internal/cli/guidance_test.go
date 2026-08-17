package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/client-go/kubernetes/fake"
)

func guidanceSession(phase domain.Phase) *domain.Session {
	session := domain.NewSession("mig-test", domain.NewSessionSpec(domain.OperationMigratePod, domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "pvc-migrate-system", DestinationNamespace: "app", SessionNamespace: "pvc-migrate-system",
		Volumes: []domain.VolumeSpec{{SourcePVC: domain.ObjectReference{Namespace: "app", Name: "data"}, SourcePV: domain.ObjectReference{Name: "pv-source"}}},
	}, domain.WorkloadSpec{Adapter: domain.WorkloadStandalone}, false), time.Now())
	session.Status.Phase = phase
	return session
}

func TestSessionStatusKeepsStructuredOutputSeparateFromGuidance(t *testing.T) {
	client := fake.NewClientset()
	store := kube.NewConfigMapSessionStore(client)
	session := guidanceSession(domain.PhaseCompleted)
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	command := NewRoot(Options{Version: "test", In: strings.NewReader(""), Out: &stdout, ErrOut: &stderr, runtimeFactory: func(state *rootState) (*commandRuntime, error) {
		return &commandRuntime{store: store, printer: printerFor(state)}, nil
	}})
	command.SetArgs([]string{"--output", "json", "session", "status", session.ID})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	var decoded domain.Session
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, stdout.String())
	}
	if decoded.ID != session.ID || !strings.Contains(stderr.String(), "Next steps") || strings.Contains(stdout.String(), "Next steps") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestSessionGuidancePreservesCommandConnectionSettings(t *testing.T) {
	client := fake.NewClientset()
	store := kube.NewConfigMapSessionStore(client)
	session := guidanceSession(domain.PhaseCompleted)
	session.Spec.SessionNamespace = "migration-control"
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	command := NewRoot(Options{Version: "test", Out: &stdout, ErrOut: &stderr, runtimeFactory: func(state *rootState) (*commandRuntime, error) {
		return &commandRuntime{store: store, printer: printerFor(state)}, nil
	}})
	command.SetArgs([]string{
		"--kubeconfig", "/tmp/config local",
		"--context", "cluster-a",
		"--session-namespace", "migration-control",
		"--timeout", "45m",
		"--retries", "5",
		"--retry-backoff", "3s",
		"--helm-timeout", "12m",
		"--stream-tool-logs=false",
		"--no-compress",
		"--tool-image", "registry.example/tool:dev",
		"--log-level", "debug",
		"--color", "never",
		"session", "status", session.ID,
	})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	text := stderr.String()
	pvcMigratePrefix := "pvc-migrate --kubeconfig '/tmp/config local' --context cluster-a --timeout=45m0s --retries=5 --retry-backoff=3s --helm-timeout=12m0s --stream-tool-logs=false --no-compress=true --session-namespace migration-control"
	for _, want := range []string{
		pvcMigratePrefix + " session status " + session.ID,
		"kubectl --kubeconfig '/tmp/config local' --context cluster-a --namespace app get pvc data",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}
	for _, excluded := range []string{"registry.example/tool:dev", "--log-level", "--color"} {
		if strings.Contains(text, excluded) {
			t.Fatalf("guidance=%q includes presentation or stored-session setting %q", text, excluded)
		}
	}
}

func TestShellQuoteUsesSelectedShellSyntax(t *testing.T) {
	for _, test := range []struct {
		name  string
		shell guidanceShell
		value string
		want  string
	}{
		{name: "posix apostrophe", shell: guidanceShellPOSIX, value: "config 'local'", want: `'config '"'"'local'"'"''`},
		{name: "powershell apostrophe", shell: guidanceShellPowerShell, value: "config 'local'", want: `'config ''local'''`},
		{name: "powershell splatting prefix", shell: guidanceShellPowerShell, value: "@prod", want: `'@prod'`},
		{name: "posix at sign", shell: guidanceShellPOSIX, value: "@prod", want: `'@prod'`},
		{name: "safe value", shell: guidanceShellPowerShell, value: "cluster-a", want: "cluster-a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := shellQuoteFor(test.value, test.shell); got != test.want {
				t.Fatalf("shellQuoteFor(%q)=%q want %q", test.value, got, test.want)
			}
		})
	}
}

func TestDetectGuidanceShell(t *testing.T) {
	for _, test := range []struct {
		name string
		goos string
		env  map[string]string
		want guidanceShell
	}{
		{name: "native windows", goos: "windows", want: guidanceShellPowerShell},
		{name: "windows msys", goos: "windows", env: map[string]string{"MSYSTEM": "MINGW64"}, want: guidanceShellPOSIX},
		{name: "unix powershell", goos: "linux", env: map[string]string{"PSModulePath": "/opt/powershell/modules"}, want: guidanceShellPowerShell},
		{name: "unix posix", goos: "darwin", want: guidanceShellPOSIX},
	} {
		t.Run(test.name, func(t *testing.T) {
			getenv := func(name string) string { return test.env[name] }
			if got := detectGuidanceShell(test.goos, getenv); got != test.want {
				t.Fatalf("detectGuidanceShell()=%v want %v", got, test.want)
			}
		})
	}
}

func TestGuidancePrefixesUsePowerShellQuoting(t *testing.T) {
	t.Setenv("PSModulePath", "/opt/powershell/modules")
	t.Setenv("MSYSTEM", "")
	root := NewRoot(Options{Version: "test"})
	for name, value := range map[string]string{
		"kubeconfig": "config 'local'",
		"context":    "@prod",
	} {
		if err := root.PersistentFlags().Set(name, value); err != nil {
			t.Fatal(err)
		}
	}
	command, _, err := root.Find([]string{"session", "status"})
	if err != nil {
		t.Fatal(err)
	}
	prefixes := guidancePrefixesForCommand(command, "pvc-migrate-system")
	if want := "pvc-migrate --kubeconfig 'config ''local''' --context '@prod'"; prefixes.pvcMigrate != want {
		t.Fatalf("pvc-migrate prefix=%q want %q", prefixes.pvcMigrate, want)
	}
	if want := "kubectl --kubeconfig 'config ''local''' --context '@prod'"; prefixes.kubectl != want {
		t.Fatalf("kubectl prefix=%q want %q", prefixes.kubectl, want)
	}
}

func TestSessionGuidanceCoversTerminalActions(t *testing.T) {
	for _, test := range []struct {
		phase domain.Phase
		want  []string
	}{
		{domain.PhaseCompleted, []string{"ConfigMap pvc-migrate-system/pvc-migrate-session-mig-test", "session rollback mig-test --dry-run", "--delete-rollback-pv", "--delete-session"}},
		{domain.PhaseFailed, []string{"session resume mig-test --dry-run", "session abort mig-test --dry-run"}},
		{domain.PhaseAborted, []string{"session cleanup mig-test", "--finalize"}},
		{domain.PhaseRolledBack, []string{"session cleanup mig-test", "--delete-session"}},
	} {
		t.Run(string(test.phase), func(t *testing.T) {
			var output bytes.Buffer
			if err := writeSessionGuidance(&output, guidanceSession(test.phase)); err != nil {
				t.Fatal(err)
			}
			for _, want := range test.want {
				if !strings.Contains(output.String(), want) {
					t.Fatalf("phase=%s guidance=%q missing %q", test.phase, output.String(), want)
				}
			}
		})
	}
}

func TestSessionGuidanceIncludesCustomNamespaceAndApproval(t *testing.T) {
	session := guidanceSession(domain.PhaseWarmCopied)
	session.Spec.SessionNamespace = "migration-control"
	var output bytes.Buffer
	if err := writeSessionGuidance(&output, session); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{"--session-namespace migration-control", "--yes", "--dry-run=false"} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}
}

func TestSessionGuidanceAbortIncludesValidationAndApproval(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved, domain.PhaseWarmCopying, domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced} {
		t.Run(string(phase), func(t *testing.T) {
			var output bytes.Buffer
			if err := writeSessionGuidance(&output, guidanceSession(phase)); err != nil {
				t.Fatal(err)
			}
			text := output.String()
			for _, want := range []string{
				"session abort mig-test --dry-run",
				"--yes session abort mig-test --dry-run=false",
			} {
				if !strings.Contains(text, want) {
					t.Fatalf("guidance=%q missing %q", text, want)
				}
			}
		})
	}

	copySession := guidanceSession(domain.PhaseWarmCopied)
	copySession.Spec = domain.NewSessionSpec(domain.OperationCopy, copySession.Spec.SessionCommon, domain.WorkloadSpec{}, false)
	var output bytes.Buffer
	if err := writeSessionGuidance(&output, copySession); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "session abort") {
		t.Fatalf("completed copy guidance contains redundant abort action: %q", output.String())
	}
}

func TestTransferDryRunGuidanceUsesOperationName(t *testing.T) {
	for _, operation := range []string{"backup plan", "live-backup plan", "restore plan"} {
		var output bytes.Buffer
		if err := writeTransferDryRunGuidance(&output, operation, "app", "data"); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), operation+" dry-run completed") {
			t.Fatalf("operation=%q guidance=%q", operation, output.String())
		}
	}
}

func TestTransferDryRunGuidanceUsesKubectlConnectionSettings(t *testing.T) {
	var output bytes.Buffer
	if err := writeTransferDryRunGuidance(&output, "backup plan", "app", "data", "kubectl --kubeconfig '/tmp/config local' --context cluster-a"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "kubectl --kubeconfig '/tmp/config local' --context cluster-a --namespace app get pvc data") {
		t.Fatalf("guidance=%q", output.String())
	}
}

func TestSessionGuidanceUsesOperationSpecificCompletionPaths(t *testing.T) {
	reserve := guidanceSession(domain.PhaseReserved)
	reserve.Spec = domain.NewSessionSpec(domain.OperationReserve, reserve.Spec.SessionCommon, domain.WorkloadSpec{}, false)
	var reserveOutput bytes.Buffer
	if err := writeSessionGuidance(&reserveOutput, reserve); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reserveOutput.String(), "copy --session mig-test --dry-run") {
		t.Fatalf("reserve guidance=%q", reserveOutput.String())
	}

	copySession := guidanceSession(domain.PhaseWarmCopied)
	copySession.Spec = domain.NewSessionSpec(domain.OperationCopy, copySession.Spec.SessionCommon, domain.WorkloadSpec{}, false)
	var copyOutput bytes.Buffer
	if err := writeSessionGuidance(&copyOutput, copySession); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Keep the copied PVC", "Discard the copied PVC", "--finalize --delete-session"} {
		if !strings.Contains(copyOutput.String(), want) {
			t.Fatalf("copy guidance=%q missing %q", copyOutput.String(), want)
		}
	}

	failed := guidanceSession(domain.PhaseFailed)
	failed.Status.ResumeFrom = domain.PhaseActivating
	var failedOutput bytes.Buffer
	if err := writeSessionGuidance(&failedOutput, failed); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(failedOutput.String(), "rollback after cutover failure") || strings.Contains(failedOutput.String(), "Abort pre-cutover") {
		t.Fatalf("failed guidance=%q", failedOutput.String())
	}
}

func TestSessionGuidanceKeepsPVCIdentityCleanupFreeOfPVDeletion(t *testing.T) {
	for _, operation := range []domain.Operation{domain.OperationRename, domain.OperationMove} {
		t.Run(string(operation), func(t *testing.T) {
			session := guidanceSession(domain.PhaseCompleted)
			session.Spec = domain.NewSessionSpec(operation, session.Spec.SessionCommon, domain.WorkloadSpec{}, false)
			var output bytes.Buffer
			if err := writeSessionGuidance(&output, session); err != nil {
				t.Fatal(err)
			}
			text := output.String()
			if !strings.Contains(text, "session cleanup mig-test --finalize --delete-session") {
				t.Fatalf("guidance=%q", text)
			}
			if strings.Contains(text, "--delete-rollback-pv") || strings.Contains(text, "--delete-temporary") {
				t.Fatalf("identity cleanup contains storage deletion flags: %q", text)
			}
		})
	}
}

func TestSessionListGuidancePointsToStatusCommand(t *testing.T) {
	var output bytes.Buffer
	if err := writeSessionListGuidance(&output, "migration-control", []*domain.Session{guidanceSession(domain.PhaseFailed)}); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, "pvc-migrate --session-namespace migration-control session status SESSION") {
		t.Fatalf("guidance=%q", text)
	}
}

type guidanceErrorCommand struct{ stderr bytes.Buffer }

func (c *guidanceErrorCommand) ErrOrStderr() io.Writer { return &c.stderr }

func TestErrorGuidanceIncludesRecoveryInspection(t *testing.T) {
	command := &guidanceErrorCommand{}
	if err := reportSessionLookupError(command, "migration-control", "mig-test", domain.NewError(domain.ErrorValidation, "get session", "missing")); err == nil {
		t.Fatal("expected original lookup error")
	}
	text := command.stderr.String()
	for _, want := range []string{
		"session status",
		"configmap pvc-migrate-session-mig-test",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}
}

func TestSessionCreationErrorGuidanceInspectsPotentialRecord(t *testing.T) {
	command := &guidanceErrorCommand{}
	creationErr := domain.NewError(domain.ErrorKubernetes, "create session", "connection reset after create")
	if err := reportSessionCreationError(command, "migration-control", "mig-test", creationErr); !errors.Is(err, creationErr) {
		t.Fatalf("returned error=%v", err)
	}
	text := command.stderr.String()
	for _, want := range []string{"session status mig-test", "get configmap pvc-migrate-session-mig-test"} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}
}

func TestTransferErrorGuidanceIncludesPVCInspection(t *testing.T) {
	command := &guidanceErrorCommand{}
	if err := reportTransferError(command, "backup", "app", "data", domain.NewError(domain.ErrorPrecondition, "backup", "offline")); err == nil {
		t.Fatal("expected original transfer error")
	}
	if !strings.Contains(command.stderr.String(), "kubectl --namespace app get pvc data") {
		t.Fatalf("guidance=%q", command.stderr.String())
	}
}

func TestRuntimeErrorGuidanceAvoidsPVCInspection(t *testing.T) {
	command := &guidanceErrorCommand{}
	if err := reportRuntimeError(command, domain.NewError(domain.ErrorValidation, "flags", "unsupported output format")); err == nil {
		t.Fatal("expected original runtime error")
	}
	text := command.stderr.String()
	if strings.Contains(text, "get pvc") || !strings.Contains(text, "--kubeconfig") || !strings.Contains(text, "--output") {
		t.Fatalf("guidance=%q", text)
	}
}

func TestCleanupErrorGuidanceUsesActualLeaseName(t *testing.T) {
	command := &guidanceErrorCommand{}
	session := guidanceSession(domain.PhaseCompleted)
	if err := reportCleanupError(command, session, app.CleanupOptions{DeleteSession: true}, domain.NewError(domain.ErrorKubernetes, "delete session", "delete failed")); err == nil {
		t.Fatal("expected original cleanup error")
	}
	text := command.stderr.String()
	if !strings.Contains(text, "get configmap "+kube.SessionConfigMapName(session.ID)) {
		t.Fatalf("guidance=%q", text)
	}
	if !strings.Contains(text, "get lease "+kube.SessionLockName(session.ID)) {
		t.Fatalf("guidance=%q", text)
	}
	if strings.Contains(text, "get configmap,lease "+kube.SessionConfigMapName(session.ID)) {
		t.Fatalf("guidance still uses ConfigMap name for Lease: %q", text)
	}
}

func TestCleanupErrorGuidanceFindsJoinedLeaseDeletion(t *testing.T) {
	command := &guidanceErrorCommand{}
	session := guidanceSession(domain.PhaseCompleted)
	cleanupErr := errors.Join(
		domain.NewError(domain.ErrorKubernetes, "cleanup", "resource cleanup failed"),
		domain.NewError(domain.ErrorKubernetes, "delete session lock", "Lease deletion failed"),
	)
	if err := reportCleanupError(command, session, app.CleanupOptions{DeleteSession: true}, cleanupErr); !errors.Is(err, cleanupErr) {
		t.Fatalf("returned error=%v", err)
	}
	text := command.stderr.String()
	if !strings.Contains(text, "session record removal") || !strings.Contains(text, "get lease "+kube.SessionLockName(session.ID)) {
		t.Fatalf("guidance=%q", text)
	}
}

func TestCleanupLockErrorGuidanceDescribesRetryableCleanup(t *testing.T) {
	command := &guidanceErrorCommand{}
	session := guidanceSession(domain.PhaseCompleted)
	options := app.CleanupOptions{DeleteTemporary: true, DeleteRollback: true, Finalize: true, DeleteSession: true}
	cleanupErr := domain.NewError(domain.ErrorKubernetes, "acquire session lock", "read Lease failed")
	if err := reportCleanupError(command, session, options, cleanupErr); !errors.Is(err, cleanupErr) {
		t.Fatalf("returned error=%v", err)
	}
	text := command.stderr.String()
	for _, want := range []string{
		"Cleanup stopped before confirmed completion",
		"session status " + session.ID,
		"session cleanup " + session.ID + " --delete-temporary --delete-rollback-pv --finalize --delete-session --dry-run",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}
	if strings.Contains(text, "session record removal") || strings.Contains(text, "cleanup-orphan") {
		t.Fatalf("lock acquisition was misreported as record removal: %q", text)
	}
}

func TestCleanupPodBlockerGuidanceNamesOwnerAndPreservesConnection(t *testing.T) {
	for _, test := range []struct {
		kind     string
		resource string
	}{
		{kind: "Job", resource: "job"},
		{kind: "Deployment", resource: "deployment"},
	} {
		t.Run(test.kind, func(t *testing.T) {
			var stderr bytes.Buffer
			root := NewRoot(Options{Version: "test", ErrOut: &stderr})
			for name, value := range map[string]string{"kubeconfig": "/tmp/config local", "context": "cluster-a"} {
				if err := root.PersistentFlags().Set(name, value); err != nil {
					t.Fatal(err)
				}
			}
			command, _, err := root.Find([]string{"session", "cleanup"})
			if err != nil {
				t.Fatal(err)
			}
			blocker := &app.CleanupPodBlockerError{
				PVCNamespace: "system", PVCName: "data-migrated", PodNamespace: "system", PodName: "copy-tool",
				PodPhase: "Failed", OwnerKind: test.kind, OwnerName: "copy-owner", OwnerVerified: true, SessionOwned: true, Terminal: true,
			}
			if err := writeCleanupPodBlockerGuidance(&stderr, command, blocker); err != nil {
				t.Fatal(err)
			}
			prefix := "kubectl --kubeconfig '/tmp/config local' --context cluster-a --namespace system"
			for _, want := range []string{
				prefix + " get " + test.resource + " copy-owner -o wide",
				prefix + " delete " + test.resource + " copy-owner --ignore-not-found=true --wait=true",
				prefix + " delete pod copy-tool --ignore-not-found=true --wait=true",
			} {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("guidance=%q missing %q", stderr.String(), want)
				}
			}
		})
	}
}

func TestCleanupPodBlockerGuidanceDoesNotDeleteUnverifiedOwner(t *testing.T) {
	var output bytes.Buffer
	blocker := &app.CleanupPodBlockerError{
		PVCNamespace: "system", PVCName: "data-migrated", PodNamespace: "system", PodName: "copy-tool",
		OwnerKind: "Job", OwnerName: "copy-owner", SessionOwned: true, Terminal: true,
	}
	if err := writeCleanupPodBlockerGuidance(&output, &guidanceErrorCommand{}, blocker); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, "UID could not be verified") || strings.Contains(text, "delete job copy-owner") {
		t.Fatalf("unsafe unverified owner guidance=%q", text)
	}
}

func TestWarmCopyMountGuidanceIncludesAbortCleanupAndOfflineRetry(t *testing.T) {
	for _, test := range []struct {
		operation domain.Operation
		want      string
		absent    string
	}{
		{operation: domain.OperationMigratePod, want: "--precopy-passes 0", absent: "without --online"},
		{operation: domain.OperationCopy, want: "without --online", absent: "--precopy-passes"},
	} {
		t.Run(string(test.operation), func(t *testing.T) {
			var output bytes.Buffer
			session := guidanceSession(domain.PhaseFailed)
			if test.operation == domain.OperationCopy {
				session.Spec = domain.NewSessionSpec(domain.OperationCopy, session.Spec.SessionCommon, domain.WorkloadSpec{}, true, session.Spec.WorkflowOptions())
			}
			if err := writeWarmCopyMountGuidance(&output, &guidanceErrorCommand{}, session); err != nil {
				t.Fatal(err)
			}
			text := output.String()
			for _, want := range []string{
				"session abort mig-test --dry-run",
				"session cleanup mig-test --delete-temporary --delete-rollback-pv --finalize --delete-session --dry-run",
				test.want,
			} {
				if !strings.Contains(text, want) {
					t.Fatalf("guidance=%q missing %q", text, want)
				}
			}
			if strings.Contains(text, test.absent) {
				t.Fatalf("guidance=%q contains operation-invalid advice %q", text, test.absent)
			}
		})
	}
}

func TestCopyPlanFailureGuidanceDoesNotSuggestPrecopyPasses(t *testing.T) {
	var output bytes.Buffer
	plan := &domain.MigrationPlan{
		SessionSpec: domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{}, domain.WorkloadSpec{}, true),
		Checks:      []domain.Check{{Name: "warm-copy-mount", Severity: domain.SeverityError}},
	}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}
	if text := output.String(); !strings.Contains(text, "without --online") || strings.Contains(text, "--precopy-passes") {
		t.Fatalf("copy plan guidance=%q", text)
	}
}

func TestApprovalErrorGuidanceStatesProtectedActionDidNotStart(t *testing.T) {
	command := &guidanceErrorCommand{}
	approvalErr := domain.WrapError(domain.ErrorTimeout, "approval", "typed approval canceled", context.Canceled)
	if err := reportApprovalError(command, approvalErr); !errors.Is(err, approvalErr) {
		t.Fatalf("returned error=%v", err)
	}
	text := command.stderr.String()
	if !strings.Contains(text, "before the protected action began") || !strings.Contains(text, "--dry-run") || !strings.Contains(text, "--yes") {
		t.Fatalf("guidance=%q", text)
	}
}
