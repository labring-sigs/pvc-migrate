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
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	"k8s.io/client-go/kubernetes/fake"
)

func guidanceSession(phase domain.Phase) *domain.Session {
	session := domain.NewSession(
		"mig-test",
		domain.NewPodMigrationSessionSpec(domain.SessionCommon{
			SourceNamespace:      "app",
			TemporaryNamespace:   "pvc-migrate-system",
			DestinationNamespace: "app",
			SessionNamespace:     "pvc-migrate-system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Namespace: "app",
						Name:      "data",
						UID:       "source-pvc-uid",
					},
					SourcePV: domain.ObjectReference{Name: "pv-source", UID: "source-pv-uid"},
					DestinationPVC: domain.ObjectReference{
						Namespace: "pvc-migrate-system",
						Name:      "data-migrated",
					},
				},
			},
		}, domain.WorkloadSpec{
			Adapter: domain.WorkloadStandalone,
			Pod: domain.ObjectReference{
				Namespace: "app",
				Name:      "application",
				UID:       "application-uid",
			},
		}, domain.SessionWorkflowOptions{}, 1, false),
		time.Now(),
	)
	session.Status.Phase = phase

	return session
}

func guidancePrefixesForSession(session *domain.Session) guidancePrefixes {
	return guidancePrefixes{
		pvcMigrate: sessionCommandPrefix(session.Spec.SessionNamespace),
		kubectl:    "kubectl",
	}
}

func TestSessionStatusKeepsStructuredOutputSeparateFromGuidance(t *testing.T) {
	client := fake.NewClientset()
	store := kube.NewConfigMapSessionStore(client)

	session := guidanceSession(domain.PhaseCompleted)
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer

	command := NewRoot(
		Options{
			Version: "test",
			In:      strings.NewReader(""),
			Out:     &stdout,
			ErrOut:  &stderr,
			runtimeFactory: func(state *rootState) (*commandRuntime, error) {
				return &commandRuntime{store: store, printer: printerFor(state)}, nil
			},
		},
	)
	command.SetArgs([]string{"--output", "json", "migrate-pod", "status", session.ID})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	var decoded domain.Session
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, stdout.String())
	}

	if decoded.ID != session.ID || !strings.Contains(stderr.String(), "Next steps") ||
		strings.Contains(stdout.String(), "Next steps") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestCompletedBackupGuidanceOmitsRollback(t *testing.T) {
	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{SourceNamespace: "app", SessionNamespace: "pvc-migrate-system"},

		true,
		domain.SessionWorkflowOptions{},
	)

	spec.Backup.SourcePVC = domain.ObjectReference{Namespace: "app", Name: "data", UID: "pvc-uid"}
	spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data", UID: "pv-uid"}
	spec.Backup.Backend = "s3"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	session := domain.NewSession("backup-test", spec, time.Now())
	session.Status.Phase = domain.PhaseCompleted

	var output bytes.Buffer
	if err := writeSessionGuidance(
		&output,
		session,
		guidancePrefixesForSession(session),
	); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if strings.Contains(text, "backup rollback ") ||
		strings.Contains(text, "--delete-rollback-pv") ||
		!strings.Contains(text, "Validate cleanup:") ||
		!strings.Contains(text, "published recovery point") {
		t.Fatalf("backup guidance=%q", text)
	}
}

func TestAbortedBackupGuidanceUsesBackupTerms(t *testing.T) {
	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{SourceNamespace: "app", SessionNamespace: "pvc-migrate-system"},

		true,
		domain.SessionWorkflowOptions{},
	)

	spec.Backup.SourcePVC = domain.ObjectReference{Namespace: "app", Name: "data", UID: "pvc-uid"}
	spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data", UID: "pv-uid"}
	spec.Backup.Backend = "s3"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	session := domain.NewSession("backup-test", spec, time.Now())
	session.Status.Phase = domain.PhaseAborted

	var output bytes.Buffer
	if err := writeSessionGuidance(
		&output,
		session,
		guidancePrefixesForSession(session),
	); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if strings.Contains(text, "retained resources") ||
		!strings.Contains(text, "source workload and PVC") ||
		!strings.Contains(text, "Delete retained credentials and session") {
		t.Fatalf("backup guidance=%q", text)
	}
}

func TestRestoreGuidanceUsesRestoreLifecycle(t *testing.T) {
	spec := domain.NewSessionSpec(
		domain.OperationRestore,
		domain.SessionCommon{
			SourceNamespace:      "app",
			DestinationNamespace: "app",
			SessionNamespace:     "pvc-migrate-system",
		},
		false,
		domain.SessionWorkflowOptions{},
	)
	spec.Restore.DestinationPVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "data",
	}
	spec.Restore.Backend = "s3"
	spec.Restore.Bucket = "backups"
	spec.Restore.Name = "daily"
	session := domain.NewSession("restore-test", spec, time.Now())
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseWarmCopying

	var output bytes.Buffer
	if err := writeSessionGuidance(
		&output,
		session,
		guidancePrefixesForSession(session),
	); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	for _, want := range []string{
		"restore status restore-test",
		"restore resume restore-test",
		"restore abort restore-test",
		"Verify active PVCs: kubectl --namespace app get pvc data",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("restore guidance=%q missing %q", text, want)
		}
	}

	for _, forbidden := range []string{
		"migrate status restore-test",
		"restore rollback restore-test",
		"--delete-temporary",
		"--delete-rollback-pv",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("restore guidance=%q contains %q", text, forbidden)
		}
	}
}

func TestCompletedRestoreResultLoadsDurableSessionAndPrintsGuidance(t *testing.T) {
	client := fake.NewClientset()
	store := kube.NewConfigMapSessionStore(client)
	spec := domain.NewSessionSpec(
		domain.OperationRestore,
		domain.SessionCommon{
			SourceNamespace:      "app",
			DestinationNamespace: "app",
			SessionNamespace:     "migration-control",
		},
		false,
		domain.SessionWorkflowOptions{},
	)
	spec.Restore.DestinationPVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "restored-data",
	}
	spec.Restore.Backend = domain.BackupBackendS3
	spec.Restore.Bucket = "backups"
	spec.Restore.Name = "daily"
	session := domain.NewSession("restore-result", spec, time.Now())

	session.Status.Phase = domain.PhaseCompleted
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	objectStore, err := objectstore.NewConfigOnly(objectstore.Config{
		Bucket: "backups",
		Name:   "daily",
	})
	if err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer

	root := NewRoot(Options{Version: "test", Out: &stdout, ErrOut: &stderr})

	command, _, err := root.Find([]string{"restore"})
	if err != nil {
		t.Fatal(err)
	}

	state := &rootState{
		options: Options{Out: &stdout, ErrOut: &stderr},
		global:  globals{sessionNamespace: "migration-control"},
	}

	err = state.printObjectTransferResult(
		command,
		&commandRuntime{store: store},
		&bucketFlags{
			id:        session.ID,
			namespace: "app",
			pvc:       "restored-data",
			name:      "daily",
		},
		"restore",
		true,
		false,
		&backup.Plan{Path: "/"},
		objectStore,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(stdout.String(), "restore-result") {
		t.Fatalf("stdout=%q", stdout.String())
	}

	for _, want := range []string{
		"restore status restore-result",
		"restore cleanup restore-result",
		"--session-namespace migration-control",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr=%q missing %q", stderr.String(), want)
		}
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

	command := NewRoot(
		Options{
			Version: "test",
			Out:     &stdout,
			ErrOut:  &stderr,
			runtimeFactory: func(state *rootState) (*commandRuntime, error) {
				return &commandRuntime{store: store, printer: printerFor(state)}, nil
			},
		},
	)
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
		"migrate-pod", "status", session.ID,
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	text := stderr.String()

	pvcMigratePrefix := "pvc-migrate --kubeconfig '/tmp/config local' --context cluster-a --timeout=45m0s --retries=5 --retry-backoff=3s --helm-timeout=12m0s --stream-tool-logs=false --no-compress=true --session-namespace migration-control"
	for _, want := range []string{
		pvcMigratePrefix + " migrate-pod status " + session.ID,
		"kubectl --kubeconfig '/tmp/config local' --context cluster-a --namespace app get pvc data",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}

	for _, excluded := range []string{"registry.example/tool:dev", "--log-level", "--color"} {
		if strings.Contains(text, excluded) {
			t.Fatalf(
				"guidance=%q includes presentation or stored-session setting %q",
				text,
				excluded,
			)
		}
	}
}

func TestControllerGuidancePreservesModeAndWorkflowNamespace(t *testing.T) {
	root := NewRoot(
		Options{Version: "test", In: strings.NewReader(""), Out: io.Discard, ErrOut: io.Discard},
	)
	for name, value := range map[string]string{
		"kubeconfig":         "/tmp/config local",
		"mode":               "controller",
		"session-namespace":  "controller-system",
		"workflow-namespace": "application",
	} {
		if err := root.PersistentFlags().Set(name, value); err != nil {
			t.Fatal(err)
		}
	}

	command, _, err := root.Find([]string{"copy", "status"})
	if err != nil {
		t.Fatal(err)
	}

	prefix := guidancePrefixesForCommand(command, "application").pvcMigrate
	for _, want := range []string{
		"--kubeconfig '/tmp/config local'",
		"--mode=controller",
		"--workflow-namespace application",
	} {
		if !strings.Contains(prefix, want) {
			t.Fatalf("controller guidance prefix=%q missing %q", prefix, want)
		}
	}

	for _, forbidden := range []string{
		"--session-namespace application",
		"--session-namespace controller-system",
	} {
		if strings.Contains(prefix, forbidden) {
			t.Fatalf("controller guidance prefix=%q contains %q", prefix, forbidden)
		}
	}
}

func TestCopySessionDryRunGuidanceContinuesWithCopyCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer

	root := NewRoot(Options{Version: "test", Out: &stdout, ErrOut: &stderr})
	for name, value := range map[string]string{
		"kubeconfig":        "/tmp/config local",
		"context":           "cluster-a",
		"timeout":           "45m",
		"session-namespace": "migration-control",
	} {
		if err := root.PersistentFlags().Set(name, value); err != nil {
			t.Fatal(err)
		}
	}

	command, _, err := root.Find([]string{"copy"})
	if err != nil {
		t.Fatal(err)
	}

	session := guidanceSession(domain.PhaseReserved)
	session.Spec.SessionNamespace = "migration-control"
	session.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		session.Spec.SessionCommon,

		true,
		domain.SessionWorkflowOptions{SourceNode: "node-a"},
	)

	runtime := &commandRuntime{printer: output.Printer{Writer: &stdout, Format: output.JSON}}

	flags := &copyFlags{sourceNode: "node-a", online: true}
	if err := printCopyDryRunResult(command, runtime, session, flags); err != nil {
		t.Fatal(err)
	}

	var decoded domain.Session
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, stdout.String())
	}

	want := "pvc-migrate --kubeconfig '/tmp/config local' --context cluster-a --timeout=45m0s --session-namespace migration-control copy --session mig-test --online --source-node node-a --dry-run=false"
	if !strings.Contains(stderr.String(), want) {
		t.Fatalf("guidance=%q missing %q", stderr.String(), want)
	}

	if strings.Contains(stderr.String(), "copy resume") {
		t.Fatalf("copy dry-run guidance points to generic resume: %q", stderr.String())
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

	command, _, err := root.Find([]string{"migrate-pod", "status"})
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
		{domain.PhaseCompleted, []string{"ConfigMap pvc-migrate-system/pvc-migrate-session-mig-test", "migrate-pod rollback mig-test --dry-run", "--delete-rollback-pv", "--delete-session"}},
		{domain.PhaseFailed, []string{"migrate-pod resume mig-test --dry-run", "migrate-pod abort mig-test --dry-run"}},
		{domain.PhaseAborted, []string{"migrate-pod cleanup mig-test", "--finalize"}},
		{domain.PhaseRolledBack, []string{"migrate-pod cleanup mig-test", "--delete-session"}},
	} {
		t.Run(string(test.phase), func(t *testing.T) {
			var output bytes.Buffer

			session := guidanceSession(test.phase)
			if err := writeSessionGuidance(
				&output,
				session,
				guidancePrefixesForSession(session),
			); err != nil {
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

func TestSessionGuidanceNamesCRDRecord(t *testing.T) {
	var output bytes.Buffer

	session := guidanceSession(domain.PhasePlanned)

	session.Backend = kube.SessionBackendCRD

	if err := writeSessionGuidance(
		&output,
		session,
		guidancePrefixesForSession(session),
	); err != nil {
		t.Fatal(err)
	}

	text := output.String()

	if !strings.Contains(
		text,
		"Record: ClusterPodMigration "+session.ID,
	) {
		t.Fatalf("guidance=%q", text)
	}

	if strings.Contains(text, "Record: ConfigMap") {
		t.Fatalf("CRD guidance still names ConfigMap: %q", text)
	}
}

func TestDestinationCapacityFailureGuidanceRequiresNewSession(t *testing.T) {
	session := guidanceSession(domain.PhaseFailed)
	session.Status.FailureReason = domain.FailureDestinationCapacityExhausted

	var output bytes.Buffer
	if err := writeSessionGuidance(
		&output,
		session,
		guidancePrefixesForSession(session),
	); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	for _, want := range []string{
		"cannot resume because its destination capacity is immutable",
		"migrate-pod abort mig-test --dry-run",
		"migrate-pod cleanup mig-test --delete-temporary --delete-rollback-pv --finalize --delete-session --dry-run",
		"new --session and a larger --destination-capacity",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}

	if strings.Contains(text, "migrate-pod resume") {
		t.Fatalf("guidance offers an ineffective resume: %q", text)
	}
}

func TestSessionGuidanceIncludesCustomNamespaceAndApproval(t *testing.T) {
	session := guidanceSession(domain.PhaseWarmCopied)
	session.Spec.SessionNamespace = "migration-control"

	var output bytes.Buffer
	if err := writeSessionGuidance(
		&output,
		session,
		guidancePrefixesForSession(session),
	); err != nil {
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

			session := guidanceSession(phase)
			if err := writeSessionGuidance(
				&output,
				session,
				guidancePrefixesForSession(session),
			); err != nil {
				t.Fatal(err)
			}

			text := output.String()
			for _, want := range []string{
				"migrate-pod abort mig-test --dry-run",
				"--yes migrate-pod abort mig-test --dry-run=false",
			} {
				if !strings.Contains(text, want) {
					t.Fatalf("guidance=%q missing %q", text, want)
				}
			}
		})
	}

	copySession := guidanceSession(domain.PhaseWarmCopied)
	copySession.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		copySession.Spec.SessionCommon,

		false,
		domain.SessionWorkflowOptions{},
	)

	var output bytes.Buffer
	if err := writeSessionGuidance(
		&output,
		copySession,
		guidancePrefixesForSession(copySession),
	); err != nil {
		t.Fatal(err)
	}

	if strings.Contains(output.String(), "copy abort") {
		t.Fatalf("completed copy guidance contains redundant abort action: %q", output.String())
	}
}

func TestTransferDryRunGuidanceUsesOperationName(t *testing.T) {
	for _, operation := range []string{"backup plan", "restore plan"} {
		var output bytes.Buffer
		if err := writeTransferDryRunGuidance(
			&output,
			operation,
			"app",
			"data",
			"kubectl",
		); err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(output.String(), operation+" dry-run completed") {
			t.Fatalf("operation=%q guidance=%q", operation, output.String())
		}
	}
}

func TestTransferDryRunGuidanceUsesKubectlConnectionSettings(t *testing.T) {
	var output bytes.Buffer
	if err := writeTransferDryRunGuidance(
		&output,
		"backup plan",
		"app",
		"data",
		"kubectl --kubeconfig '/tmp/config local' --context cluster-a",
	); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(
		output.String(),
		"kubectl --kubeconfig '/tmp/config local' --context cluster-a --namespace app get pvc data",
	) {
		t.Fatalf("guidance=%q", output.String())
	}
}

func TestSessionGuidanceUsesOperationSpecificCompletionPaths(t *testing.T) {
	reserve := guidanceSession(domain.PhaseReserved)
	reserve.Spec = domain.NewSessionSpec(
		domain.OperationReserve,
		reserve.Spec.SessionCommon,

		false,
		domain.SessionWorkflowOptions{},
	)

	var reserveOutput bytes.Buffer
	if err := writeSessionGuidance(
		&reserveOutput,
		reserve,
		guidancePrefixesForSession(reserve),
	); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(reserveOutput.String(), "copy --session mig-test --dry-run") {
		t.Fatalf("reserve guidance=%q", reserveOutput.String())
	}

	copySession := guidanceSession(domain.PhaseWarmCopied)
	copySession.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		copySession.Spec.SessionCommon,

		false,
		domain.SessionWorkflowOptions{},
	)

	var copyOutput bytes.Buffer
	if err := writeSessionGuidance(
		&copyOutput,
		copySession,
		guidancePrefixesForSession(copySession),
	); err != nil {
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
	if err := writeSessionGuidance(
		&failedOutput,
		failed,
		guidancePrefixesForSession(failed),
	); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(failedOutput.String(), "rollback after cutover failure") ||
		strings.Contains(failedOutput.String(), "Abort pre-cutover") {
		t.Fatalf("failed guidance=%q", failedOutput.String())
	}
}

func TestSessionGuidanceKeepsPVCIdentityCleanupFreeOfPVDeletion(t *testing.T) {
	for _, operation := range []domain.Operation{domain.OperationRename, domain.OperationMove} {
		t.Run(string(operation), func(t *testing.T) {
			session := guidanceSession(domain.PhaseCompleted)
			session.Spec = domain.NewSessionSpec(
				operation,
				session.Spec.SessionCommon,

				false,
				domain.SessionWorkflowOptions{},
			)

			var output bytes.Buffer
			if err := writeSessionGuidance(
				&output,
				session,
				guidancePrefixesForSession(session),
			); err != nil {
				t.Fatal(err)
			}

			text := output.String()

			workflow := "rename"
			if operation == domain.OperationMove {
				workflow = "move"
			}

			if !strings.Contains(text, workflow+" cleanup mig-test --finalize --delete-session") {
				t.Fatalf("guidance=%q", text)
			}

			if strings.Contains(text, "--delete-rollback-pv") ||
				strings.Contains(text, "--delete-temporary") {
				t.Fatalf("identity cleanup contains storage deletion flags: %q", text)
			}
		})
	}
}

func TestSessionListGuidancePointsToStatusCommand(t *testing.T) {
	var output bytes.Buffer
	if err := writeSessionListGuidance(
		&output,
		"migration-control",
		[]*domain.Session{guidanceSession(domain.PhaseFailed)},
		sessionCommandPrefix("migration-control"),
		"migrate-pod",
	); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if !strings.Contains(
		text,
		"pvc-migrate --session-namespace migration-control migrate-pod status SESSION",
	) {
		t.Fatalf("guidance=%q", text)
	}
}

type guidanceErrorCommand struct{ stderr bytes.Buffer }

func (c *guidanceErrorCommand) ErrOrStderr() io.Writer { return &c.stderr }

func TestCapacityFailureGuidanceDoesNotRecommendResume(t *testing.T) {
	command := &guidanceErrorCommand{}
	session := guidanceSession(domain.PhaseFailed)
	session.Status.ResumeFrom = domain.PhaseFinalSyncing

	err := domain.NewError(
		domain.ErrorConflict,
		"copy capacity",
		"destination PVC ran out of space",
	)
	if got := reportSessionError(command, session, err); !errors.Is(got, err) {
		t.Fatalf("returned error=%v", got)
	}

	text := command.stderr.String()
	for _, want := range []string{
		"Destination capacity was exhausted",
		"migrate-pod abort mig-test --dry-run",
		"--yes migrate-pod abort mig-test --dry-run=false",
		"migrate-pod cleanup mig-test --delete-temporary --delete-rollback-pv --finalize --delete-session --dry-run",
		"larger --destination-capacity",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}

	if strings.Contains(text, "migrate-pod resume") {
		t.Fatalf("capacity guidance recommends retrying an undersized destination: %q", text)
	}
}

func TestPersistedCapacityFailureStatusKeepsSpecializedGuidance(t *testing.T) {
	session := guidanceSession(domain.PhaseFailed)
	session.Status.ResumeFrom = domain.PhaseWarmCopying
	session.Status.Message = "copy capacity: destination PVC is full"

	var output bytes.Buffer
	if err := writeSessionGuidance(
		&output,
		session,
		guidancePrefixesForSession(session),
	); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if !strings.Contains(text, "Destination capacity was exhausted") ||
		!strings.Contains(text, "migrate-pod abort mig-test") ||
		strings.Contains(text, "migrate-pod resume mig-test") {
		t.Fatalf("persisted capacity guidance=%q", text)
	}
}

func TestErrorGuidanceIncludesRecoveryInspection(t *testing.T) {
	command := &guidanceErrorCommand{}
	if err := reportSessionLookupError(
		command,
		"migration-control",
		"mig-test",
		domain.NewError(domain.ErrorValidation, "get session", "missing"),
	); err == nil {
		t.Fatal("expected original lookup error")
	}

	text := command.stderr.String()
	for _, want := range []string{
		"migrate status",
		"configmap pvc-migrate-session-mig-test",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}
}

func TestSessionLookupGuidanceUsesOwningWorkflow(t *testing.T) {
	root := NewRoot(
		Options{Version: "test", In: strings.NewReader(""), Out: io.Discard, ErrOut: io.Discard},
	)

	command, _, err := root.Find([]string{"copy", "resume"})
	if err != nil {
		t.Fatal(err)
	}

	var stderr bytes.Buffer
	command.SetErr(&stderr)

	if err := reportSessionLookupError(
		command,
		"sessions",
		"copy-test",
		domain.NewError(domain.ErrorValidation, "get session", "missing"),
	); err == nil {
		t.Fatal("expected original lookup error")
	}

	text := stderr.String()
	if !strings.Contains(text, "copy status") || strings.Contains(text, "migrate status") {
		t.Fatalf("workflow-specific guidance=%q", text)
	}
}

func TestSessionCreationErrorGuidanceInspectsPotentialRecord(t *testing.T) {
	command := &guidanceErrorCommand{}

	creationErr := domain.NewError(
		domain.ErrorKubernetes,
		"create session",
		"connection reset after create",
	)
	if err := reportSessionCreationError(
		command,
		"migration-control",
		"mig-test",
		creationErr,
	); !errors.Is(
		err,
		creationErr,
	) {
		t.Fatalf("returned error=%v", err)
	}

	text := command.stderr.String()
	for _, want := range []string{"migrate status mig-test", "get configmap pvc-migrate-session-mig-test"} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}
}

func TestTransferErrorGuidanceIncludesPVCInspection(t *testing.T) {
	command := &guidanceErrorCommand{}
	if err := reportTransferError(
		command,
		"backup",
		"app",
		"data",
		domain.NewError(domain.ErrorPrecondition, "backup", "offline"),
	); err == nil {
		t.Fatal("expected original transfer error")
	}

	if !strings.Contains(command.stderr.String(), "kubectl --namespace app get pvc data") {
		t.Fatalf("guidance=%q", command.stderr.String())
	}
}

func TestTransferPathErrorGuidanceDoesNotClaimPlanMountsPVC(t *testing.T) {
	command := &guidanceErrorCommand{}

	err := domain.NewError(
		domain.ErrorPrecondition,
		"transfer path preflight",
		"selected path is a symbolic link",
	)
	if got := reportTransferError(
		command,
		"backup",
		"app",
		"data",
		err,
	); !errors.Is(
		got,
		err,
	) {
		t.Fatalf("returned error=%v", got)
	}

	text := command.stderr.String()
	if !strings.Contains(text, "Correct --path and rerun the write command") ||
		!strings.Contains(text, "mounted directory") {
		t.Fatalf("guidance=%q", text)
	}

	if strings.Contains(text, "rerun its plan") {
		t.Fatalf("path guidance incorrectly claims plan performs the mount check: %q", text)
	}
}

func TestRuntimeErrorGuidanceAvoidsPVCInspection(t *testing.T) {
	command := &guidanceErrorCommand{}
	if err := reportRuntimeError(
		command,
		domain.NewError(domain.ErrorValidation, "flags", "unsupported output format"),
	); err == nil {
		t.Fatal("expected original runtime error")
	}

	text := command.stderr.String()
	if strings.Contains(text, "get pvc") || !strings.Contains(text, "--kubeconfig") ||
		!strings.Contains(text, "--output") {
		t.Fatalf("guidance=%q", text)
	}
}

func TestCleanupErrorGuidanceUsesActualLeaseName(t *testing.T) {
	command := &guidanceErrorCommand{}

	session := guidanceSession(domain.PhaseCompleted)
	if err := reportCleanupError(
		command,
		session,
		app.CleanupOptions{DeleteSession: true},
		domain.NewError(domain.ErrorKubernetes, "delete session", "delete failed"),
	); err == nil {
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
	if err := reportCleanupError(
		command,
		session,
		app.CleanupOptions{DeleteSession: true},
		cleanupErr,
	); !errors.Is(
		err,
		cleanupErr,
	) {
		t.Fatalf("returned error=%v", err)
	}

	text := command.stderr.String()
	if !strings.Contains(text, "session record removal") ||
		!strings.Contains(text, "get lease "+kube.SessionLockName(session.ID)) {
		t.Fatalf("guidance=%q", text)
	}
}

func TestCleanupLockErrorGuidanceDescribesRetryableCleanup(t *testing.T) {
	command := &guidanceErrorCommand{}
	session := guidanceSession(domain.PhaseCompleted)
	options := app.CleanupOptions{
		DeleteTemporary: true,
		DeleteRollback:  true,
		Finalize:        true,
		DeleteSession:   true,
	}

	cleanupErr := domain.NewError(
		domain.ErrorKubernetes,
		"acquire session lock",
		"read Lease failed",
	)
	if err := reportCleanupError(
		command,
		session,
		options,
		cleanupErr,
	); !errors.Is(
		err,
		cleanupErr,
	) {
		t.Fatalf("returned error=%v", err)
	}

	text := command.stderr.String()
	for _, want := range []string{
		"Cleanup stopped before confirmed completion",
		"migrate-pod status " + session.ID,
		"migrate-pod cleanup " + session.ID + " --delete-temporary --delete-rollback-pv --finalize --delete-session --dry-run",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("guidance=%q missing %q", text, want)
		}
	}

	if strings.Contains(text, "session record removal") ||
		strings.Contains(text, "cleanup-orphan") {
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

			command, _, err := root.Find([]string{"migrate-pod", "cleanup"})
			if err != nil {
				t.Fatal(err)
			}

			blocker := &app.CleanupPodBlockerError{
				PVCNamespace:  "system",
				PVCName:       "data-migrated",
				PodNamespace:  "system",
				PodName:       "copy-tool",
				PodPhase:      "Failed",
				OwnerKind:     test.kind,
				OwnerName:     "copy-owner",
				OwnerVerified: true,
				SessionOwned:  true,
				Terminal:      true,
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
		PVCNamespace: "system",
		PVCName:      "data-migrated",
		PodNamespace: "system",
		PodName:      "copy-tool",
		OwnerKind:    "Job",
		OwnerName:    "copy-owner",
		SessionOwned: true,
		Terminal:     true,
	}
	if err := writeCleanupPodBlockerGuidance(
		&output,
		&guidanceErrorCommand{},
		blocker,
	); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if !strings.Contains(text, "UID could not be verified") ||
		strings.Contains(text, "delete job copy-owner") {
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
				session.Spec = domain.NewSessionSpec(
					domain.OperationCopy,
					session.Spec.SessionCommon,

					true,
					session.Spec.WorkflowOptions(),
				)
			}

			if err := writeWarmCopyMountGuidance(
				&output,
				&guidanceErrorCommand{},
				session,
			); err != nil {
				t.Fatal(err)
			}

			text := output.String()

			workflow := "migrate-pod"
			if test.operation == domain.OperationCopy {
				workflow = "copy"
			}

			for _, want := range []string{
				workflow + " abort mig-test --dry-run",
				workflow + " cleanup mig-test --delete-temporary --delete-rollback-pv --finalize --delete-session --dry-run",
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
		SessionSpec: domain.NewSessionSpec(
			domain.OperationCopy,
			domain.SessionCommon{},

			true,
			domain.SessionWorkflowOptions{},
		),

		Checks: []domain.Check{{Name: "warm-copy-mount", Severity: domain.SeverityError}},
	}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}

	if text := output.String(); !strings.Contains(text, "without --online") ||
		strings.Contains(text, "--precopy-passes") {
		t.Fatalf("copy plan guidance=%q", text)
	}
}

func TestRealtimeOpenEBSLVMPlanFailureGuidanceOffersWarmCopyRecovery(t *testing.T) {
	var output bytes.Buffer

	plan := &domain.MigrationPlan{
		SessionSpec: domain.NewPodMigrationSessionSpec(
			domain.SessionCommon{},
			domain.WorkloadSpec{},
			domain.SessionWorkflowOptions{},
			1,
			false,
		),
		Checks: []domain.Check{{
			Name:     "warm-copy-mount",
			Severity: domain.SeverityError,
			Message:  "OpenEBS LVMVolume does not support a second mount",
		}},
	}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	for _, want := range []string{"--precopy-passes 0", "--openebs-lvm-enable-shared"} {
		if !strings.Contains(text, want) {
			t.Fatalf("realtime OpenEBS plan guidance=%q missing %q", text, want)
		}
	}
}

func TestKubeBlocksDestinationCapacityGuidanceUsesClusterCapacity(t *testing.T) {
	var output bytes.Buffer

	plan := &domain.MigrationPlan{
		SessionSpec: domain.NewPodMigrationSessionSpec(
			domain.SessionCommon{},
			domain.WorkloadSpec{
				Adapter: domain.WorkloadKubeBlocks,
				KubeBlocks: &domain.KubeBlocksSpec{
					Cluster:   "mongo-cluster",
					Component: "mongo",
				},
			},
			domain.SessionWorkflowOptions{},
			0,
			false,
		),
		Checks: []domain.Check{{
			Name:     "destination-capacity",
			Severity: domain.SeverityError,
			Message:  "destination capacity 4Gi is below source capacity 8Gi for PVC app/data",
		}},
	}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if !strings.Contains(
		text,
		"update Cluster mongo-cluster component mongo volumeClaimTemplates storage request",
	) ||
		!strings.Contains(text, "rerun migrate-pod") ||
		strings.Contains(text, "--destination-capacity") {
		t.Fatalf("KubeBlocks capacity guidance=%q", text)
	}
}

func TestOfflineConsumerGuidanceDoesNotSuggestMigratePodFlags(t *testing.T) {
	var output bytes.Buffer

	plan := &domain.MigrationPlan{
		SessionSpec: domain.NewOfflineMigrationSessionSpec(
			domain.SessionCommon{},
			domain.SessionWorkflowOptions{},
		),
		Checks: []domain.Check{{
			Name:     "pvc-consumers",
			Severity: domain.SeverityError,
			Message:  "offline migrate found active Pod consumer",
		}},
	}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if !strings.Contains(text, "stop every consumer") || strings.Contains(text, "--pod") {
		t.Fatalf("offline consumer guidance=%q", text)
	}
}

func TestPodConsumerGuidanceRejectsMultipleWorkloadAssumption(t *testing.T) {
	var output bytes.Buffer

	plan := &domain.MigrationPlan{
		SessionSpec: domain.NewPodMigrationSessionSpec(
			domain.SessionCommon{},
			domain.WorkloadSpec{Adapter: domain.WorkloadStandalone},
			domain.SessionWorkflowOptions{},
			0,
			false,
		),
		Checks: []domain.Check{
			{
				Name:     "pvc-consumers",
				Severity: domain.SeverityError,
				Message:  "PVC app/data is shared with Pod(s): other-workload; migrate-pod coordinates one selected workload only",
			},
		},
	}
	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if !strings.Contains(text, "stop every consumer outside the selected workload") ||
		!strings.Contains(text, "cannot cut over multiple independent workloads") ||
		strings.Contains(text, "select the owning workload with --pod") {
		t.Fatalf("Pod consumer guidance=%q", text)
	}
}

func TestKubeBlocksStorageCapacityPlanGuidanceUsesClusterCapacity(t *testing.T) {
	var output bytes.Buffer

	plan := &domain.MigrationPlan{
		SessionSpec: domain.NewPodMigrationSessionSpec(
			domain.SessionCommon{},
			domain.WorkloadSpec{
				Adapter: domain.WorkloadKubeBlocks,
				KubeBlocks: &domain.KubeBlocksSpec{
					Cluster:   "redis-cluster",
					Component: "redis",
				},
			},
			domain.SessionWorkflowOptions{},
			0,
			false,
		),
		Checks: []domain.Check{{
			Name:     "storage-capacity",
			Severity: domain.SeverityError,
			Message:  "StorageClass fast has insufficient capacity on target node",
		}},
	}

	if err := writePlanFailureGuidance(&output, plan); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if !strings.Contains(
		text,
		"update Cluster redis-cluster component redis volumeClaimTemplates storage request",
	) ||
		strings.Contains(text, "correct --destination-capacity") {
		t.Fatalf("KubeBlocks storage capacity guidance=%q", text)
	}
}

func TestApprovalErrorGuidanceStatesProtectedActionDidNotStart(t *testing.T) {
	command := &guidanceErrorCommand{}

	approvalErr := domain.WrapError(
		domain.ErrorTimeout,
		"approval",
		"typed approval canceled",
		context.Canceled,
	)
	if err := reportApprovalError(command, approvalErr); !errors.Is(err, approvalErr) {
		t.Fatalf("returned error=%v", err)
	}

	text := command.stderr.String()
	if !strings.Contains(text, "before the protected action began") ||
		!strings.Contains(text, "--dry-run") ||
		!strings.Contains(text, "--yes") {
		t.Fatalf("guidance=%q", text)
	}
}
