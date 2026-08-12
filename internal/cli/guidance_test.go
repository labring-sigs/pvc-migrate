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
	if err := reportCleanupError(command, session, app.CleanupOptions{DeleteSession: true}, domain.NewError(domain.ErrorKubernetes, "cleanup", "delete failed")); err == nil {
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
