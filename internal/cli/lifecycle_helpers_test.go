package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	"github.com/spf13/cobra"
)

type scriptedControllerWaiter struct {
	updates []*domain.Session
	err     error
	called  bool
}

func (w *scriptedControllerWaiter) Wait(
	_ context.Context,
	_ *domain.Session,
	onUpdate func(*domain.Session) (bool, error),
) (*domain.Session, error) {
	w.called = true
	for _, update := range w.updates {
		done, err := onUpdate(update)
		if err != nil {
			return update, err
		}

		if done {
			return update, w.err
		}
	}

	return nil, w.err
}

func TestControllerResourceNamesCoverEveryWorkflow(t *testing.T) {
	for _, workflow := range domain.ControllerWorkflows() {
		common := domain.SessionCommon{
			SourceNamespace: "app", TemporaryNamespace: "app",
			DestinationNamespace: "app", SessionNamespace: "app",
		}

		wantKind, wantResource, wantSingular := workflow.Kind, workflow.Resource, workflow.Singular
		if workflow.Kind == "" {
			common.TemporaryNamespace = "archive"
			common.DestinationNamespace = "archive"
			common.SessionNamespace = "system"
			wantKind = workflow.ClusterKind
			wantResource = workflow.ClusterResource
			wantSingular = workflow.ClusterSingular
		}

		var spec domain.SessionSpec
		switch workflow.Type {
		case domain.SessionTypeMigrate:
			spec = domain.NewOfflineMigrationSessionSpec(common, domain.SessionWorkflowOptions{})
		case domain.SessionTypeMigratePod:
			spec = domain.NewPodMigrationSessionSpec(
				common,
				domain.WorkloadSpec{Adapter: domain.WorkloadNone},
				domain.SessionWorkflowOptions{},
				1,
				false,
			)
		default:
			spec = domain.NewSessionSpec(
				operationForSessionType(
					workflow.Type,
				),
				common,
				false,
				domain.SessionWorkflowOptions{},
			)
		}

		session := domain.NewSession("workflow-test", spec, time.Now())

		if got := controllerResourceKind(session); got != wantKind {
			t.Fatalf("type %q kind=%q, want %q", workflow.Type, got, wantKind)
		}

		if got := controllerResourceName(session); got != wantSingular {
			t.Fatalf("type %q resource=%q, want %q", workflow.Type, got, wantSingular)
		}

		if got := controllerResourceForKubectl(
			session,
		); got != wantResource+"."+domain.SessionAPIGroup {
			t.Fatalf(
				"type %q kubectl resource=%q, want %q",
				workflow.Type,
				got,
				wantResource+"."+domain.SessionAPIGroup,
			)
		}
	}
}

func TestDeferControllerExecutionSkipsTerminalSessions(t *testing.T) {
	var stdout, stderr bytes.Buffer

	command := &cobra.Command{}
	command.SetOut(&stdout)
	command.SetErr(&stderr)

	runtime := &commandRuntime{
		mode:    "controller",
		printer: output.Printer{Writer: &stdout, Format: output.JSON},
	}

	spec := domain.NewOfflineMigrationSessionSpec(
		domain.SessionCommon{
			SourceNamespace:      "app",
			TemporaryNamespace:   "app",
			DestinationNamespace: "app",
			SessionNamespace:     "app",
		},
		domain.SessionWorkflowOptions{},
	)

	session := domain.NewSession("workflow-test", spec, time.Now())
	session.Backend = kube.SessionBackendCRD

	deferred, err := deferControllerExecution(context.Background(), command, runtime, session)
	if err != nil || !deferred {
		t.Fatalf("deferred=%t error=%v, want controller acknowledgement", deferred, err)
	}

	if !strings.Contains(stderr.String(), "migration workflow-test was submitted") {
		t.Fatalf("controller acknowledgement=%q", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()

	session.Status.Phase = domain.PhaseCompleted

	deferred, err = deferControllerExecution(
		context.Background(), command, runtime, session,
	)
	if err != nil || deferred {
		t.Fatalf("terminal deferred=%t error=%v, want synchronous terminal handling", deferred, err)
	}

	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf(
			"terminal acknowledgement output stdout=%q stderr=%q",
			stdout.String(),
			stderr.String(),
		)
	}

	session.Status.Phase = domain.PhaseFailed

	deferred, err = deferControllerExecution(context.Background(), command, runtime, session)
	if err != nil || deferred {
		t.Fatalf("failed deferred=%t error=%v, want explicit CLI recovery", deferred, err)
	}
}

func TestDeferControllerExecutionWaitsAndPrintsOnlyFinalSession(t *testing.T) {
	var stdout, stderr bytes.Buffer

	command := &cobra.Command{}
	command.SetOut(&stdout)
	command.SetErr(&stderr)

	initial := controllerTestSession(domain.PhasePlanned, "session planned")
	warmCopying := controllerTestSession(domain.PhaseWarmCopying, "copy started")
	completed := controllerTestSession(domain.PhaseCompleted, "migration completed")
	waiter := &scriptedControllerWaiter{
		updates: []*domain.Session{initial, warmCopying, completed},
	}
	runtime := &commandRuntime{
		mode:              "controller",
		printer:           output.Printer{Writer: &stdout, Format: output.JSON},
		waitForController: true,
		controllerWaiter:  waiter,
	}

	deferred, err := deferControllerExecution(
		context.Background(), command, runtime, initial,
	)
	if err != nil || !deferred || !waiter.called {
		t.Fatalf("deferred=%t called=%t error=%v", deferred, waiter.called, err)
	}

	var printed domain.Session
	if err := json.Unmarshal(stdout.Bytes(), &printed); err != nil {
		t.Fatalf("stdout is not one session JSON document: %v\n%s", err, stdout.String())
	}

	if printed.Status.Phase != domain.PhaseCompleted {
		t.Fatalf("printed phase=%s, want Completed", printed.Status.Phase)
	}

	progress := stderr.String()
	for _, want := range []string{
		"migration workflow-test was submitted",
		"migration workflow-test: WarmCopying - copy started",
		"migration workflow-test: Completed - migration completed",
	} {
		if !strings.Contains(progress, want) {
			t.Fatalf("stderr=%q missing %q", progress, want)
		}
	}
}

func TestDeferControllerExecutionPrintsFailedSessionAndReturnsError(t *testing.T) {
	var stdout, stderr bytes.Buffer

	command := &cobra.Command{}
	command.SetOut(&stdout)
	command.SetErr(&stderr)

	initial := controllerTestSession(domain.PhasePlanned, "session planned")
	failed := controllerTestSession(domain.PhaseFailed, "copy pod failed")
	waiter := &scriptedControllerWaiter{updates: []*domain.Session{failed}}
	runtime := &commandRuntime{
		mode:              "controller",
		printer:           output.Printer{Writer: &stdout, Format: output.JSON},
		waitForController: true,
		controllerWaiter:  waiter,
	}

	deferred, err := deferControllerExecution(
		context.Background(), command, runtime, initial,
	)
	if !deferred || err == nil || domain.CategoryOf(err) != domain.ErrorInternal {
		t.Fatalf("deferred=%t category=%s error=%v", deferred, domain.CategoryOf(err), err)
	}

	if !strings.Contains(stdout.String(), `"phase": "Failed"`) ||
		!strings.Contains(stderr.String(), "Failed - copy pod failed") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestDeferControllerExecutionDetachedDoesNotStartWaiter(t *testing.T) {
	var stdout, stderr bytes.Buffer

	command := &cobra.Command{}
	command.SetOut(&stdout)
	command.SetErr(&stderr)

	initial := controllerTestSession(domain.PhasePlanned, "session planned")
	waiter := &scriptedControllerWaiter{err: errors.New("must not be called")}
	runtime := &commandRuntime{
		mode:              "controller",
		printer:           output.Printer{Writer: &stdout, Format: output.JSON},
		controllerWaiter:  waiter,
		waitForController: false,
	}

	deferred, err := deferControllerExecution(
		context.Background(), command, runtime, initial,
	)
	if err != nil || !deferred || waiter.called {
		t.Fatalf("deferred=%t called=%t error=%v", deferred, waiter.called, err)
	}

	if !strings.Contains(stdout.String(), `"phase": "Planned"`) {
		t.Fatalf("detached stdout=%q", stdout.String())
	}
}

func controllerTestSession(phase domain.Phase, message string) *domain.Session {
	spec := domain.NewOfflineMigrationSessionSpec(
		domain.SessionCommon{
			SourceNamespace:      "app",
			TemporaryNamespace:   "app",
			DestinationNamespace: "app",
			SessionNamespace:     "app",
		},
		domain.SessionWorkflowOptions{},
	)
	session := domain.NewSession("workflow-test", spec, time.Now())
	session.Backend = kube.SessionBackendCRD
	session.BackendResource = domain.ControllerKindMigration
	session.Status.Phase = phase
	session.Status.Message = message

	return session
}

func TestControllerExecutionFinishedUsesOperationSpecificCompletionPhase(t *testing.T) {
	tests := []struct {
		name     string
		typeName domain.SessionType
		phase    domain.Phase
		want     bool
	}{
		{
			name: "migration completed", typeName: domain.SessionTypeMigrate,
			phase: domain.PhaseCompleted, want: true,
		},
		{
			name: "reservation reserved", typeName: domain.SessionTypeReserve,
			phase: domain.PhaseReserved, want: true,
		},
		{
			name: "copy warm copied", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseWarmCopied, want: true,
		},
		{
			name: "migration reserved", typeName: domain.SessionTypeMigrate,
			phase: domain.PhaseReserved, want: false,
		},
		{
			name: "pod migration warm copied", typeName: domain.SessionTypeMigratePod,
			phase: domain.PhaseWarmCopied, want: false,
		},
		{
			name: "copy warm copying", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseWarmCopying, want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &domain.Session{
				Spec:   domain.SessionSpec{Type: test.typeName},
				Status: domain.SessionStatus{Phase: test.phase},
			}
			if got := controllerExecutionFinished(session); got != test.want {
				t.Fatalf("controllerExecutionFinished()=%t, want %t", got, test.want)
			}
		})
	}
}

func TestControllerWorkflowAvailabilityHonorsPartialDiscovery(t *testing.T) {
	runtime := &commandRuntime{
		mode:            "controller",
		controllerStore: kube.NewCRDSessionStore(nil),
		controllerKinds: []domain.ControllerKind{
			domain.ControllerKindBackup,
			domain.ControllerKindMigration,
		},
		controllerModeExplicit: false,
	}

	if !controllerWorkflowAvailable(runtime, domain.SessionTypeBackup) {
		t.Fatal("backup workflow should use its installed CRD")
	}

	if controllerWorkflowAvailable(runtime, domain.SessionTypeRestore) {
		t.Fatal("restore workflow should fall back when its CRD is absent")
	}

	if err := requireControllerWorkflow(runtime, domain.SessionTypeRestore); err != nil {
		t.Fatalf("auto mode should allow session fallback: %v", err)
	}

	runtime.controllerModeExplicit = true
	if err := requireControllerWorkflow(runtime, domain.SessionTypeRestore); err == nil {
		t.Fatal("explicit controller mode should reject an absent Restore CRD")
	}
}

func TestControllerPlanNamespacesRespectTenantBoundary(t *testing.T) {
	r := &rootState{global: globals{sessionNamespace: "pvc-migrate-system"}}
	runtime := &commandRuntime{
		mode:            executionModeController,
		controllerStore: kube.NewCRDSessionStore(nil),
		controllerKinds: []domain.ControllerKind{domain.ControllerKindMigration},
	}

	sessionNamespace, temporaryNamespace := r.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigrate,
		"application",
		"application",
		"pvc-migrate-system",
		false,
	)
	if sessionNamespace != "application" || temporaryNamespace != "application" {
		t.Fatalf(
			"controller defaults = %q/%q, want application/application",
			sessionNamespace,
			temporaryNamespace,
		)
	}

	sessionNamespace, temporaryNamespace = r.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigrate,
		"application",
		"application",
		"pvc-migrate-system",
		true,
	)
	if sessionNamespace != "pvc-migrate-system" || temporaryNamespace != "pvc-migrate-system" {
		t.Fatalf(
			"explicit temporary namespace = %q/%q, want global/system",
			sessionNamespace,
			temporaryNamespace,
		)
	}

	sessionNamespace, temporaryNamespace = r.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigrate,
		"application",
		"archive",
		"pvc-migrate-system",
		false,
	)
	if sessionNamespace != "pvc-migrate-system" || temporaryNamespace != "pvc-migrate-system" {
		t.Fatalf(
			"cross namespace = %q/%q, want global/system",
			sessionNamespace,
			temporaryNamespace,
		)
	}

	runtime.controllerKinds = []domain.ControllerKind{domain.ControllerKindBackup}

	sessionNamespace, temporaryNamespace = r.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigrate,
		"application",
		"application",
		"pvc-migrate-system",
		false,
	)
	if sessionNamespace != "pvc-migrate-system" || temporaryNamespace != "pvc-migrate-system" {
		t.Fatalf("missing CRD = %q/%q, want global/system", sessionNamespace, temporaryNamespace)
	}
}

func TestWorkflowNamespaceForCommandHonorsExplicitTenantFlag(t *testing.T) {
	r := &rootState{
		global: globals{sessionNamespace: "pvc-migrate-system", workflowNamespace: "global-tenant"},
	}
	command := &cobra.Command{}
	command.Flags().String("namespace", "", "")

	if err := command.Flags().Set("namespace", "application"); err != nil {
		t.Fatal(err)
	}

	if got := workflowNamespaceForCommand(r, command); got != "application" {
		t.Fatalf("explicit namespace=%q, want application", got)
	}

	command = &cobra.Command{}
	if got := workflowNamespaceForCommand(r, command); got != "global-tenant" {
		t.Fatalf("global workflow namespace=%q, want global-tenant", got)
	}
}

func TestAdoptReservedSessionBuildsCopyOwnedOptions(t *testing.T) {
	spec := domain.NewSessionSpec(
		domain.OperationReserve,
		domain.SessionCommon{SessionNamespace: "system"},
		false,
		domain.SessionWorkflowOptions{
			TargetNode:           "target-a",
			ToolImage:            "example/tool:v1",
			SkipSourceUsageCheck: true,
		},
	)
	session := domain.NewSession("reserved", spec, time.Now())
	flags := &copyFlags{
		sourceNode:       "source-a",
		strategies:       []string{domain.StrategyMount},
		online:           true,
		verifyChecksum:   true,
		deleteExtraneous: true,
	}

	if err := adoptReservedSessionForCopy(session, flags); err != nil {
		t.Fatal(err)
	}

	options := session.Spec.WorkflowOptions()
	if session.Spec.Type != domain.SessionTypeCopy || !session.Spec.Online() ||
		options.SourceNode != "source-a" || options.TargetNode != "target-a" ||
		options.ToolImage != "example/tool:v1" || !options.SkipSourceUsageCheck ||
		!options.VerifyChecksum || !options.DeleteExtraneous ||
		len(options.Strategies) != 1 || options.Strategies[0] != domain.StrategyMount {
		t.Fatalf("adopted copy spec=%#v", session.Spec)
	}
}

func operationForSessionType(sessionType domain.SessionType) domain.Operation {
	switch sessionType {
	case domain.SessionTypeMigrate:
		return domain.OperationMigrate
	case domain.SessionTypeMigratePod:
		return domain.OperationMigratePod
	case domain.SessionTypeReserve:
		return domain.OperationReserve
	case domain.SessionTypeCopy:
		return domain.OperationCopy
	case domain.SessionTypeBackup:
		return domain.OperationBackup
	case domain.SessionTypeRestore:
		return domain.OperationRestore
	case domain.SessionTypeRename:
		return domain.OperationRename
	case domain.SessionTypeMove:
		return domain.OperationMove
	default:
		return ""
	}
}
