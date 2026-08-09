package copyengine

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

func TestAllUpstreamStrategiesMapExactly(t *testing.T) {
	wants := map[string]pvmigrate.Strategy{
		"mount":        pvmigrate.Mount,
		"clusterip":    pvmigrate.ClusterIP,
		"loadbalancer": pvmigrate.LoadBalancer,
		"nodeport":     pvmigrate.NodePort,
		"local":        pvmigrate.Local,
	}
	for input, want := range wants {
		got, err := strategyValue(input)
		if err != nil {
			t.Fatalf("strategyValue(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("strategyValue(%q) = %q, want %q", input, got, want)
		}
	}
	for _, input := range []string{"", "MOUNT", "cluster-ip", "exec", " mount"} {
		_, err := strategyValue(input)
		if domain.CategoryOf(err) != domain.ErrorValidation || !strings.Contains(err.Error(), input) {
			t.Fatalf("strategyValue(%q) error=%v category=%q", input, err, domain.CategoryOf(err))
		}
	}
	for _, strategy := range pvmigrate.AllStrategies {
		got, err := strategyValue(string(strategy))
		if err != nil || got != strategy {
			t.Fatalf("upstream strategy %q is not supported by the adapter: value=%q error=%v", strategy, got, err)
		}
	}
}

func TestCopyBuildsCompleteUpstreamMigrationContract(t *testing.T) {
	var captured pvmigrate.Migration
	engine := &PVMigrate{run: func(_ context.Context, migration pvmigrate.Migration) error {
		captured = migration
		return nil
	}}
	writer := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	request := Request{
		SessionID:             "session-1",
		ToolImage:             "registry.example:5000/team/pvc-migrate:tool-image",
		Source:                domain.ObjectReference{Namespace: "source-ns", Name: "source-pvc"},
		Destination:           domain.ObjectReference{Namespace: "target-ns", Name: "target-pvc"},
		Mode:                  ModeFinal,
		Attempt:               4,
		KubeconfigPath:        "/tmp/kubeconfig",
		Context:               "cluster-context",
		Strategies:            []string{"mount", "clusterip", "loadbalancer", "nodeport", "local"},
		DeleteExtraneousFiles: true,
		VerifyChecksum:        true,
		NoCompress:            true,
		HelmTimeout:           37 * time.Second,
		HelmStringValues:      []string{"sshd.nodeName=source-node"},
		Writer:                writer,
		Logger:                logger,
	}
	var updates []Progress
	if err := engine.Copy(context.Background(), request, func(progress Progress) { updates = append(updates, progress) }); err != nil {
		t.Fatal(err)
	}

	if captured.ID != operationID(request) || captured.Source != (pvmigrate.PVC{KubeconfigPath: "/tmp/kubeconfig", Context: "cluster-context", Namespace: "source-ns", Name: "source-pvc"}) || captured.Dest != (pvmigrate.PVC{KubeconfigPath: "/tmp/kubeconfig", Context: "cluster-context", Namespace: "target-ns", Name: "target-pvc"}) {
		t.Fatalf("upstream PVC request=%#v", captured)
	}
	if !captured.DeleteExtraneousFiles || captured.IgnoreMounted || !captured.NoCompress || !captured.StructuredLogs || captured.Writer != writer || captured.Logger != logger {
		t.Fatalf("upstream migration policy=%#v", captured)
	}
	if captured.RsyncExtraArgs != "-HAXS --numeric-ids --checksum" {
		t.Fatalf("upstream rsync args=%q", captured.RsyncExtraArgs)
	}
	if captured.HelmTimeout != request.HelmTimeout || len(captured.Strategies) != len(request.Strategies) || captured.Strategies[0] != pvmigrate.Mount || captured.Strategies[4] != pvmigrate.Local {
		t.Fatalf("upstream execution fields timeout=%s strategies=%v", captured.HelmTimeout, captured.Strategies)
	}
	if len(captured.HelmValues) != len(kube.ToolSecurityContextHelmValues()) || len(captured.HelmStringValues) != 7 || captured.HelmStringValues[len(captured.HelmStringValues)-1] != "sshd.nodeName=source-node" {
		t.Fatalf("upstream Helm values=%v stringValues=%v", captured.HelmValues, captured.HelmStringValues)
	}
	if len(updates) != 2 || updates[0].State != "running" || updates[1].State != "completed" || updates[0].Mode != ModeFinal || updates[1].Attempt != request.Attempt {
		t.Fatalf("progress=%#v", updates)
	}
}

func TestCopyBuildsWarmUpstreamMigrationContract(t *testing.T) {
	var captured pvmigrate.Migration
	engine := &PVMigrate{run: func(_ context.Context, migration pvmigrate.Migration) error {
		captured = migration
		return nil
	}}
	request := Request{
		SessionID:      "session-warm",
		ToolImage:      "registry.example/pvc-migrate:contract",
		Source:         domain.ObjectReference{Namespace: "source", Name: "data"},
		Destination:    domain.ObjectReference{Namespace: "target", Name: "data"},
		Mode:           ModeWarm,
		Attempt:        1,
		Strategies:     []string{"mount"},
		HelmTimeout:    0,
		VerifyChecksum: false,
	}
	if err := engine.Copy(context.Background(), request, nil); err != nil {
		t.Fatal(err)
	}
	if !captured.IgnoreMounted || captured.RsyncExtraArgs != "-HAXS --numeric-ids" || captured.HelmTimeout != 10*time.Minute {
		t.Fatalf("warm upstream migration: ignoreMounted=%t rsyncArgs=%q helmTimeout=%s", captured.IgnoreMounted, captured.RsyncExtraArgs, captured.HelmTimeout)
	}
}

func TestOperationIDIncludesEveryCollisionBoundary(t *testing.T) {
	base := Request{
		SessionID: "migration-123",
		Source:    domain.ObjectReference{Namespace: "app", Name: "data"},
		Mode:      ModeWarm,
		Attempt:   1,
	}
	baseID := operationID(base)
	cases := []Request{
		func() Request { value := base; value.SessionID = "migration-124"; return value }(),
		func() Request { value := base; value.Source.Namespace = "other"; return value }(),
		func() Request { value := base; value.Source.Name = "logs"; return value }(),
		func() Request { value := base; value.Mode = ModeFinal; return value }(),
		func() Request { value := base; value.Attempt = 2; return value }(),
	}
	seen := map[string]struct{}{baseID: {}}
	for _, request := range cases {
		id := operationID(request)
		if id == baseID {
			t.Fatalf("request %#v collided with base ID %q", request, id)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate operation ID %q", id)
		}
		seen[id] = struct{}{}
		if len(id) > pvmigrate.MaxIDLength || !strings.HasPrefix(id, "pm-") {
			t.Fatalf("invalid operation ID %q", id)
		}
	}
}

func TestCopyRejectsStrategyBeforeReportingProgress(t *testing.T) {
	var progress []Progress
	err := NewPVMigrate().Copy(context.Background(), Request{
		Strategies: []string{"mount", "unsupported"},
	}, func(update Progress) {
		progress = append(progress, update)
	})
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("Copy() error=%v category=%q", err, domain.CategoryOf(err))
	}
	if len(progress) != 0 {
		t.Fatalf("progress emitted before validation completed: %#v", progress)
	}
}

func TestCopyReportsUpstreamFailureWithStableOperationIdentity(t *testing.T) {
	cases := []struct {
		name     string
		mode     Mode
		verify   bool
		timeout  time.Duration
		strategy string
	}{
		{name: "warm defaults", mode: ModeWarm, strategy: "mount"},
		{name: "final checksum and explicit timeout", mode: ModeFinal, verify: true, timeout: time.Second, strategy: "clusterip"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var updates []Progress
			var logs bytes.Buffer
			request := Request{
				SessionID:      "mig-test",
				Source:         domain.ObjectReference{Namespace: "app", Name: "source"},
				Destination:    domain.ObjectReference{Namespace: "staging", Name: "destination"},
				Mode:           tc.mode,
				Attempt:        2,
				KubeconfigPath: t.TempDir() + "/missing-kubeconfig",
				Strategies:     []string{tc.strategy},
				VerifyChecksum: tc.verify,
				HelmTimeout:    tc.timeout,
				Writer:         io.Discard,
				Logger:         slog.New(slog.NewTextHandler(&logs, nil)),
			}
			err := NewPVMigrate().Copy(context.Background(), request, func(update Progress) {
				updates = append(updates, update)
			})
			if domain.CategoryOf(err) != domain.ErrorCopy {
				t.Fatalf("Copy() error=%v category=%q logs=%s", err, domain.CategoryOf(err), logs.String())
			}
			operation := operationID(request)
			if !strings.Contains(err.Error(), operation) {
				t.Fatalf("error %q omits operation ID %q", err, operation)
			}
			if len(updates) != 2 || updates[0].State != "running" || updates[0].Message != operation || updates[1].State != "failed" || updates[1].Message == "" {
				t.Fatalf("progress updates = %#v", updates)
			}
			for _, update := range updates {
				if update.Mode != tc.mode || update.Attempt != 2 {
					t.Fatalf("progress lost request identity: %#v", update)
				}
			}
		})
	}
}

func TestProgressSerializesStableFields(t *testing.T) {
	progress := Progress{Mode: ModeFinal, Attempt: 3, State: "completed", Message: "pm-id", Bytes: 42}
	if progress.Mode != ModeFinal || progress.Attempt != 3 || progress.State != "completed" || progress.Message != "pm-id" || progress.Bytes != 42 {
		t.Fatalf("progress = %#v", progress)
	}
}

func TestClassifyRunErrorPreservesTimeoutAndCancellation(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		err      error
		category domain.ErrorCategory
	}{
		{name: "deadline error", ctx: context.Background(), err: context.DeadlineExceeded, category: domain.ErrorTimeout},
		{name: "cancel error", ctx: context.Background(), err: context.Canceled, category: domain.ErrorTimeout},
		{name: "copy error", ctx: context.Background(), err: io.EOF, category: domain.ErrorCopy},
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	tests = append(tests, struct {
		name     string
		ctx      context.Context
		err      error
		category domain.ErrorCategory
	}{name: "deadline from context", ctx: deadlineCtx, err: io.EOF, category: domain.ErrorTimeout})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := classifyRunError(test.ctx, "pm-test", test.err)
			if domain.CategoryOf(err) != test.category {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
			if !strings.Contains(err.Error(), "pm-test") {
				t.Fatalf("operation identity missing from %q", err)
			}
		})
	}
}
