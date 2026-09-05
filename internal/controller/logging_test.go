package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestControllerLoggerNormalizesDependencyMessages(t *testing.T) {
	var output bytes.Buffer

	logger := NewControllerLogger(slog.New(slog.NewTextHandler(&output, nil)))

	logger.Info("🚁 Attempt using strategy", "strategy", "mnt-rsync")
	logger.Info("workflow entered failed state", "workflow", "tenant/copy", "phase", "Failed")

	text := output.String()
	if strings.Contains(text, "🚁") {
		t.Fatalf("controller log retained terminal emoji: %s", text)
	}

	for _, want := range []string{
		"msg=\"Attempt using strategy\"",
		"strategy=mnt-rsync",
		"msg=\"workflow entered failed state\"",
		"workflow=tenant/copy",
		"phase=Failed",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("controller log lacks %q: %s", want, text)
		}
	}
}

func TestControllerLoggerWithAttrsAndGroupsRemainStructured(t *testing.T) {
	var output bytes.Buffer

	logger := NewControllerLogger(slog.New(slog.NewJSONHandler(&output, nil))).
		With("component", "workflow-controller").
		WithGroup("reconcile")

	if err := logger.Handler().Handle(
		context.Background(),
		slog.NewRecord(time.Time{}, slog.LevelInfo, "✅ Operation succeeded", 0),
	); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	for _, want := range []string{
		`"msg":"Operation succeeded"`,
		`"component":"workflow-controller"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("structured controller log lacks %q: %s", want, text)
		}
	}
}

func TestControllerLoggerSuppressesExpectedWatchCancellation(t *testing.T) {
	var output bytes.Buffer
	logger := NewControllerLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	logger.Error("Failed to watch", "err", context.Canceled, "resource", "pods")

	if output.Len() != 0 {
		t.Fatalf("expected canceled watch log to be suppressed, got %q", output.String())
	}

	logger.Error("Failed to watch", "err", errors.New("forbidden"), "resource", "pods")
	if !strings.Contains(output.String(), `"msg":"Failed to watch"`) {
		t.Fatalf("unexpected watch error was suppressed: %q", output.String())
	}
}
