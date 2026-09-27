package controller

import (
	"log/slog"
	"testing"
)

// Package-level reconcile helpers log through the controller-owned logger,
// never the process-global slog default that a different CLI command may have
// reconfigured.
func TestReconcileLoggerPrefersInstalledControllerLogger(t *testing.T) {
	t.Parallel()

	installed := slog.New(slog.DiscardHandler)
	previous := currentReconcileLogger()

	setReconcileLogger(installed)
	t.Cleanup(func() { setReconcileLogger(previous) })

	if currentReconcileLogger() != installed {
		t.Fatal("reconcile helpers did not observe the installed controller logger")
	}
}
