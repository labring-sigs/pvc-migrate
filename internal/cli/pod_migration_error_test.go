package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

// TestReportPodMigrationErrorNamesRecoveryCommands pins the execution-failure
// epilogue: a Run failure happens after the session exists, so the operator
// must be pointed at status/resume/abort/cleanup — not at the pre-session
// planning epilogue.
func TestReportPodMigrationErrorNamesRecoveryCommands(t *testing.T) {
	var stderr bytes.Buffer

	command := &cobra.Command{}
	command.SetErr(&stderr)

	cause := errors.New("final sync failed")

	err := reportPodMigrationError(command, "session-1", domain.PhasePausing, cause)
	if !errors.Is(err, cause) {
		t.Fatalf("reported error=%v, want the cause joined", err)
	}

	output := stderr.String()
	for _, phrase := range []string{
		"Pod migration session-1 stopped in phase Pausing",
		"migrate-pod status session-1",
		"resume, abort or cleanup",
	} {
		if !strings.Contains(output, phrase) {
			t.Fatalf("epilogue=%q missing %q", output, phrase)
		}
	}

	if strings.Contains(output, "before session creation") {
		t.Fatalf("epilogue=%q wrongly claims the session was never created", output)
	}
}
