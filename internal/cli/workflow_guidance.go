package cli

import (
	"fmt"
	"io"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// workflowTerminalPhase reports whether a workflow phase closes the lifecycle:
// from here the only remaining actions are inspection and finalizing cleanup.
func workflowTerminalPhase(phase domain.Phase) bool {
	switch phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return true
	default:
		return false
	}
}

// writeWorkflowNextSteps prints the follow-up block after a workflow command
// finished in (or a status command revealed) a terminal phase. Lines carry the
// labels the stderr colorizer highlights, and every command is the execute
// form — dry-run is the default, so the preview form is the same command
// without --yes and --dry-run=false. Active and failed phases stay silent:
// run and error paths already print their own recovery guidance.
func writeWorkflowNextSteps(
	w io.Writer,
	prefix, workflow, session string,
	phase domain.Phase,
	rollback bool,
) error {
	if session == "" || !workflowTerminalPhase(phase) {
		return nil
	}

	if _, err := fmt.Fprintf(
		w,
		"\nNext steps for session %s (phase %s):\n",
		session,
		phase,
	); err != nil {
		return err
	}

	lines := []string{
		"  Inspect: " + workflowStatusCommand(prefix, workflow, session),
	}

	if phase == domain.PhaseCompleted && rollback {
		lines = append(
			lines,
			"  Roll back: "+lifecycleExecuteCommand(prefix, workflow, "rollback", session),
		)
	}

	lines = append(
		lines,
		"  Finalize and delete retained resources/session: "+
			cleanupExecuteCommand(prefix, workflow, session, "", true, true),
	)

	for _, line := range lines {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}

	return nil
}

// writeDryRunNotice explains that a lifecycle command only validated the
// workflow and prints the execute form of the same invocation, so a preview
// can never be mistaken for an applied change.
func writeDryRunNotice(w io.Writer, execute string) error {
	_, err := fmt.Fprintf(
		w,
		"Dry run: validated only, nothing was changed. Execute with: %s\n",
		execute,
	)

	return err
}

func workflowStatusCommand(prefix, workflow, session string) string {
	return prefix + " " + workflow + " status " + shellQuote(session)
}

func lifecycleExecuteCommand(prefix, workflow, subcommand, session string) string {
	return fmt.Sprintf(
		"%s --yes %s %s %s --dry-run=false",
		prefix,
		workflow,
		subcommand,
		shellQuote(session),
	)
}

// cleanupExecuteCommand renders the execute form of a cleanup invocation,
// mirroring the flags the operator passed so the suggested command preserves
// their reclaim decisions.
func cleanupExecuteCommand(
	prefix, workflow, session string,
	policy string,
	finalize, deleteSession bool,
) string {
	args := shellQuote(session)
	if policy != "" {
		args += " --unused-storage-policy " + shellQuote(policy)
	}

	if finalize {
		args += " --finalize"
	}

	if deleteSession {
		args += " --delete-session"
	}

	return fmt.Sprintf("%s --yes %s cleanup %s --dry-run=false", prefix, workflow, args)
}
