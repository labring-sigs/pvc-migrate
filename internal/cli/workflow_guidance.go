package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
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

// writeControllerWorkflowNextSteps prints follow-ups for a workflow the
// elected controller owns: inspection and finalization stay in kubectl,
// because deleting the CR converges storage through the controller's
// finalizer rather than a CLI lifecycle command.
func writeControllerWorkflowNextSteps(
	w io.Writer,
	kubectlPrefix, resource, namespace, name string,
	phase domain.Phase,
) error {
	if name == "" || !workflowTerminalPhase(phase) {
		return nil
	}

	scope := ""
	if namespace != "" {
		scope = "-n " + namespace + " "
	}

	if _, err := fmt.Fprintf(
		w,
		"\nNext steps for workflow %s/%s (phase %s):\n",
		resource,
		name,
		phase,
	); err != nil {
		return err
	}

	lines := []string{
		"  Inspect: " + kubectlPrefix + " " + scope + "get " + resource + " " + shellQuote(name),
		"  Finalize: " + kubectlPrefix + " " + scope + "delete " + resource + " " +
			shellQuote(name) +
			" (the finalizer converges storage per the spec reclaim policies)",
	}

	for _, line := range lines {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}

	return nil
}

// writeDryRunApprovalNotice explains a submission preview whose execute form
// is the same invocation plus the approval flags; the operator's shell
// history already carries the remaining inputs.
func writeDryRunApprovalNotice(w io.Writer) error {
	_, err := fmt.Fprintln(
		w,
		"Dry run: nothing was submitted or changed. "+
			"Execute with the same command plus --yes and --dry-run=false.",
	)

	return err
}

// crossClusterExecuteCommand renders the execute form of a cross-cluster
// lifecycle invocation, carrying over the connection flags the operator
// changed so the suggestion reaches the same two clusters. path is the
// command path below the root, e.g. "copy cross-cluster resume".
func crossClusterExecuteCommand(cmd *cobra.Command, path, session string) string {
	args := []string{"pvc-migrate", "--yes"}

	args = append(args, strings.Fields(path)...)
	args = append(args, shellQuote(session))

	for _, name := range []string{
		"source-kubeconfig", "source-context",
		"destination-kubeconfig", "destination-context", "session-namespace",
		"unused-storage-policy", "delete-session",
	} {
		if flag := cmd.Flags().Lookup(name); flag != nil && flag.Changed {
			args = append(args, "--"+name+"="+shellQuote(flag.Value.String()))
		}
	}

	args = append(args, "--dry-run=false")

	return strings.Join(args, " ")
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
