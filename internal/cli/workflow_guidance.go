package cli

import (
	"fmt"
	"io"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
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
//
// The block is mode-aware through cmd: session commands print session-flavored
// follow-ups; cr commands print cr-flavored follow-ups addressed with -n.
func writeWorkflowNextSteps(
	w io.Writer,
	cmd *cobra.Command,
	prefix, family, namespace, name string,
	phase domain.Phase,
	rollback bool,
) error {
	if name == "" || !workflowTerminalPhase(phase) {
		return nil
	}

	label, scope := "session", name

	finalizeLabel := "Finalize and delete retained resources/session"
	if isControllerCommand(cmd) {
		label, scope = "workflow", workflowScopeName(namespace, name)
		finalizeLabel = "Finalize and delete retained resources/workflow"
	}

	if _, err := fmt.Fprintf(
		w,
		"\nNext steps for %s %s (phase %s):\n",
		label,
		scope,
		phase,
	); err != nil {
		return err
	}

	address := workflowHintAddress(cmd, namespace, name)

	lines := []string{
		"  Inspect: " + prefix + " " + workflowCommandPath(cmd, family) + " status " + address,
	}

	if phase == domain.PhaseCompleted && rollback {
		lines = append(
			lines,
			"  Roll back: "+lifecycleExecuteCommand(
				cmd,
				prefix,
				family,
				"rollback",
				namespace,
				name,
			),
		)
	}

	lines = append(
		lines,
		"  "+finalizeLabel+": "+
			cleanupExecuteCommand(cmd, prefix, family, namespace, name, "", true, true),
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
// elected controller owns, in cr command form: cr status inspects, cr cleanup
// finalizes, and a kubectl delete remains the declarative alternative (the
// finalizer converges storage per the spec reclaim policies).
func writeControllerWorkflowNextSteps(
	w io.Writer,
	cmd *cobra.Command,
	prefix, resource, namespace, name string,
	phase domain.Phase,
) error {
	if name == "" || !workflowTerminalPhase(phase) {
		return nil
	}

	if _, err := fmt.Fprintf(
		w,
		"\nNext steps for workflow %s (phase %s):\n",
		workflowScopeName(namespace, name),
		phase,
	); err != nil {
		return err
	}

	family := crFamilyPathForResource(resource)
	address := workflowHintAddress(cmd, namespace, name)

	lines := []string{
		"  Inspect: " + prefix + " " + family + " status " + address,
		"  Finalize: " + prefix + " --yes " + family + " cleanup " + address +
			" --finalize --delete-session --dry-run=false" +
			" (or delete the CR; the finalizer converges storage per the spec reclaim policies)",
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

// workflowHintNamespace resolves the namespace a suggested command needs to
// find this workflow again, across every workflow object shape: cluster
// workflows carry their storage namespace in the spec, while namespaced and
// session-backed records resolve through the lease namespace.
func workflowHintNamespace(
	backend string,
	r *rootState,
	cmd *cobra.Command,
	object crclient.Object,
) string {
	switch current := object.(type) {
	case *v1alpha1.ClusterMigration:
		return clusterMigrationStorageNamespace(current)
	case *v1alpha1.ClusterReservation:
		return clusterWorkflowSpecNamespace(
			string(current.Spec.SessionNamespace),
			string(current.Spec.SourceNamespace),
		)
	case *v1alpha1.ClusterCopy:
		return clusterWorkflowSpecNamespace(
			string(current.Spec.SessionNamespace),
			string(current.Spec.SourceNamespace),
		)
	default:
		return workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object)
	}
}

func clusterWorkflowSpecNamespace(sessionNamespace, sourceNamespace string) string {
	if sessionNamespace != "" {
		return sessionNamespace
	}

	return sourceNamespace
}

// workflowObjectPhase reads the workflow phase from any workflow object
// shape; guidance renderers stay shape-agnostic.
func workflowObjectPhase(object crclient.Object) domain.Phase {
	switch current := object.(type) {
	case *v1alpha1.Migration:
		return current.Status.Phase
	case *v1alpha1.ClusterMigration:
		return current.Status.Phase
	case *v1alpha1.Reservation:
		return current.Status.Phase
	case *v1alpha1.ClusterReservation:
		return current.Status.Phase
	case *v1alpha1.Copy:
		return current.Status.Phase
	case *v1alpha1.ClusterCopy:
		return current.Status.Phase
	default:
		return ""
	}
}

func lifecycleExecuteCommand(
	cmd *cobra.Command,
	prefix, family, subcommand, namespace, name string,
) string {
	return fmt.Sprintf(
		"%s --yes %s %s %s --dry-run=false",
		prefix,
		workflowCommandPath(cmd, family),
		subcommand,
		workflowHintAddress(cmd, namespace, name),
	)
}

// cleanupExecuteCommand renders the execute form of a cleanup invocation,
// mirroring the flags the operator passed so the suggested command preserves
// their reclaim decisions.
func cleanupExecuteCommand(
	cmd *cobra.Command,
	prefix, family, namespace, name string,
	policy string,
	finalize, deleteSession bool,
) string {
	args := workflowHintAddress(cmd, namespace, name)
	if policy != "" {
		args += " --unused-storage-policy " + shellQuote(policy)
	}

	if finalize {
		args += " --finalize"
	}

	if deleteSession {
		args += " --delete-session"
	}

	return fmt.Sprintf(
		"%s --yes %s cleanup %s --dry-run=false",
		prefix,
		workflowCommandPath(cmd, family),
		args,
	)
}
