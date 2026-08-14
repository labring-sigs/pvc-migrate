package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"sort"
	"strings"
	"unicode"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
)

type guidancePrefixes struct {
	pvcMigrate string
	kubectl    string
}

type guidanceShell uint8

const (
	guidanceShellPOSIX guidanceShell = iota
	guidanceShellPowerShell
)

// writeSessionGuidance keeps operational instructions on stderr so JSON and
// YAML stdout remain one parseable document. Every destructive command is
// shown with its dry-run form first and explicit approval on execution.
func writeSessionGuidance(w io.Writer, session *domain.Session, supplied ...guidancePrefixes) error {
	if session == nil {
		return nil
	}
	prefixes := guidancePrefixes{pvcMigrate: sessionCommandPrefix(session.Spec.SessionNamespace), kubectl: "kubectl"}
	if len(supplied) > 0 {
		prefixes = supplied[0]
	}
	prefix := prefixes.pvcMigrate
	status := fmt.Sprintf("%s session status %s", prefix, session.ID)
	resumePlan := fmt.Sprintf("%s session resume %s --dry-run", prefix, session.ID)
	resume := fmt.Sprintf("%s --yes session resume %s --dry-run=false", prefix, session.ID)
	cleanupPlan := fmt.Sprintf("%s %s --dry-run", prefix, cleanupCommandArgs(session))
	cleanup := fmt.Sprintf("%s --yes %s --dry-run=false", prefix, cleanupCommandArgs(session))
	keepCopyPlan := fmt.Sprintf("%s session cleanup %s --finalize --delete-session --dry-run", prefix, session.ID)
	keepCopy := fmt.Sprintf("%s --yes session cleanup %s --finalize --delete-session --dry-run=false", prefix, session.ID)
	copyPlan := fmt.Sprintf("%s copy --session %s --dry-run", prefix, session.ID)
	copy := fmt.Sprintf("%s copy --session %s --dry-run=false", prefix, session.ID)
	rollbackPlan := fmt.Sprintf("%s session rollback %s --dry-run", prefix, session.ID)
	rollback := fmt.Sprintf("%s --yes session rollback %s --dry-run=false", prefix, session.ID)
	abortPlan := fmt.Sprintf("%s session abort %s --dry-run", prefix, session.ID)
	abort := fmt.Sprintf("%s --yes session abort %s --dry-run=false", prefix, session.ID)

	if _, err := fmt.Fprintf(w, "\nNext steps for session %s (phase %s):\n", session.ID, session.Status.Phase); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Record: ConfigMap %s/%s\n", session.Spec.SessionNamespace, kube.SessionConfigMapName(session.ID)); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "  Inspect:", status); err != nil {
		return err
	}
	if err := writeSessionInspection(w, session, prefixes.kubectl); err != nil {
		return err
	}
	switch session.Status.Phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved, domain.PhaseWarmCopying,
		domain.PhaseWarmCopied, domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing,
		domain.PhaseFinalSynced, domain.PhaseActivating, domain.PhaseActivated, domain.PhaseResuming,
		domain.PhaseRenaming, domain.PhaseMoving, domain.PhaseRollingBack, domain.PhaseAborting:
		switch {
		case session.Spec.Operation() == domain.OperationReserve && session.Status.Phase == domain.PhaseReserved:
			if _, err := fmt.Fprintln(w, "  Continue as a copy (validate first):", copyPlan); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, "  Continue as a copy:", copy); err != nil {
				return err
			}
		case session.Spec.Operation() == domain.OperationCopy && session.Status.Phase == domain.PhaseWarmCopied:
			if _, err := fmt.Fprintln(w, "  Keep the copied PVC and close session (validate first):", keepCopyPlan); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, "  Keep the copied PVC and close session:", keepCopy); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, "  Discard the copied PVC and close session (validate first):", cleanupPlan); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, "  Discard the copied PVC and close session:", cleanup); err != nil {
				return err
			}
		default:
			if _, err := fmt.Fprintln(w, "  Continue (validate first):", resumePlan); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, "  Continue:", resume); err != nil {
				return err
			}
		}
		if session.Status.Phase == domain.PhaseReserved && session.Spec.Operation() == domain.OperationReserve {
			if _, err := fmt.Fprintln(w, "  Close retained resources (validate first):", cleanupPlan); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, "  Close retained resources:", cleanup); err != nil {
				return err
			}
		}
		if phaseCanAbortBeforeActivation(session) {
			if _, err := fmt.Fprintln(w, "  Abort before activation (validate first):", abortPlan); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, "  Abort before activation:", abort); err != nil {
				return err
			}
		}
	case domain.PhaseFailed:
		if _, err := fmt.Fprintln(w, "  Validate recovery:", resumePlan); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "  Resume:", resume); err != nil {
			return err
		}
		if failedCanAbort(session) {
			if _, err := fmt.Fprintln(w, "  Abort pre-cutover work (validate first):", fmt.Sprintf("%s session abort %s --dry-run", prefix, session.ID)); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, "  Abort:", fmt.Sprintf("%s --yes session abort %s --dry-run=false", prefix, session.ID)); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintln(w, "  Validate rollback after cutover failure:", rollbackPlan); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, "  Roll back:", rollback); err != nil {
				return err
			}
		}
	case domain.PhaseCompleted:
		if _, err := fmt.Fprintln(w, "  Verify workload and active PVCs before closing the rollback window."); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "  Validate rollback:", rollbackPlan); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "  Roll back:", rollback); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "  Validate cleanup:", cleanupPlan); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "  Finalize and delete retained resources/session:", cleanup); err != nil {
			return err
		}
	case domain.PhaseAborted, domain.PhaseRolledBack:
		if _, err := fmt.Fprintln(w, "  Verify workload and PVC state before deleting retained resources."); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "  Validate cleanup:", cleanupPlan); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "  Finalize and delete retained resources/session:", cleanup); err != nil {
			return err
		}
	default:
		if _, err := fmt.Fprintln(w, "  Inspect the persisted history and resolve the reported precondition before retrying."); err != nil {
			return err
		}
	}
	return nil
}

func phaseCanAbortBeforeActivation(session *domain.Session) bool {
	if session == nil {
		return false
	}
	if session.Spec.Operation() == domain.OperationCopy && session.Status.Phase == domain.PhaseWarmCopied {
		return false
	}
	switch session.Status.Phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved, domain.PhaseWarmCopying,
		domain.PhaseWarmCopied, domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing,
		domain.PhaseFinalSynced:
		return true
	default:
		return false
	}
}

func cleanupCommandArgs(session *domain.Session) string {
	args := []string{"session", "cleanup", session.ID}
	if !session.Spec.Operation().RebindsPVC() {
		args = append(args, "--delete-temporary", "--delete-rollback-pv")
	}
	args = append(args, "--finalize", "--delete-session")
	return strings.Join(args, " ")
}

func writeSessionInspection(w io.Writer, session *domain.Session, kubectlPrefix string) error {
	workload := session.Spec.Workload()
	if workload.Pod.Name != "" && workload.Pod.Namespace != "" {
		if _, err := fmt.Fprintf(w, "  Verify workload readiness: %s --namespace %s get pod %s -o wide\n", kubectlPrefix, workload.Pod.Namespace, workload.Pod.Name); err != nil {
			return err
		}
	}
	refs := make(map[string][]string)
	for index, volume := range session.Spec.Volumes {
		ref := volume.SourcePVC
		if index < len(session.Status.Volumes) && session.Status.Volumes[index].Activation.ActivePVC.Name != "" {
			ref = session.Status.Volumes[index].Activation.ActivePVC
		}
		if ref.Namespace == "" || ref.Name == "" {
			continue
		}
		refs[ref.Namespace] = append(refs[ref.Namespace], ref.Name)
	}
	namespaces := make([]string, 0, len(refs))
	for namespace := range refs {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	for _, namespace := range namespaces {
		names := refs[namespace]
		sort.Strings(names)
		if _, err := fmt.Fprintf(w, "  Verify active PVCs: %s --namespace %s get pvc %s\n", kubectlPrefix, namespace, strings.Join(names, " ")); err != nil {
			return err
		}
	}
	return nil
}

func failedCanAbort(session *domain.Session) bool {
	if session == nil || session.Status.Phase != domain.PhaseFailed {
		return false
	}
	switch session.Status.ResumeFrom {
	case domain.PhaseActivating, domain.PhaseActivated, domain.PhaseResuming, domain.PhaseCompleted, domain.PhaseRollingBack:
		return false
	default:
		return true
	}
}

func printSessionResult(cmd interface {
	OutOrStdout() io.Writer
	ErrOrStderr() io.Writer
}, runtime *commandRuntime, session *domain.Session) error {
	if err := runtime.printer.Print(session); err != nil {
		return reportSessionError(cmd, session, err)
	}
	return writeSessionGuidance(cmd.ErrOrStderr(), session, guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace))
}

func reportSessionError(cmd interface{ ErrOrStderr() io.Writer }, session *domain.Session, err error) error {
	if session != nil {
		_ = writeSessionGuidance(cmd.ErrOrStderr(), session, guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace))
	}
	return err
}

func reportSessionLookupError(cmd interface{ ErrOrStderr() io.Writer }, namespace, id string, err error) error {
	prefixes := guidancePrefixesForCommand(cmd, namespace)
	prefix := prefixes.pvcMigrate
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\nSession lookup failed. List persisted sessions: %s session status\n", prefix)
	if id != "" {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Inspect the expected record: %s --namespace %s get configmap %s\n", prefixes.kubectl, namespace, kube.SessionConfigMapName(id))
	}
	return err
}

func reportPlanningError(cmd interface{ ErrOrStderr() io.Writer }, err error) error {
	_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "\nPlanning ended before session creation. Correct the reported condition and rerun the command.")
	return err
}

func reportPreSessionError(cmd interface{ ErrOrStderr() io.Writer }, err error) error {
	_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "\nThe command stopped before session creation. Correct the reported condition and rerun; dry-run remains the default.")
	return err
}

func reportApprovalError(cmd interface{ ErrOrStderr() io.Writer }, err error) error {
	_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "\nApproval stopped before the protected action began. Revalidate with --dry-run, then rerun and type the requested value exactly or use --yes.")
	return err
}

func reportRuntimeError(cmd interface{ ErrOrStderr() io.Writer }, err error) error {
	_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "\nCommand initialization stopped before any cluster operation. Check --kubeconfig, --context, --output, --log-format, --log-level, and --color, then rerun the command.")
	return err
}

func reportTransferError(cmd interface{ ErrOrStderr() io.Writer }, operation, namespace, pvc string, err error) error {
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\n%s stopped before confirmed completion. Inspect the PVC state: %s --namespace %s get pvc %s\n", operation, kubectlCommandPrefixForCommand(cmd), namespace, pvc)
	_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "Fix the reported condition, rerun its plan, then execute with --dry-run=false.")
	return err
}

func writeTransferDryRunGuidance(w io.Writer, operation, namespace, pvc string, supplied ...string) error {
	kubectlPrefix := "kubectl"
	if len(supplied) > 0 {
		kubectlPrefix = supplied[0]
	}
	_, err := fmt.Fprintf(w, "\n%s dry-run completed without cluster mutations. Inspect the PVC with %s --namespace %s get pvc %s, then run the write command with --dry-run=false.\n", operation, kubectlPrefix, namespace, pvc)
	return err
}

func reportSessionCreationError(cmd interface{ ErrOrStderr() io.Writer }, namespace, id string, err error) error {
	prefixes := guidancePrefixesForCommand(cmd, namespace)
	prefix := prefixes.pvcMigrate
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\nSession creation did not return a confirmed result. Inspect before retrying: %s session status %s\n", prefix, id)
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Inspect the expected record directly: %s --namespace %s get configmap %s\n", prefixes.kubectl, namespace, kube.SessionConfigMapName(id))
	return err
}

func reportCleanupError(cmd interface{ ErrOrStderr() io.Writer }, session *domain.Session, options app.CleanupOptions, err error) error {
	deleteRecordFailed := errorHasOperation(err, "delete session", "delete session lock")
	if session != nil && options.DeleteSession && deleteRecordFailed {
		prefixes := guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace)
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\nCleanup stopped during session record removal. Inspect the ConfigMap: %s --namespace %s get configmap %s\n", prefixes.kubectl, session.Spec.SessionNamespace, kube.SessionConfigMapName(session.ID))
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Inspect the Lease: %s --namespace %s get lease %s\n", prefixes.kubectl, session.Spec.SessionNamespace, kube.SessionLockName(session.ID))
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "Use session status when the ConfigMap remains; use session cleanup-orphan when ownership remains after the ConfigMap is gone.")
		return err
	}
	if session != nil {
		prefix := guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace).pvcMigrate
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\nCleanup stopped before confirmed completion. Inspect current state: %s session status %s\n", prefix, session.ID)
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Revalidate cleanup before retrying: %s %s --dry-run\n", prefix, cleanupCommandArgsForOptions(session.ID, options))
		return err
	}
	return reportSessionError(cmd, session, err)
}

func errorHasOperation(err error, operations ...string) bool {
	if err == nil {
		return false
	}
	var typed *domain.Error
	if errors.As(err, &typed) && slices.Contains(operations, typed.Op) {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, nested := range joined.Unwrap() {
			if errorHasOperation(nested, operations...) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return errorHasOperation(wrapped.Unwrap(), operations...)
	}
	return false
}

func cleanupCommandArgsForOptions(id string, options app.CleanupOptions) string {
	args := []string{"session", "cleanup", id}
	if options.DeleteTemporary {
		args = append(args, "--delete-temporary")
	}
	if options.DeleteRollback {
		args = append(args, "--delete-rollback-pv")
	}
	if options.Finalize {
		args = append(args, "--finalize")
	}
	if options.DeleteSession {
		args = append(args, "--delete-session")
	}
	return strings.Join(args, " ")
}

func printPlanResult(cmd interface{ ErrOrStderr() io.Writer }, runtime *commandRuntime, plan *domain.MigrationPlan) error {
	if err := runtime.printer.Print(plan); err != nil {
		return reportPlanningError(cmd, err)
	}
	message := "\nPlanning completed without cluster mutations. Resolve the failed checks, then rerun the command."
	if plan.Ready {
		message = "\nDry-run completed without cluster mutations. Run the write command with the same inputs and --dry-run=false; provide --yes or typed approval when requested."
	}
	if _, err := fmt.Fprintln(cmd.ErrOrStderr(), message); err != nil {
		return err
	}
	if plan.Ready {
		return nil
	}
	return writePlanFailureGuidance(cmd.ErrOrStderr(), plan)
}

func writePlanFailureGuidance(w io.Writer, plan *domain.MigrationPlan) error {
	if plan == nil {
		return nil
	}
	seen := make(map[string]struct{})
	for _, check := range plan.Checks {
		if check.Passed || check.Severity != domain.SeverityError {
			continue
		}
		var advice string
		switch {
		case strings.Contains(check.Message, "PVC retention whenScaled is"):
			advice = "StatefulSet action: set persistentVolumeClaimRetentionPolicy.whenScaled=Retain and verify the StatefulSet before rerunning the plan."
		case strings.Contains(check.Message, "scale-down affects"):
			advice = "StatefulSet action: complete an application switchover, or explicitly acknowledge the restart with --allow-leader-downtime when the workload can tolerate it."
		case check.Name == "pvc-consumers":
			advice = "PVC action: stop or select every consumer with --pod, then verify that the PVC has no unmanaged Pod references before rerunning the plan."
		case check.Name == "controller-adapter":
			advice = "Workload action: use the controller's native maintenance or pause procedure, then rerun the plan; migrate-pod mutates only workloads with a verified adapter."
		case check.Name == "target-node":
			advice = "Node action: choose a Ready, schedulable target with --target-node, or correct the target node condition before rerunning the plan."
		case check.Name == "storage-topology" || check.Name == "storage-capacity":
			advice = "Storage action: choose a compatible StorageClass or target node, then verify topology and capacity before rerunning the plan."
		case check.Name == "migration-needed":
			advice = "Migration action: the requested node and StorageClass already match; use --force-reprovision only for an intentional backing-PV replacement."
		}
		if advice == "" {
			continue
		}
		if _, exists := seen[advice]; exists {
			continue
		}
		seen[advice] = struct{}{}
		if _, err := fmt.Fprintln(w, "  "+advice); err != nil {
			return err
		}
	}
	return nil
}

func sessionCommandPrefix(namespace string) string {
	return sessionCommandPrefixForCommand(nil, namespace)
}

func sessionCommandPrefixForCommand(value any, namespace string) string {
	args := []string{"pvc-migrate"}
	command, ok := value.(*cobra.Command)
	if ok {
		rootFlags := command.Root().PersistentFlags()
		for _, name := range []string{"kubeconfig", "context"} {
			if flag := rootFlags.Lookup(name); flag != nil && flag.Value.String() != "" {
				args = append(args, "--"+name, shellQuote(flag.Value.String()))
			}
		}
		for _, name := range []string{"timeout", "retries", "retry-backoff", "helm-timeout", "stream-tool-logs", "no-compress"} {
			if flag := rootFlags.Lookup(name); flag != nil && flag.Changed {
				args = append(args, "--"+name+"="+shellQuote(flag.Value.String()))
			}
		}
	}
	if namespace != "" && namespace != "pvc-migrate-system" {
		args = append(args, "--session-namespace", shellQuote(namespace))
	}
	return strings.Join(args, " ")
}

func kubectlCommandPrefixForCommand(value any) string {
	args := []string{"kubectl"}
	command, ok := value.(*cobra.Command)
	if !ok {
		return args[0]
	}
	rootFlags := command.Root().PersistentFlags()
	for _, name := range []string{"kubeconfig", "context"} {
		if flag := rootFlags.Lookup(name); flag != nil && flag.Value.String() != "" {
			args = append(args, "--"+name, shellQuote(flag.Value.String()))
		}
	}
	return strings.Join(args, " ")
}

func guidancePrefixesForCommand(value any, namespace string) guidancePrefixes {
	return guidancePrefixes{
		pvcMigrate: sessionCommandPrefixForCommand(value, namespace),
		kubectl:    kubectlCommandPrefixForCommand(value),
	}
}

func shellQuote(value string) string {
	return shellQuoteFor(value, detectGuidanceShell(runtime.GOOS, os.Getenv))
}

func detectGuidanceShell(goos string, getenv func(string) string) guidanceShell {
	if goos == "windows" {
		if getenv("MSYSTEM") != "" {
			return guidanceShellPOSIX
		}
		return guidanceShellPowerShell
	}
	if getenv("PSModulePath") != "" {
		return guidanceShellPowerShell
	}
	return guidanceShellPOSIX
}

func shellQuoteFor(value string, shell guidanceShell) string {
	if value != "" {
		safe := true
		for _, char := range value {
			if unicode.IsLetter(char) || unicode.IsDigit(char) || strings.ContainsRune("-._/:%+=,", char) {
				continue
			}
			safe = false
			break
		}
		if safe {
			return value
		}
	}
	if shell == guidanceShellPowerShell {
		return "'" + strings.ReplaceAll(value, "'", "''") + "'"
	}
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func writeSessionListGuidance(w io.Writer, namespace string, sessions []*domain.Session, supplied ...string) error {
	prefix := sessionCommandPrefix(namespace)
	if len(supplied) > 0 {
		prefix = supplied[0]
	}
	if len(sessions) == 0 {
		_, err := fmt.Fprintf(w, "\nNo migration sessions found in %s.\n", namespace)
		return err
	}
	_, err := fmt.Fprintf(w, "\nInspect a session and its next steps: %s session status SESSION\n", prefix)
	return err
}
