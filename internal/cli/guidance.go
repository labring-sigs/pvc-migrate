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
func writeSessionGuidance(w io.Writer, session *domain.Session, prefixes guidancePrefixes) error {
	if session == nil {
		return nil
	}

	if sessionHasCapacityFailure(session) {
		return writeCapacityFailureGuidance(w, session, prefixes)
	}

	commands := buildSessionGuidanceCommands(session, prefixes)
	if _, err := fmt.Fprintf(
		w,
		"\nNext steps for session %s (phase %s):\n",
		session.ID,
		session.Status.Phase,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"  Record: ConfigMap %s/%s\n",
		session.Spec.SessionNamespace,
		kube.SessionConfigMapName(session.ID),
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "  Inspect:", commands.status); err != nil {
		return err
	}

	if err := writeSessionInspection(w, session, prefixes.kubectl); err != nil {
		return err
	}

	return writeSessionPhaseGuidance(w, session, commands)
}

type sessionGuidanceCommands struct {
	prefix       string
	status       string
	resumePlan   string
	resume       string
	cleanupPlan  string
	cleanup      string
	keepCopyPlan string
	keepCopy     string
	copyPlan     string
	copyCommand  string
	rollbackPlan string
	rollback     string
	abortPlan    string
	abort        string
}

func buildSessionGuidanceCommands(
	session *domain.Session,
	prefixes guidancePrefixes,
) sessionGuidanceCommands {
	prefix := prefixes.pvcMigrate

	return sessionGuidanceCommands{
		prefix:      prefix,
		status:      fmt.Sprintf("%s session status %s", prefix, session.ID),
		resumePlan:  fmt.Sprintf("%s session resume %s --dry-run", prefix, session.ID),
		resume:      fmt.Sprintf("%s --yes session resume %s --dry-run=false", prefix, session.ID),
		cleanupPlan: fmt.Sprintf("%s %s --dry-run", prefix, cleanupCommandArgs(session)),
		cleanup: fmt.Sprintf(
			"%s --yes %s --dry-run=false",
			prefix,
			cleanupCommandArgs(session),
		),
		keepCopyPlan: fmt.Sprintf(
			"%s session cleanup %s --finalize --delete-session --dry-run",
			prefix,
			session.ID,
		),
		keepCopy: fmt.Sprintf(
			"%s --yes session cleanup %s --finalize --delete-session --dry-run=false",
			prefix,
			session.ID,
		),
		copyPlan:     fmt.Sprintf("%s copy --session %s --dry-run", prefix, session.ID),
		copyCommand:  fmt.Sprintf("%s copy --session %s --dry-run=false", prefix, session.ID),
		rollbackPlan: fmt.Sprintf("%s session rollback %s --dry-run", prefix, session.ID),
		rollback: fmt.Sprintf(
			"%s --yes session rollback %s --dry-run=false",
			prefix,
			session.ID,
		),
		abortPlan: fmt.Sprintf("%s session abort %s --dry-run", prefix, session.ID),
		abort:     fmt.Sprintf("%s --yes session abort %s --dry-run=false", prefix, session.ID),
	}
}

func writeSessionPhaseGuidance(
	w io.Writer,
	session *domain.Session,
	commands sessionGuidanceCommands,
) error {
	switch session.Status.Phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved, domain.PhaseWarmCopying,
		domain.PhaseWarmCopied, domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing,
		domain.PhaseFinalSynced, domain.PhaseActivating, domain.PhaseActivated, domain.PhaseResuming,
		domain.PhaseRenaming, domain.PhaseMoving, domain.PhaseRollingBack, domain.PhaseAborting:
		return writeActiveSessionGuidance(w, session, commands)
	case domain.PhaseFailed:
		return writeFailedSessionGuidance(w, session, commands)
	case domain.PhaseCompleted:
		if session.Spec.Type == domain.SessionTypeBackup {
			return writeCompletedBackupSessionGuidance(w, commands)
		}
		return writeCompletedSessionGuidance(w, commands)
	case domain.PhaseAborted, domain.PhaseRolledBack:
		if session.Spec.Type == domain.SessionTypeBackup {
			return writeClosedBackupSessionGuidance(w, commands)
		}
		return writeClosedSessionGuidance(w, commands)
	default:
		_, err := fmt.Fprintln(
			w,
			"  Inspect the persisted history and resolve the reported precondition before retrying.",
		)

		return err
	}
}

func writeActiveSessionGuidance(
	w io.Writer,
	session *domain.Session,
	commands sessionGuidanceCommands,
) error {
	handled := false
	switch session.Spec.Operation() {
	case domain.OperationReserve:
		if session.Status.Phase == domain.PhaseReserved {
			if _, err := fmt.Fprintln(
				w,
				"  Continue as a copy (validate first):",
				commands.copyPlan,
			); err != nil {
				return err
			}

			if _, err := fmt.Fprintln(
				w,
				"  Continue as a copy:",
				commands.copyCommand,
			); err != nil {
				return err
			}

			handled = true
		}
	case domain.OperationCopy:
		if session.Status.Phase == domain.PhaseWarmCopied {
			if err := writeCopiedSessionGuidance(w, commands); err != nil {
				return err
			}

			handled = true
		}
	}

	if !handled {
		if _, err := fmt.Fprintln(
			w,
			"  Continue (validate first):",
			commands.resumePlan,
		); err != nil {
			return err
		}

		if _, err := fmt.Fprintln(w, "  Continue:", commands.resume); err != nil {
			return err
		}
	}

	if session.Status.Phase == domain.PhaseReserved &&
		session.Spec.Operation() == domain.OperationReserve {
		if _, err := fmt.Fprintln(
			w,
			"  Close retained resources (validate first):",
			commands.cleanupPlan,
		); err != nil {
			return err
		}

		if _, err := fmt.Fprintln(w, "  Close retained resources:", commands.cleanup); err != nil {
			return err
		}
	}

	if !phaseCanAbortBeforeActivation(session) {
		return nil
	}

	if _, err := fmt.Fprintln(
		w,
		"  Abort before activation (validate first):",
		commands.abortPlan,
	); err != nil {
		return err
	}

	_, err := fmt.Fprintln(w, "  Abort before activation:", commands.abort)

	return err
}

func writeCopiedSessionGuidance(w io.Writer, commands sessionGuidanceCommands) error {
	lines := [][2]string{
		{"  Keep the copied PVC and close session (validate first):", commands.keepCopyPlan},
		{"  Keep the copied PVC and close session:", commands.keepCopy},
		{"  Discard the copied PVC and close session (validate first):", commands.cleanupPlan},
		{"  Discard the copied PVC and close session:", commands.cleanup},
	}

	return writeGuidanceLines(w, lines)
}

func writeFailedSessionGuidance(
	w io.Writer,
	session *domain.Session,
	commands sessionGuidanceCommands,
) error {
	if session.Status.FailureReason == domain.FailureDestinationCapacityExhausted {
		lines := [][2]string{
			{"  This session cannot resume because its destination capacity is immutable.", ""},
			{"  Abort pre-cutover work (validate first):", commands.abortPlan},
			{"  Abort:", commands.abort},
			{"  After abort, validate cleanup:", commands.cleanupPlan},
			{"  After abort, finalize cleanup:", commands.cleanup},
		}
		for _, line := range lines {
			var err error
			if line[1] == "" {
				_, err = fmt.Fprintln(w, line[0])
			} else {
				_, err = fmt.Fprintln(w, line[0], line[1])
			}

			if err != nil {
				return err
			}
		}

		_, err := fmt.Fprintf(
			w,
			"  Then rerun the original %s command with a new --session and a larger --destination-capacity.\n",
			session.Spec.Operation(),
		)

		return err
	}

	if _, err := fmt.Fprintln(w, "  Validate recovery:", commands.resumePlan); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "  Resume:", commands.resume); err != nil {
		return err
	}

	if failedCanAbort(session) {
		if _, err := fmt.Fprintln(
			w,
			"  Abort pre-cutover work (validate first):",
			commands.abortPlan,
		); err != nil {
			return err
		}

		_, err := fmt.Fprintln(w, "  Abort:", commands.abort)

		return err
	}

	if _, err := fmt.Fprintln(
		w,
		"  Validate rollback after cutover failure:",
		commands.rollbackPlan,
	); err != nil {
		return err
	}

	_, err := fmt.Fprintln(w, "  Roll back:", commands.rollback)

	return err
}

func writeCompletedSessionGuidance(w io.Writer, commands sessionGuidanceCommands) error {
	lines := [][2]string{
		{"  Verify workload and active PVCs before closing the rollback window.", ""},
		{"  Validate rollback:", commands.rollbackPlan},
		{"  Roll back:", commands.rollback},
		{"  Validate cleanup:", commands.cleanupPlan},
		{"  Finalize and delete retained resources/session:", commands.cleanup},
	}

	return writeGuidanceLines(w, lines)
}

func writeCompletedBackupSessionGuidance(w io.Writer, commands sessionGuidanceCommands) error {
	lines := [][2]string{
		{"  Verify the published recovery point before deleting session credentials.", ""},
		{"  Validate cleanup:", commands.cleanupPlan},
		{"  Finalize and delete retained resources/session:", commands.cleanup},
	}

	return writeGuidanceLines(w, lines)
}

func writeClosedSessionGuidance(w io.Writer, commands sessionGuidanceCommands) error {
	lines := [][2]string{
		{"  Verify workload and PVC state before deleting retained resources.", ""},
		{"  Validate cleanup:", commands.cleanupPlan},
		{"  Finalize and delete retained resources/session:", commands.cleanup},
	}

	return writeGuidanceLines(w, lines)
}

func writeClosedBackupSessionGuidance(w io.Writer, commands sessionGuidanceCommands) error {
	lines := [][2]string{
		{"  Verify the source workload and PVC remain healthy.", ""},
		{"  Validate cleanup:", commands.cleanupPlan},
		{"  Delete retained credentials and session:", commands.cleanup},
	}

	return writeGuidanceLines(w, lines)
}

func writeGuidanceLines(w io.Writer, lines [][2]string) error {
	for _, line := range lines {
		if line[1] == "" {
			if _, err := fmt.Fprintln(w, line[0]); err != nil {
				return err
			}
			continue
		}

		if _, err := fmt.Fprintln(w, line[0], line[1]); err != nil {
			return err
		}
	}

	return nil
}

func phaseCanAbortBeforeActivation(session *domain.Session) bool {
	if session == nil {
		return false
	}

	if session.Spec.Operation() == domain.OperationCopy &&
		session.Status.Phase == domain.PhaseWarmCopied {
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
	if session.Spec.Type != domain.SessionTypeBackup && !session.Spec.Operation().RebindsPVC() {
		args = append(args, "--delete-temporary", "--delete-rollback-pv")
	}

	args = append(args, "--finalize", "--delete-session")

	return strings.Join(args, " ")
}

func writeSessionInspection(w io.Writer, session *domain.Session, kubectlPrefix string) error {
	workload := session.Spec.Workload()
	if workload.Pod.Name != "" && workload.Pod.Namespace != "" {
		if _, err := fmt.Fprintf(
			w,
			"  Verify workload readiness: %s --namespace %s get pod %s -o wide\n",
			kubectlPrefix,
			workload.Pod.Namespace,
			workload.Pod.Name,
		); err != nil {
			return err
		}
	}

	refs := make(map[string][]string)
	for index, volume := range session.Spec.Volumes {
		ref := volume.SourcePVC
		if index < len(session.Status.Volumes) &&
			session.Status.Volumes[index].Activation.ActivePVC.Name != "" {
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

		if _, err := fmt.Fprintf(
			w,
			"  Verify active PVCs: %s --namespace %s get pvc %s\n",
			kubectlPrefix,
			namespace,
			strings.Join(names, " "),
		); err != nil {
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
	case domain.PhaseActivating,
		domain.PhaseActivated,
		domain.PhaseResuming,
		domain.PhaseCompleted,
		domain.PhaseRollingBack:
		return false
	default:
		return true
	}
}

func printSessionResult(cmd interface {
	OutOrStdout() io.Writer
	ErrOrStderr() io.Writer
}, runtime *commandRuntime, session *domain.Session,
) error {
	if err := runtime.printer.Print(session); err != nil {
		return reportSessionError(cmd, session, err)
	}

	return writeSessionGuidance(
		cmd.ErrOrStderr(),
		session,
		guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace),
	)
}

func reportSessionError(
	cmd interface{ ErrOrStderr() io.Writer },
	session *domain.Session,
	err error,
) error {
	if blocker, ok := errors.AsType[*app.CleanupPodBlockerError](err); ok {
		_ = writeCleanupPodBlockerGuidance(cmd.ErrOrStderr(), cmd, blocker)
	}

	if session != nil && errorHasOperation(err, "warm-copy mount probe") {
		_ = writeWarmCopyMountGuidance(cmd.ErrOrStderr(), cmd, session)
		return err
	}

	if session != nil && errorHasOperation(err, "copy capacity") {
		_ = writeCapacityFailureGuidance(
			cmd.ErrOrStderr(),
			session,
			guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace),
		)

		return err
	}

	if session != nil && errorHasOperation(err, "source usage check") {
		_, _ = fmt.Fprintln(
			cmd.ErrOrStderr(),
			"\nSource usage could not be read from a trusted storage-backend CRD. Increase --destination-capacity, or independently verify the data size and create a new session with --skip-source-usage-check.",
		)

		return err
	}

	if session != nil && errorHasOperation(err, "transfer path preflight") {
		_, _ = fmt.Fprintln(
			cmd.ErrOrStderr(),
			"\nTransfer directory validation failed. Correct the source or destination directory and retry. If the stored path is wrong, abort before activation, clean up the retained resources, and create a new session with corrected path flags; transfer paths cannot be changed on an existing session.",
		)
		_ = writeSessionGuidance(
			cmd.ErrOrStderr(),
			session,
			guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace),
		)

		return err
	}

	if session != nil {
		_ = writeSessionGuidance(
			cmd.ErrOrStderr(),
			session,
			guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace),
		)
	}

	return err
}

func writeCapacityFailureGuidance(
	w io.Writer,
	session *domain.Session,
	prefixes guidancePrefixes,
) error {
	if session == nil {
		return nil
	}

	prefix := prefixes.pvcMigrate

	if _, err := fmt.Fprintln(
		w,
		"\nDestination capacity was exhausted. Keep the workload paused when applicable. Abort and clean up this session, then create a new session with a larger --destination-capacity.",
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"\nNext steps for session %s (phase %s):\n",
		session.ID,
		session.Status.Phase,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"  Record: ConfigMap %s/%s\n",
		session.Spec.SessionNamespace,
		kube.SessionConfigMapName(session.ID),
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(
		w,
		"  Inspect:",
		fmt.Sprintf("%s session status %s", prefix, session.ID),
	); err != nil {
		return err
	}

	if err := writeSessionInspection(w, session, prefixes.kubectl); err != nil {
		return err
	}

	if !failedCanAbort(session) {
		if _, err := fmt.Fprintln(
			w,
			"  Validate rollback:",
			fmt.Sprintf("%s session rollback %s --dry-run", prefix, session.ID),
		); err != nil {
			return err
		}

		_, err := fmt.Fprintln(
			w,
			"  Roll back:",
			fmt.Sprintf("%s --yes session rollback %s --dry-run=false", prefix, session.ID),
		)

		return err
	}

	abortPlan := fmt.Sprintf("%s session abort %s --dry-run", prefix, session.ID)
	abort := fmt.Sprintf("%s --yes session abort %s --dry-run=false", prefix, session.ID)
	cleanupPlan := fmt.Sprintf("%s %s --dry-run", prefix, cleanupCommandArgs(session))

	cleanup := fmt.Sprintf("%s --yes %s --dry-run=false", prefix, cleanupCommandArgs(session))
	for _, step := range []struct {
		label   string
		command string
	}{
		{label: "Validate abort and workload restoration:", command: abortPlan},
		{label: "Abort and restore the workload:", command: abort},
		{label: "After abort, validate removal of the undersized destination:", command: cleanupPlan},
		{label: "After abort, remove the undersized destination:", command: cleanup},
	} {
		if _, err := fmt.Fprintln(w, "  "+step.label, step.command); err != nil {
			return err
		}
	}

	_, err := fmt.Fprintln(
		w,
		"  Create a new session with a larger --destination-capacity; capacity cannot be changed on this session.",
	)

	return err
}

func sessionHasCapacityFailure(session *domain.Session) bool {
	return session != nil && session.Status.Phase == domain.PhaseFailed &&
		strings.HasPrefix(session.Status.Message, "copy capacity:")
}

func reportSessionLookupError(
	cmd interface{ ErrOrStderr() io.Writer },
	namespace, id string,
	err error,
) error {
	prefixes := guidancePrefixesForCommand(cmd, namespace)
	prefix := prefixes.pvcMigrate

	_, _ = fmt.Fprintf(
		cmd.ErrOrStderr(),
		"\nSession lookup failed. List persisted sessions: %s session status\n",
		prefix,
	)
	if id != "" {
		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"Inspect the expected record: %s --namespace %s get configmap %s\n",
			prefixes.kubectl,
			namespace,
			kube.SessionConfigMapName(id),
		)
	}

	return err
}

func reportPlanningError(cmd interface{ ErrOrStderr() io.Writer }, err error) error {
	_, _ = fmt.Fprintln(
		cmd.ErrOrStderr(),
		"\nPlanning ended before session creation. Correct the reported condition and rerun the command.",
	)

	return err
}

func reportPreSessionError(cmd interface{ ErrOrStderr() io.Writer }, err error) error {
	_, _ = fmt.Fprintln(
		cmd.ErrOrStderr(),
		"\nThe command stopped before session creation. Correct the reported condition and rerun; dry-run remains the default.",
	)

	return err
}

func reportApprovalError(cmd interface{ ErrOrStderr() io.Writer }, err error) error {
	_, _ = fmt.Fprintln(
		cmd.ErrOrStderr(),
		"\nApproval stopped before the protected action began. Revalidate with --dry-run, then rerun and type the requested value exactly or use --yes.",
	)

	return err
}

func reportRuntimeError(cmd interface{ ErrOrStderr() io.Writer }, err error) error {
	_, _ = fmt.Fprintln(
		cmd.ErrOrStderr(),
		"\nCommand initialization stopped before any cluster operation. Check --kubeconfig, --context, --output, --log-format, --log-level, and --color, then rerun the command.",
	)

	return err
}

func reportTransferError(
	cmd interface{ ErrOrStderr() io.Writer },
	operation, namespace, pvc string,
	err error,
) error {
	_, _ = fmt.Fprintf(
		cmd.ErrOrStderr(),
		"\n%s stopped before confirmed completion. Inspect the PVC state: %s --namespace %s get pvc %s\n",
		operation,
		kubectlCommandPrefixForCommand(cmd),
		namespace,
		pvc,
	)
	if errorHasOperation(err, "transfer path preflight") {
		_, _ = fmt.Fprintln(
			cmd.ErrOrStderr(),
			"Correct --path and rerun the write command. Object-storage plan validates the path syntax and PVC state, while the mounted directory and symbolic-link checks run immediately before data transfer.",
		)

		return err
	}

	_, _ = fmt.Fprintln(
		cmd.ErrOrStderr(),
		"Fix the reported condition, rerun its plan, then execute with --dry-run=false.",
	)

	return err
}

func writeTransferDryRunGuidance(
	w io.Writer,
	operation, namespace, pvc, kubectlPrefix string,
) error {
	_, err := fmt.Fprintf(
		w,
		"\n%s dry-run completed without cluster mutations. Inspect the PVC with %s --namespace %s get pvc %s, then run the write command with --dry-run=false.\n",
		operation,
		kubectlPrefix,
		namespace,
		pvc,
	)

	return err
}

func reportSessionCreationError(
	cmd interface{ ErrOrStderr() io.Writer },
	namespace, id string,
	err error,
) error {
	prefixes := guidancePrefixesForCommand(cmd, namespace)
	prefix := prefixes.pvcMigrate
	_, _ = fmt.Fprintf(
		cmd.ErrOrStderr(),
		"\nSession creation did not return a confirmed result. Inspect before retrying: %s session status %s\n",
		prefix,
		id,
	)
	_, _ = fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Inspect the expected record directly: %s --namespace %s get configmap %s\n",
		prefixes.kubectl,
		namespace,
		kube.SessionConfigMapName(id),
	)

	return err
}

func reportCleanupError(
	cmd interface{ ErrOrStderr() io.Writer },
	session *domain.Session,
	options app.CleanupOptions,
	err error,
) error {
	if blocker, ok := errors.AsType[*app.CleanupPodBlockerError](err); ok {
		_ = writeCleanupPodBlockerGuidance(cmd.ErrOrStderr(), cmd, blocker)
	}

	deleteRecordFailed := errorHasOperation(err, "delete session", "delete session lock")
	if session != nil && options.DeleteSession && deleteRecordFailed {
		prefixes := guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace)
		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"\nCleanup stopped during session record removal. Inspect the ConfigMap: %s --namespace %s get configmap %s\n",
			prefixes.kubectl,
			session.Spec.SessionNamespace,
			kube.SessionConfigMapName(session.ID),
		)
		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"Inspect the Lease: %s --namespace %s get lease %s\n",
			prefixes.kubectl,
			session.Spec.SessionNamespace,
			kube.SessionLockName(session.ID),
		)
		_, _ = fmt.Fprintln(
			cmd.ErrOrStderr(),
			"Use session status when the ConfigMap remains; use session cleanup-orphan when ownership remains after the ConfigMap is gone.",
		)

		return err
	}

	if session != nil {
		prefix := guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace).pvcMigrate
		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"\nCleanup stopped before confirmed completion. Inspect current state: %s session status %s\n",
			prefix,
			session.ID,
		)
		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"Revalidate cleanup before retrying: %s %s --dry-run\n",
			prefix,
			cleanupCommandArgsForOptions(session.ID, options),
		)

		return err
	}

	return reportSessionError(cmd, session, err)
}

func writeWarmCopyMountGuidance(w io.Writer, command any, session *domain.Session) error {
	if session == nil {
		return nil
	}

	prefixes := guidancePrefixesForCommand(command, session.Spec.SessionNamespace)
	abortArgs := "session abort " + session.ID
	cleanupArgs := cleanupCommandArgsForOptions(session.ID, app.CleanupOptions{
		DeleteTemporary: true,
		DeleteRollback:  true,
		Finalize:        true,
		DeleteSession:   true,
	})

	if _, err := fmt.Fprintln(
		w,
		"\nWarm-copy mount compatibility failed before the protected cutover began.",
	); err != nil {
		return err
	}

	if err := writeSessionInspection(w, session, prefixes.kubectl); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"  Validate abort: %s %s --dry-run\n",
		prefixes.pvcMigrate,
		abortArgs,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"  Abort the pre-cutover session: %s --yes %s --dry-run=false\n",
		prefixes.pvcMigrate,
		abortArgs,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"  Validate cleanup after abort: %s %s --dry-run\n",
		prefixes.pvcMigrate,
		cleanupArgs,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"  Delete retained resources/session after abort: %s --yes %s --dry-run=false\n",
		prefixes.pvcMigrate,
		cleanupArgs,
	); err != nil {
		return err
	}

	retry := "Rerun the original migration with --precopy-passes 0 after cleanup completes."
	if session.Spec.Operation() == domain.OperationCopy {
		retry = "Rerun the original copy without --online after cleanup completes and the source PVC has no active Pod consumers."
	}

	if _, err := fmt.Fprintln(w, "  "+retry); err != nil {
		return err
	}

	return nil
}

func writeCleanupPodBlockerGuidance(
	w io.Writer,
	command any,
	blocker *app.CleanupPodBlockerError,
) error {
	if blocker == nil {
		return nil
	}

	kubectlPrefix := kubectlCommandPrefixForCommand(command)

	if _, err := fmt.Fprintf(
		w,
		"\nCleanup action for PVC %s/%s:\n",
		blocker.PVCNamespace,
		blocker.PVCName,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"  Inspect blocking Pod: %s --namespace %s get pod %s -o wide\n",
		kubectlPrefix,
		blocker.PodNamespace,
		blocker.PodName,
	); err != nil {
		return err
	}

	if blocker.OwnerKind != "" && blocker.OwnerName != "" {
		resource := strings.ToLower(blocker.OwnerKind)
		if _, err := fmt.Fprintf(
			w,
			"  Inspect owning %s: %s --namespace %s get %s %s -o wide\n",
			blocker.OwnerKind,
			kubectlPrefix,
			blocker.PodNamespace,
			resource,
			blocker.OwnerName,
		); err != nil {
			return err
		}

		if blocker.SessionOwned && blocker.OwnerVerified {
			if _, err := fmt.Fprintf(
				w,
				"  Delete owning migration %s and its Pod(s): %s --namespace %s delete %s %s --ignore-not-found=true --wait=true\n",
				blocker.OwnerKind,
				kubectlPrefix,
				blocker.PodNamespace,
				resource,
				blocker.OwnerName,
			); err != nil {
				return err
			}
		} else if blocker.SessionOwned {
			if _, err := fmt.Fprintf(
				w,
				"  The current %s/%s UID could not be verified against the Pod owner reference; inspect it before deleting any controller.\n",
				blocker.OwnerKind,
				blocker.OwnerName,
			); err != nil {
				return err
			}
		} else if _, err := fmt.Fprintf(w, "  Pod recreation is controlled by %s/%s; stop or remove that controller after verifying it is safe.\n", blocker.OwnerKind, blocker.OwnerName); err != nil {
			return err
		}
	}

	label := "Delete blocking Pod object"
	if blocker.Terminal {
		label = "Delete terminal Pod object"
	} else if blocker.OwnerKind != "" && !blocker.SessionOwned {
		label = "Delete blocking Pod after its controller is stopped"
	}

	if _, err := fmt.Fprintf(
		w,
		"  %s: %s --namespace %s delete pod %s --ignore-not-found=true --wait=true\n",
		label,
		kubectlPrefix,
		blocker.PodNamespace,
		blocker.PodName,
	); err != nil {
		return err
	}

	return nil
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

func printPlanResult(
	cmd interface{ ErrOrStderr() io.Writer },
	runtime *commandRuntime,
	plan *domain.MigrationPlan,
) error {
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
			advice = "Workload action: use a supported workload adapter or the controller's native maintenance procedure, then rerun the plan; ordinary Deployments require no operator owner, and directly scaled Deployments and StatefulSets require no HorizontalPodAutoscaler."
		case check.Name == "target-node":
			advice = "Node action: choose a Ready, schedulable target with --target-node, or correct the target node condition before rerunning the plan."
		case check.Name == "storage-topology" || check.Name == "storage-capacity":
			advice = "Storage action: choose a compatible StorageClass or target node, then verify topology and capacity before rerunning the plan."
		case check.Name == "destination-capacity":
			advice = "Capacity action: correct --destination-capacity, or add --allow-volume-shrink only after verifying the copied data fits in every smaller destination PVC."
		case check.Name == "source-usage":
			advice = "Usage action: use a destination that is at least the source capacity, or independently verify the data size and rerun with --skip-source-usage-check."
		case check.Name == "migration-needed":
			advice = "Migration action: the requested node and StorageClass already match; use --force-reprovision only for an intentional backing-PV replacement."
		case check.Name == "warm-copy-mount" && plan.SessionSpec.Operation() == domain.OperationCopy:
			advice = "Copy action: stop all active PVC consumers and rerun without --online, or use storage that explicitly supports a second same-node Pod mount."
		case check.Name == "warm-copy-mount":
			advice = "Warm-copy action: rerun with --precopy-passes 0 for offline final sync, or use storage that explicitly supports a second same-node Pod mount."
			if strings.Contains(check.Message, "OpenEBS LVM") {
				advice = "OpenEBS LVM action: rerun with --precopy-passes 0 for offline final sync, or explicitly pass --openebs-lvm-enable-shared to temporarily patch the matching LVMVolume before the mount probe."
			}
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
			if unicode.IsLetter(char) || unicode.IsDigit(char) ||
				strings.ContainsRune("-._/:%+=,", char) {
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

func writeSessionListGuidance(
	w io.Writer,
	namespace string,
	sessions []*domain.Session,
	prefix string,
) error {
	if len(sessions) == 0 {
		_, err := fmt.Fprintf(w, "\nNo migration sessions found in %s.\n", namespace)
		return err
	}

	_, err := fmt.Fprintf(
		w,
		"\nInspect a session and its next steps: %s session status SESSION\n",
		prefix,
	)

	return err
}
