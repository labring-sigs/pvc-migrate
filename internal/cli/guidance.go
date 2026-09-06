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

	if session.Deleting {
		if _, err := fmt.Fprintln(
			w,
			"\nWorkflow deletion is in progress; the controller owns recovery and cleanup. Inspect the Deleting and DeletionBlocked conditions and workflow Events. Do not remove the protection finalizer while recovery is incomplete.",
		); err != nil {
			return err
		}

		return writeSessionInspection(w, session, prefixes.kubectl)
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

	if err := writeSessionRecord(w, session); err != nil {
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
	workflow := workflowCommandName(session)

	return sessionGuidanceCommands{
		prefix:     prefix,
		status:     fmt.Sprintf("%s %s status %s", prefix, workflow, session.ID),
		resumePlan: fmt.Sprintf("%s %s resume %s --dry-run", prefix, workflow, session.ID),
		resume: fmt.Sprintf(
			"%s --yes %s resume %s --dry-run=false",
			prefix,
			workflow,
			session.ID,
		),
		cleanupPlan: fmt.Sprintf("%s %s --dry-run", prefix, cleanupCommandArgs(session)),
		cleanup: fmt.Sprintf(
			"%s --yes %s --dry-run=false",
			prefix,
			cleanupCommandArgs(session),
		),
		keepCopyPlan: fmt.Sprintf(
			"%s %s cleanup %s --finalize --delete-session --dry-run",
			prefix,
			workflow,
			session.ID,
		),
		keepCopy: fmt.Sprintf(
			"%s --yes %s cleanup %s --finalize --delete-session --dry-run=false",
			prefix,
			workflow,
			session.ID,
		),
		copyPlan:     fmt.Sprintf("%s copy --session %s --dry-run", prefix, session.ID),
		copyCommand:  fmt.Sprintf("%s copy --session %s --dry-run=false", prefix, session.ID),
		rollbackPlan: fmt.Sprintf("%s %s rollback %s --dry-run", prefix, workflow, session.ID),
		rollback: fmt.Sprintf(
			"%s --yes %s rollback %s --dry-run=false",
			prefix,
			workflow,
			session.ID,
		),
		abortPlan: fmt.Sprintf("%s %s abort %s --dry-run", prefix, workflow, session.ID),
		abort: fmt.Sprintf(
			"%s --yes %s abort %s --dry-run=false",
			prefix,
			workflow,
			session.ID,
		),
	}
}

func workflowCommandName(session *domain.Session) string {
	if session == nil {
		return "migrate"
	}

	switch session.Spec.Type {
	case domain.SessionTypeMigrate:
		return "migrate"
	case domain.SessionTypeMigratePod:
		return "migrate-pod"
	case domain.SessionTypeReserve:
		return "reserve"
	case domain.SessionTypeCopy:
		return "copy"
	case domain.SessionTypeBackup:
		return "backup"
	case domain.SessionTypeRestore:
		return "restore"
	case domain.SessionTypeRename:
		return "rename"
	case domain.SessionTypeMove:
		return "move"
	default:
		return "migrate"
	}
}

// workflowCommandNameForCommand identifies the local workflow owning a CLI
// command before a Session has been loaded. This keeps pre-session errors
// actionable without routing every failure through a global session command.
func workflowCommandNameForCommand(value any) string {
	command, ok := value.(*cobra.Command)
	if !ok || command == nil {
		return "migrate"
	}

	root := command.Root()

	current := command
	for current != nil && current.Parent() != nil && current.Parent() != root {
		current = current.Parent()
	}

	if current == nil || current == root {
		return "migrate"
	}

	switch current.Name() {
	case "migrate", "migrate-pod", "reserve", "copy", "backup", "restore", "rename", "move":
		return current.Name()
	default:
		return "migrate"
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

		if session.Spec.Type == domain.SessionTypeRestore {
			return writeCompletedRestoreSessionGuidance(w, commands)
		}

		return writeCompletedSessionGuidance(w, commands)
	case domain.PhaseAborted, domain.PhaseRolledBack:
		if session.Spec.Type == domain.SessionTypeBackup {
			return writeClosedBackupSessionGuidance(w, commands)
		}

		if session.Spec.Type == domain.SessionTypeRestore {
			return writeClosedRestoreSessionGuidance(w, commands)
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

		if kubeblocks, ok := session.Spec.KubeBlocksPodMigration(); ok {
			_, err := fmt.Fprintf(
				w,
				"  Update KubeBlocks Cluster %s component %s volumeClaimTemplates storage request, then create a new migrate-pod session after cleanup.\n",
				kubeblocks.Cluster,
				kubeblocks.Component,
			)

			return err
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

	if session.Spec.Type == domain.SessionTypeBackup ||
		session.Spec.Type == domain.SessionTypeRestore {
		if failedCanAbort(session) {
			_, err := fmt.Fprintln(w, "  Abort:", commands.abort)
			return err
		}

		return nil
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

func writeCompletedRestoreSessionGuidance(w io.Writer, commands sessionGuidanceCommands) error {
	lines := [][2]string{
		{"  Verify the restored destination PVC before deleting session credentials.", ""},
		{"  Validate cleanup:", commands.cleanupPlan},
		{"  Finalize and delete session/credentials:", commands.cleanup},
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

func writeClosedRestoreSessionGuidance(w io.Writer, commands sessionGuidanceCommands) error {
	lines := [][2]string{
		{"  Verify the destination PVC remains healthy.", ""},
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
	args := []string{workflowCommandName(session), "cleanup", session.ID}
	if session.Spec.Type != domain.SessionTypeBackup &&
		session.Spec.Type != domain.SessionTypeRestore &&
		!session.Spec.Operation().RebindsPVC() {
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
	switch session.Spec.Type {
	case domain.SessionTypeBackup:
		if session.Spec.Backup != nil {
			ref := session.Spec.Backup.SourcePVC
			if ref.Namespace != "" && ref.Name != "" {
				refs[ref.Namespace] = append(refs[ref.Namespace], ref.Name)
			}
		}
	case domain.SessionTypeRestore:
		if session.Spec.Restore != nil {
			ref := session.Spec.Restore.DestinationPVC
			if ref.Namespace != "" && ref.Name != "" {
				refs[ref.Namespace] = append(refs[ref.Namespace], ref.Name)
			}
		}
	}

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

func writeSessionRecord(w io.Writer, session *domain.Session) error {
	if session == nil {
		return nil
	}

	if session.Backend == kube.SessionBackendCRD {
		resource, _ := domain.ControllerResourceForSession(session)
		if resource.Cluster {
			_, err := fmt.Fprintf(
				w,
				"  Record: %s %s\n",
				controllerResourceKind(session),
				session.ID,
			)

			return err
		}

		_, err := fmt.Fprintf(
			w,
			"  Record: %s %s/%s\n",
			controllerResourceKind(session),
			session.Spec.SessionNamespace,
			session.ID,
		)

		return err
	}

	_, err := fmt.Fprintf(
		w,
		"  Record: ConfigMap %s/%s\n",
		session.Spec.SessionNamespace,
		kube.SessionConfigMapName(session.ID),
	)

	return err
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

	if session != nil && errorHasOperation(err, domain.ErrorOperationWarmCopyMountProbe) {
		_ = writeWarmCopyMountGuidance(cmd.ErrOrStderr(), cmd, session)
		return err
	}

	if session != nil && errorHasOperation(err, domain.ErrorOperationCopyCapacity) {
		_ = writeCapacityFailureGuidance(
			cmd.ErrOrStderr(),
			session,
			guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace),
		)

		return err
	}

	if session != nil && errorHasOperation(err, domain.ErrorOperationSourceUsageCheck) {
		_, _ = fmt.Fprintln(
			cmd.ErrOrStderr(),
			"\nSource usage could not be read from a trusted storage-backend CRD. Increase --destination-capacity, or independently verify the data size and create a new session with --skip-source-usage-check.",
		)

		return err
	}

	if session != nil && errorHasOperation(err, domain.ErrorOperationTransferPathPreflight) {
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
	workflow := workflowCommandName(session)

	capacitySummary := "\nDestination capacity was exhausted. Keep the workload paused when applicable. Abort and clean up this session, then create a new session with a larger --destination-capacity."
	if kubeblocks, ok := session.Spec.KubeBlocksPodMigration(); ok {
		capacitySummary = fmt.Sprintf(
			"\nDestination capacity was exhausted during KubeBlocks Cluster %s component %s real-time migration. Keep the workload paused when applicable. Update the component volumeClaimTemplates storage request, abort and clean up this session, then create a new migrate-pod session.",
			kubeblocks.Cluster,
			kubeblocks.Component,
		)
	}

	if _, err := fmt.Fprintln(w, capacitySummary); err != nil {
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

	if err := writeSessionRecord(w, session); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(
		w,
		"  Inspect:",
		fmt.Sprintf("%s %s status %s", prefix, workflow, session.ID),
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
			fmt.Sprintf("%s %s rollback %s --dry-run", prefix, workflow, session.ID),
		); err != nil {
			return err
		}

		_, err := fmt.Fprintln(
			w,
			"  Roll back:",
			fmt.Sprintf("%s --yes %s rollback %s --dry-run=false", prefix, workflow, session.ID),
		)

		return err
	}

	abortPlan := fmt.Sprintf("%s %s abort %s --dry-run", prefix, workflow, session.ID)
	abort := fmt.Sprintf("%s --yes %s abort %s --dry-run=false", prefix, workflow, session.ID)
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

	if kubeblocks, ok := session.Spec.KubeBlocksPodMigration(); ok {
		_, err := fmt.Fprintf(
			w,
			"  Update KubeBlocks Cluster %s component %s volumeClaimTemplates storage request, then create a new migrate-pod session after cleanup.\n",
			kubeblocks.Cluster,
			kubeblocks.Component,
		)

		return err
	}

	_, err := fmt.Fprintln(
		w,
		"  Create a new session with a larger --destination-capacity; capacity cannot be changed on this session.",
	)

	return err
}

func sessionHasCapacityFailure(session *domain.Session) bool {
	return session != nil && session.Status.Phase == domain.PhaseFailed &&
		strings.HasPrefix(session.Status.Message, domain.ErrorOperationCopyCapacity+":")
}

func reportSessionLookupError(
	cmd interface{ ErrOrStderr() io.Writer },
	namespace, id string,
	err error,
) error {
	prefixes := guidancePrefixesForCommand(cmd, namespace)
	prefix := prefixes.pvcMigrate
	workflow := workflowCommandNameForCommand(cmd)

	_, _ = fmt.Fprintf(
		cmd.ErrOrStderr(),
		"\nSession lookup failed. List persisted sessions with the workflow status command: %s %s status\n",
		prefix,
		workflow,
	)
	if id != "" {
		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"Inspect the expected record: %s\n",
			sessionRecordInspectionCommand(cmd, namespace, id),
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
	if errorHasOperation(err, domain.ErrorOperationTransferPathPreflight) {
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
	workflow := workflowCommandNameForCommand(cmd)
	_, _ = fmt.Fprintf(
		cmd.ErrOrStderr(),
		"\nSession creation did not return a confirmed result. Inspect before retrying: %s %s status %s\n",
		prefix,
		workflow,
		id,
	)
	_, _ = fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Inspect the expected record directly: %s\n",
		sessionRecordInspectionCommand(cmd, namespace, id),
	)

	return err
}

func sessionRecordInspectionCommand(value any, namespace, id string) string {
	prefix := kubectlCommandPrefixForCommand(value)
	command, ok := value.(*cobra.Command)

	controllerMode := false
	if ok && command != nil {
		mode := command.Root().PersistentFlags().Lookup("mode")

		controllerMode = mode != nil &&
			strings.EqualFold(mode.Value.String(), string(executionModeController))
		for current := command; current != nil; current = current.Parent() {
			if current.Name() == "cross-cluster" {
				controllerMode = false
			}
		}
	}

	if controllerMode {
		name := workflowCommandNameForCommand(value)
		for _, workflow := range domain.ControllerWorkflows() {
			session := &domain.Session{Spec: domain.SessionSpec{Type: workflow.Type}}
			if workflowCommandName(session) != name {
				continue
			}

			resources := make([]string, 0, 2)
			for _, resource := range []string{workflow.Resource, workflow.ClusterResource} {
				if resource != "" {
					resources = append(resources, resource+"."+domain.SessionAPIGroup)
				}
			}

			return fmt.Sprintf(
				"%s --namespace %s get %s %s",
				prefix,
				shellQuote(namespace),
				strings.Join(resources, ","),
				shellQuote(id),
			)
		}
	}

	return fmt.Sprintf(
		"%s --namespace %s get configmap %s",
		prefix,
		shellQuote(namespace),
		shellQuote(kube.SessionConfigMapName(id)),
	)
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

	deleteRecordFailed := errorHasOperation(
		err,
		domain.ErrorOperationDeleteSession,
		domain.ErrorOperationDeleteSessionLock,
	)
	if session != nil && options.DeleteSession && deleteRecordFailed {
		prefixes := guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace)
		recordKind := "configmap"
		recordName := kube.SessionConfigMapName(session.ID)

		if session.Backend == kube.SessionBackendCRD {
			recordKind = controllerResourceForKubectl(session)
			recordName = session.ID
		}

		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"\nCleanup stopped during session record removal. Inspect the session record: %s --namespace %s get %s %s\n",
			prefixes.kubectl,
			session.Spec.SessionNamespace,
			recordKind,
			recordName,
		)
		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"Inspect the Lease: %s --namespace %s get lease %s\n",
			prefixes.kubectl,
			session.Spec.SessionNamespace,
			kube.SessionLockName(kube.SessionLockID(session)),
		)
		_, _ = fmt.Fprintln(
			cmd.ErrOrStderr(),
			"Use the owning workflow's status and cleanup commands when the session record remains; use recovery cleanup-orphan when ownership remains after the record is gone.",
		)

		return err
	}

	if session != nil {
		prefix := guidancePrefixesForCommand(cmd, session.Spec.SessionNamespace).pvcMigrate
		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"\nCleanup stopped before confirmed completion. Inspect current state: %s %s status %s\n",
			prefix,
			workflowCommandName(session),
			session.ID,
		)
		_, _ = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"Revalidate cleanup before retrying: %s %s --dry-run\n",
			prefix,
			cleanupCommandArgsForSessionOptions(session, options),
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
	abortArgs := workflowCommandName(session) + " abort " + session.ID
	cleanupArgs := cleanupCommandArgsForSessionOptions(session, app.CleanupOptions{
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

	retry := "Rerun the original offline migrate command after cleanup completes and all source PVC consumers have stopped."
	switch session.Spec.Operation() {
	case domain.OperationCopy:
		retry = "Rerun the original copy without --online after cleanup completes and the source PVC has no active Pod consumers."
	case domain.OperationMigratePod:
		retry = "Rerun the original migrate-pod command with --precopy-passes 0 after cleanup completes."
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

func cleanupCommandArgsForSessionOptions(
	session *domain.Session,
	options app.CleanupOptions,
) string {
	workflow := "migrate"

	id := ""
	if session != nil {
		workflow = workflowCommandName(session)
		id = session.ID
	}

	return cleanupCommandArgsForWorkflow(workflow, id, options)
}

func cleanupCommandArgsForWorkflow(workflow, id string, options app.CleanupOptions) string {
	args := []string{workflow, "cleanup", id}
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

func sessionCommandPrefix(namespace string) string {
	return sessionCommandPrefixForCommand(nil, namespace)
}

func sessionCommandPrefixForCommand(value any, namespace string) string {
	args := []string{"pvc-migrate"}
	controllerMode := false

	command, ok := value.(*cobra.Command)
	if ok {
		rootFlags := command.Root().PersistentFlags()
		for _, name := range []string{"kubeconfig", "context"} {
			if flag := rootFlags.Lookup(name); flag != nil && flag.Value.String() != "" {
				args = append(args, "--"+name, shellQuote(flag.Value.String()))
			}
		}

		for _, name := range []string{"timeout", "retries", "retry-backoff", "helm-timeout", "stream-tool-logs", "wait", "no-compress"} {
			if flag := rootFlags.Lookup(name); flag != nil && flag.Changed {
				args = append(args, "--"+name+"="+shellQuote(flag.Value.String()))
			}
		}

		if flag := rootFlags.Lookup("mode"); flag != nil && flag.Changed {
			mode := strings.ToLower(strings.TrimSpace(flag.Value.String()))
			args = append(args, "--mode="+shellQuote(mode))
			controllerMode = mode == string(executionModeController)
		}
	}

	if namespace != "" && namespace != "pvc-migrate-system" {
		namespaceFlag := "--session-namespace"
		if controllerMode {
			namespaceFlag = "--workflow-namespace"
		}

		args = append(args, namespaceFlag, shellQuote(namespace))
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
	workflow ...string,
) error {
	if len(sessions) == 0 {
		_, err := fmt.Fprintf(w, "\nNo migration sessions found in %s.\n", namespace)
		return err
	}

	name := "migrate"
	if len(workflow) > 0 && workflow[0] != "" {
		name = workflow[0]
	}

	_, err := fmt.Fprintf(
		w,
		"\nInspect a session and its next steps with the owning workflow status command: %s %s status SESSION\n",
		prefix,
		name,
	)

	return err
}
