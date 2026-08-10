package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

// writeSessionGuidance keeps operational instructions on stderr so JSON and
// YAML stdout remain one parseable document. Every destructive command is
// shown with its dry-run form first and explicit approval on execution.
func writeSessionGuidance(w io.Writer, session *domain.Session) error {
	if session == nil {
		return nil
	}
	prefix := sessionCommandPrefix(session.Spec.SessionNamespace)
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

	if _, err := fmt.Fprintf(w, "\nNext steps for session %s (phase %s):\n", session.ID, session.Status.Phase); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Record: ConfigMap %s/%s\n", session.Spec.SessionNamespace, kube.SessionConfigMapName(session.ID)); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "  Inspect:", status); err != nil {
		return err
	}
	if err := writeSessionInspection(w, session); err != nil {
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
		if session.Status.Phase == domain.PhasePaused || session.Status.Phase == domain.PhaseFinalSyncing || session.Status.Phase == domain.PhaseFinalSynced {
			if _, err := fmt.Fprintln(w, "  Abort before activation (type the session ID or use --yes):", fmt.Sprintf("%s session abort %s --dry-run=false", prefix, session.ID)); err != nil {
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

func cleanupCommandArgs(session *domain.Session) string {
	args := []string{"session", "cleanup", session.ID}
	if !session.Spec.Operation().RebindsPVC() {
		args = append(args, "--delete-temporary", "--delete-rollback-pv")
	}
	args = append(args, "--finalize", "--delete-session")
	return strings.Join(args, " ")
}

func writeSessionInspection(w io.Writer, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Pod.Name != "" && workload.Pod.Namespace != "" {
		if _, err := fmt.Fprintf(w, "  Verify workload readiness: kubectl --namespace %s get pod %s -o wide\n", workload.Pod.Namespace, workload.Pod.Name); err != nil {
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
		if _, err := fmt.Fprintf(w, "  Verify active PVCs: kubectl --namespace %s get pvc %s\n", namespace, strings.Join(names, " ")); err != nil {
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
	return writeSessionGuidance(cmd.ErrOrStderr(), session)
}

func reportSessionError(cmd interface{ ErrOrStderr() io.Writer }, session *domain.Session, err error) error {
	if session != nil {
		_ = writeSessionGuidance(cmd.ErrOrStderr(), session)
	}
	return err
}

func reportSessionLookupError(cmd interface{ ErrOrStderr() io.Writer }, namespace, id string, err error) error {
	prefix := sessionCommandPrefix(namespace)
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\nSession lookup failed. List persisted sessions: %s session status\n", prefix)
	if id != "" {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Inspect the expected record: kubectl --namespace %s get configmap %s\n", namespace, kube.SessionConfigMapName(id))
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

func reportTransferError(cmd interface{ ErrOrStderr() io.Writer }, operation, namespace, pvc string, err error) error {
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\n%s stopped before confirmed completion. Inspect the PVC state: kubectl --namespace %s get pvc %s\n", operation, namespace, pvc)
	_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "Fix the reported condition, rerun its plan, then execute with --dry-run=false.")
	return err
}

func writeTransferDryRunGuidance(w io.Writer, operation, namespace, pvc string) error {
	_, err := fmt.Fprintf(w, "\n%s dry-run completed without cluster mutations. Inspect the PVC with kubectl --namespace %s get pvc %s, then run the write command with --dry-run=false.\n", operation, namespace, pvc)
	return err
}

func reportSessionCreationError(cmd interface{ ErrOrStderr() io.Writer }, namespace, id string, err error) error {
	prefix := sessionCommandPrefix(namespace)
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\nSession creation did not return a confirmed result. Inspect before retrying: %s session status %s\n", prefix, id)
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Inspect the expected record directly: kubectl --namespace %s get configmap %s\n", namespace, kube.SessionConfigMapName(id))
	return err
}

func reportCleanupError(cmd interface{ ErrOrStderr() io.Writer }, session *domain.Session, options app.CleanupOptions, err error) error {
	if session != nil && options.DeleteSession {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\nCleanup stopped during session record removal. Inspect the ConfigMap: kubectl --namespace %s get configmap %s\n", session.Spec.SessionNamespace, kube.SessionConfigMapName(session.ID))
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Inspect the Lease: kubectl --namespace %s get lease %s\n", session.Spec.SessionNamespace, kube.SessionLockName(session.ID))
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "Use session status when the ConfigMap remains; use session cleanup-orphan when ownership remains after the ConfigMap is gone.")
		return err
	}
	return reportSessionError(cmd, session, err)
}

func printPlanResult(cmd interface{ ErrOrStderr() io.Writer }, runtime *commandRuntime, plan *domain.MigrationPlan) error {
	if err := runtime.printer.Print(plan); err != nil {
		return reportPlanningError(cmd, err)
	}
	message := "\nPlanning completed without cluster mutations. Resolve the failed checks, then rerun the command."
	if plan.Ready {
		message = "\nDry-run completed without cluster mutations. Run the write command with the same inputs and --dry-run=false; provide --yes or typed approval when requested."
	}
	_, err := fmt.Fprintln(cmd.ErrOrStderr(), message)
	return err
}

func sessionCommandPrefix(namespace string) string {
	prefix := "pvc-migrate"
	if namespace != "" && namespace != "pvc-migrate-system" {
		prefix += " --session-namespace " + namespace
	}
	return prefix
}

func writeSessionListGuidance(w io.Writer, namespace string, sessions []*domain.Session) error {
	prefix := sessionCommandPrefix(namespace)
	if len(sessions) == 0 {
		_, err := fmt.Fprintf(w, "\nNo migration sessions found in %s.\n", namespace)
		return err
	}
	_, err := fmt.Fprintf(w, "\nInspect a session and its next steps: %s session status SESSION\n", prefix)
	return err
}
