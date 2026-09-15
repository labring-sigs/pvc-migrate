package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

type guidancePrefixes struct {
	pvcMigrate string
	kubectl    string
}

func guidancePrefixesForCommand(value any, namespace string) guidancePrefixes {
	return guidancePrefixes{
		pvcMigrate: sessionCommandPrefixForCommand(value, namespace),
		kubectl:    kubectlCommandPrefixForCommand(value),
	}
}

// sessionTypeCommandName maps a workflow operation to its CLI command family.
func sessionTypeCommandName(sessionType domain.SessionType) string {
	switch sessionType {
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
// command before any object has been loaded. This keeps early errors
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

func sessionRecordInspectionCommand(value any, namespace, id string) string {
	prefix := kubectlCommandPrefixForCommand(value)
	name := workflowCommandNameForCommand(value)

	for _, workflow := range domain.ControllerWorkflows() {
		if sessionTypeCommandName(workflow.Type) != name {
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

	return fmt.Sprintf(
		"%s --namespace %s get %s %s",
		prefix,
		shellQuote(namespace),
		"migrations.migrate.sealos.io",
		shellQuote(id),
	)
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

	return nil
}
