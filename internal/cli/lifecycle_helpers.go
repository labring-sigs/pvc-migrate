package cli

import (
	"fmt"
	"slices"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

// controllerWorkflowAvailable reports whether one operation can be submitted
// to its installed CRD.
func controllerWorkflowAvailable(runtime *commandRuntime, sessionType domain.SessionType) bool {
	if runtime == nil {
		return false
	}

	workflow, ok := domain.ControllerWorkflowForType(sessionType)
	if !ok {
		return false
	}

	// Cluster-only operations (Move) carry an empty namespaced Kind; their
	// served CRD is the cluster-scoped one.
	kind := workflow.Kind
	if kind == "" {
		kind = workflow.ClusterKind
	}

	if len(runtime.controllerKinds) == 0 {
		return !runtime.controllerDiscoveryComplete
	}

	return slices.Contains(runtime.controllerKinds, kind)
}

// workflowNamespaceForCommand resolves the storage namespace session
// lifecycle and status commands use: a command-local --namespace when the
// command defines one, otherwise the configured session namespace.
// writeDeletedWorkflow prints the closing confirmation a cleanup prints when
// it deleted the workflow record; the shared form keeps every family's
// closing output identical instead of exiting silently.
//
// Callers must reach this only after a real deletion: families whose dry-run
// path shares the printing tail guard the call with `&& !dryRun`, while
// families whose dry-run branch returns early (move, rename, pod-migration)
// rely on that early return. Never let a dry-run flow fall through to it.
func writeDeletedWorkflow(cmd *cobra.Command, kind, name string) error {
	_, err := fmt.Fprintf(cmd.OutOrStdout(), "Deleted %s %s.\n", kind, name)
	return err
}

func workflowNamespaceForCommand(r *rootState, cmd *cobra.Command) string {
	if cmd != nil {
		if flag := cmd.Flags().Lookup("namespace"); flag != nil {
			if value, err := cmd.Flags().
				GetString("namespace"); err == nil &&
				strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}

	if r == nil {
		return ""
	}

	return r.global.sessionNamespace
}

// controllerPlanNamespaces returns the durable namespaces a planner should
// evaluate. Controller submission (submit=true) collapses all roles into the
// source namespace when every namespaced input already belongs to one tenant,
// so the reconciler works without cross-tenant RBAC. Session runs keep each
// namespace role as the operator typed it.
func (r *rootState) controllerPlanNamespaces(
	runtime *commandRuntime,
	sessionType domain.SessionType,
	sourceNamespace, destinationNamespace, temporaryNamespace string,
	temporaryNamespaceExplicit bool,
	submit bool,
) (sessionNamespace, resolvedTemporaryNamespace string) {
	sessionNamespace = r.global.sessionNamespace
	resolvedTemporaryNamespace = temporaryNamespace

	if !submit ||
		runtime == nil ||
		!controllerWorkflowAvailable(runtime, sessionType) ||
		sourceNamespace == "" || destinationNamespace != sourceNamespace {
		return sessionNamespace, resolvedTemporaryNamespace
	}

	if temporaryNamespaceExplicit && temporaryNamespace != sourceNamespace {
		return sessionNamespace, resolvedTemporaryNamespace
	}

	if resolvedTemporaryNamespace == "" ||
		(!temporaryNamespaceExplicit && resolvedTemporaryNamespace == "pvc-migrate-system") {
		resolvedTemporaryNamespace = sourceNamespace
	}

	if resolvedTemporaryNamespace == sourceNamespace {
		sessionNamespace = sourceNamespace
	}

	return sessionNamespace, resolvedTemporaryNamespace
}

func requireControllerWorkflow(runtime *commandRuntime, sessionType domain.SessionType) error {
	if runtime == nil || controllerWorkflowAvailable(runtime, sessionType) {
		return nil
	}

	workflow, ok := domain.ControllerWorkflowForType(sessionType)
	if !ok {
		return domain.NewError(
			domain.ErrorValidation,
			"controller mode",
			fmt.Sprintf("unsupported controller workflow type %q", sessionType),
		)
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"controller mode",
		string(workflow.Kind)+" CRD is not served by this cluster",
	)
}

func requiresResumeApproval(phase v1alpha1.WorkflowPhase) bool {
	switch phase {
	case domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing,
		domain.PhaseFinalSynced, domain.PhaseActivating, domain.PhaseActivated,
		domain.PhaseResuming, domain.PhaseRollingBack, domain.PhaseRenaming,
		domain.PhaseMoving, domain.PhaseAborting:
		return true
	default:
		return false
	}
}

func requiresOperationResumeApproval(
	operation domain.Operation,
	phase v1alpha1.WorkflowPhase,
) bool {
	return phase == domain.PhasePlanned && operation.RebindsPVC()
}

func bindIdentityCleanupFlags(command *cobra.Command, options *app.IdentityCleanupOptions) {
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Release storage ownership and restore the active PV's recorded reclaim policy")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the session record after cleanup (ConfigMap or workflow CR)")
}
