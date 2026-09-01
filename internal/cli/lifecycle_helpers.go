package cli

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
)

type controllerSessionWaiter interface {
	Wait(
		ctx context.Context,
		initial *domain.Session,
		onUpdate func(*domain.Session) (bool, error),
	) (*domain.Session, error)
}

// controllerWorkflowAvailable reports whether one operation can be submitted
// to its installed CRD in explicit controller mode.
func controllerWorkflowAvailable(runtime *commandRuntime, sessionType domain.SessionType) bool {
	if runtime == nil || runtime.mode != executionModeController || runtime.controllerStore == nil {
		return false
	}

	workflow, ok := domain.ControllerWorkflowForType(sessionType)
	if !ok {
		return false
	}

	if len(runtime.controllerKinds) == 0 {
		return true
	}

	return slices.Contains(runtime.controllerKinds, workflow.Kind)
}

// workflowNamespaceForCommand resolves the namespace used by lifecycle and
// status commands. Controller-backed workflows are namespaced tenant
// resources, while session mode uses the configured session namespace.
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

	if strings.TrimSpace(r.global.workflowNamespace) != "" {
		return strings.TrimSpace(r.global.workflowNamespace)
	}

	return r.global.sessionNamespace
}

// controllerPlanNamespaces returns the durable namespaces that should be
// passed to a planner before it performs ownership and quota checks. The
// controller contract is selected only when the matching CRD is served and
// all namespaced workflow inputs already belong to one tenant.
func (r *rootState) controllerPlanNamespaces(
	runtime *commandRuntime,
	sessionType domain.SessionType,
	sourceNamespace, destinationNamespace, temporaryNamespace string,
	temporaryNamespaceExplicit bool,
) (sessionNamespace, resolvedTemporaryNamespace string) {
	sessionNamespace = r.global.sessionNamespace
	resolvedTemporaryNamespace = temporaryNamespace

	if runtime == nil || runtime.mode == executionModeSession ||
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

func (r *rootState) controllerPlanSessionNamespace(
	runtime *commandRuntime,
	sessionType domain.SessionType,
	sourceNamespace, destinationNamespace string,
) string {
	sessionNamespace, _ := r.controllerPlanNamespaces(
		runtime,
		sessionType,
		sourceNamespace,
		destinationNamespace,
		sourceNamespace,
		false,
	)

	return sessionNamespace
}

func requireControllerWorkflow(runtime *commandRuntime, sessionType domain.SessionType) error {
	if runtime == nil || runtime.mode != executionModeController ||
		controllerWorkflowAvailable(runtime, sessionType) {
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

// workflowSession loads and type-checks a session for an operation-owned
// lifecycle command. It keeps Kubernetes lookup/error formatting mechanical;
// each operation still owns the command and service method that follows.
func (r *rootState) workflowSession(
	ctx context.Context,
	runtime *commandRuntime,
	cmd *cobra.Command,
	id string,
	expected domain.SessionType,
	action string,
) (*domain.Session, error) {
	namespace := workflowNamespaceForCommand(r, cmd)

	session, err := runtime.store.Get(ctx, namespace, id)
	if err != nil {
		return nil, reportSessionLookupError(cmd, namespace, id, err)
	}

	if err := requireCLISessionType(session, expected, action); err != nil {
		return nil, reportSessionError(cmd, session, err)
	}

	return session, nil
}

// deferControllerExecution transfers execution ownership to the elected
// controller. By default the submitting CLI follows the workflow resource to
// a terminal checkpoint; --wait=false retains detached submission.
func deferControllerExecution(
	ctx context.Context,
	cmd interface {
		OutOrStdout() io.Writer
		ErrOrStderr() io.Writer
	},
	runtime *commandRuntime,
	session *domain.Session,
) (bool, error) {
	if runtime == nil || session == nil || runtime.mode != executionModeController ||
		session.Backend != kube.SessionBackendCRD {
		return false, nil
	}

	if session.Status.Phase == domain.PhaseFailed {
		if session.Status.ResumeFrom == "" {
			return false, nil
		}

		if err := session.Reactivate("controller resume requested", time.Now()); err != nil {
			return true, err
		}

		if err := runtime.store.Update(ctx, session); err != nil {
			return true, err
		}
	}

	// Completed operation-specific checkpoints have no controller work left.
	// Let the command execute its explicit retry or lifecycle action directly;
	// the controller intentionally ignores these resources until their state is
	// changed by such an action.
	if controllerExecutionFinished(session) {
		return false, nil
	}

	resourceName := controllerResourceName(session)
	resource := controllerResourceForKubectl(session)
	controllerResource, _ := domain.ControllerResourceForSession(session)

	var err error
	if controllerResource.Cluster {
		_, err = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"%s %s was submitted to the controller; inspect it with `kubectl get %s %s -o yaml`\n",
			resourceName,
			session.ID,
			resource,
			session.ID,
		)
	} else {
		_, err = fmt.Fprintf(
			cmd.ErrOrStderr(),
			"%s %s was submitted to the controller; inspect it with `kubectl -n %s get %s %s -o yaml`\n",
			resourceName,
			session.ID,
			session.Spec.SessionNamespace,
			resource,
			session.ID,
		)
	}

	if err != nil {
		return true, err
	}

	if !runtime.waitForController {
		return true, printSessionResult(cmd, runtime, session)
	}

	if runtime.controllerWaiter == nil {
		return true, domain.NewError(
			domain.ErrorKubernetes,
			"wait for controller workflow",
			"controller session waiter is not configured",
		)
	}

	reporter := newControllerProgressReporter(cmd.ErrOrStderr(), session)

	final, err := runtime.controllerWaiter.Wait(
		ctx,
		session,
		func(update *domain.Session) (bool, error) {
			if reportErr := reporter.Report(update); reportErr != nil {
				return false, reportErr
			}

			return controllerWaitFinished(update), nil
		},
	)
	if err != nil {
		return true, err
	}

	if err := printSessionResult(cmd, runtime, final); err != nil {
		return true, err
	}

	return true, controllerWaitResultError(final)
}

type controllerProgressReporter struct {
	writer      io.Writer
	resource    string
	id          string
	historySeen int
	last        string
}

func newControllerProgressReporter(
	writer io.Writer,
	initial *domain.Session,
) *controllerProgressReporter {
	return &controllerProgressReporter{
		writer:      writer,
		resource:    controllerResourceName(initial),
		id:          initial.ID,
		historySeen: len(initial.Status.History),
		last:        controllerProgressSignature(initial),
	}
}

func (r *controllerProgressReporter) Report(session *domain.Session) error {
	if r == nil || session == nil {
		return nil
	}

	if len(session.Status.History) < r.historySeen {
		r.historySeen = 0
	}

	wroteHistory := false
	for _, entry := range session.Status.History[r.historySeen:] {
		if err := writeControllerProgress(
			r.writer, r.resource, r.id, entry.Phase, entry.Message,
		); err != nil {
			return err
		}

		wroteHistory = true
	}

	r.historySeen = len(session.Status.History)

	signature := controllerProgressSignature(session)
	if signature != r.last && !wroteHistory {
		if err := writeControllerProgress(
			r.writer,
			r.resource,
			r.id,
			session.Status.Phase,
			controllerProgressMessage(session),
		); err != nil {
			return err
		}
	}

	r.last = signature

	return nil
}

func writeControllerProgress(
	writer io.Writer,
	resource, id string,
	phase domain.Phase,
	message string,
) error {
	if strings.TrimSpace(message) == "" {
		_, err := fmt.Fprintf(writer, "%s %s: %s\n", resource, id, phase)
		return err
	}

	_, err := fmt.Fprintf(writer, "%s %s: %s - %s\n", resource, id, phase, message)

	return err
}

func controllerProgressSignature(session *domain.Session) string {
	if session == nil {
		return ""
	}

	return fmt.Sprintf(
		"%s\x00%s\x00%d\x00%#v\x00%#v",
		session.Status.Phase,
		session.Status.Message,
		session.Status.WarmPassesCompleted,
		session.Status.Conditions,
		session.Status.Volumes,
	)
}

func controllerProgressMessage(session *domain.Session) string {
	if session == nil {
		return ""
	}

	var (
		attempts    int
		bytesCopied int64
	)
	for _, volume := range session.Status.Volumes {
		attempts += volume.Sync.Attempts
		bytesCopied += volume.Sync.BytesCopied
	}

	details := make([]string, 0, 3)
	if session.Status.WarmPassesCompleted > 0 {
		details = append(
			details,
			fmt.Sprintf("warm passes=%d", session.Status.WarmPassesCompleted),
		)
	}

	if attempts > 0 {
		details = append(details, fmt.Sprintf("attempts=%d", attempts))
	}

	if bytesCopied > 0 {
		details = append(details, fmt.Sprintf("bytes=%d", bytesCopied))
	}

	if len(details) == 0 {
		return session.Status.Message
	}

	if session.Status.Message == "" {
		return strings.Join(details, ", ")
	}

	return session.Status.Message + " (" + strings.Join(details, ", ") + ")"
}

func controllerWaitFinished(session *domain.Session) bool {
	return controllerExecutionFinished(session) ||
		(session != nil && session.Status.Phase == domain.PhaseFailed)
}

func controllerWaitResultError(session *domain.Session) error {
	if session == nil || session.Status.Phase != domain.PhaseFailed {
		return nil
	}

	message := strings.TrimSpace(session.Status.Message)
	if message == "" {
		message = "controller workflow failed"
	}

	return domain.NewError(domain.ErrorInternal, "controller execution", message)
}

func controllerExecutionFinished(session *domain.Session) bool {
	if session == nil {
		return false
	}

	switch session.Status.Phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return true
	}

	switch session.Spec.Type {
	case domain.SessionTypeReserve:
		return session.Status.Phase == domain.PhaseReserved
	case domain.SessionTypeCopy:
		return session.Status.Phase == domain.PhaseWarmCopied
	default:
		return false
	}
}

func controllerResourceName(session *domain.Session) string {
	workflow, ok := domain.ControllerResourceForSession(session)
	if !ok {
		return "workflow"
	}

	return workflow.Singular
}

func controllerResourceKind(session *domain.Session) domain.ControllerKind {
	workflow, ok := domain.ControllerResourceForSession(session)
	if !ok {
		return "Workflow"
	}

	return workflow.Kind
}

func controllerResourceForKubectl(session *domain.Session) string {
	workflow, ok := domain.ControllerResourceForSession(session)
	if !ok {
		return "workflows.migrate.sealos.io"
	}

	return workflow.Resource + "." + domain.SessionAPIGroup
}

func (r *rootState) workflowSessionList(
	ctx context.Context,
	runtime *commandRuntime,
	cmd *cobra.Command,
	typeName domain.SessionType,
	name string,
) error {
	namespace := workflowNamespaceForCommand(r, cmd)

	sessions, err := runtime.store.List(ctx, namespace)
	if err != nil {
		return reportSessionLookupError(cmd, namespace, "", err)
	}

	sessions = filterSessionsByType(sessions, typeName)
	if err := runtime.printer.Print(sessions); err != nil {
		return err
	}

	return writeSessionListGuidance(
		cmd.ErrOrStderr(),
		namespace,
		sessions,
		sessionCommandPrefixForCommand(cmd, namespace),
		name,
	)
}

func requireCLISessionType(
	session *domain.Session,
	expected domain.SessionType,
	action string,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, action, "session is nil")
	}

	if session.Spec.Type != expected {
		return domain.NewError(
			domain.ErrorPrecondition,
			action,
			fmt.Sprintf("requires a %s session, got %s", expected, session.Spec.Type),
		)
	}

	return nil
}

func filterSessionsByType(
	sessions []*domain.Session,
	typeName domain.SessionType,
) []*domain.Session {
	filtered := make([]*domain.Session, 0, len(sessions))
	for _, session := range sessions {
		if session != nil && session.Spec.Type == typeName {
			filtered = append(filtered, session)
		}
	}

	return filtered
}

func sessionResumePhase(session *domain.Session) domain.Phase {
	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	return phase
}

func requiresResumeApproval(phase domain.Phase) bool {
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

func requiresOperationResumeApproval(operation domain.Operation, phase domain.Phase) bool {
	return phase == domain.PhasePlanned && operation.RebindsPVC()
}

func bindCleanupFlags(command *cobra.Command, options *app.CleanupOptions) {
	command.Flags().
		BoolVar(&options.DeleteTemporary, "delete-temporary", false, "Delete retained staged PVCs owned by the session")
	command.Flags().
		BoolVar(&options.DeleteRollback, "delete-rollback-pv", false, "Restore each Released rollback PV's recorded reclaim policy, then delete it")
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Restore the active PV's recorded reclaim policy")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the session ConfigMap after cleanup")
}

func bindIdentityCleanupFlags(command *cobra.Command, options *app.CleanupOptions) {
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Restore the active PV's recorded reclaim policy")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the session ConfigMap after cleanup")
}

func printDeletedSession(cmd *cobra.Command, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "delete session", "session is nil")
	}

	record := "the session ConfigMap"
	if session.Backend == kube.SessionBackendCRD {
		record = "the " + controllerResourceName(session) + " resource"
	}

	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"session %s cleanup completed; %s and Lease were deleted, and active workload storage was preserved\n",
		session.ID,
		record,
	)

	return err
}
