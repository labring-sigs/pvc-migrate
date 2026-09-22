package cli

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (r *rootState) newReserveCommand() *cobra.Command {
	command := r.reserveSubmissionCommand(false, false)
	command.AddCommand(r.newReserveCreateCommand(), r.newReservePlanCommand())
	r.addReserveLifecycle(command)
	return command
}

// newReserveCreateCommand submits a declarative Reservation workflow for
// controller reconciliation.
func (r *rootState) newReserveCreateCommand() *cobra.Command {
	return r.reserveSubmissionCommand(false, true)
}

func (r *rootState) newReservePlanCommand() *cobra.Command {
	return r.reserveSubmissionCommand(true, false)
}

func (r *rootState) reserveSubmissionCommand(planOnly, submit bool) *cobra.Command {
	flags := &reserveFlags{}
	dryRun := planOnly
	wait := true

	command := &cobra.Command{
		Use:   "reserve",
		Short: "Provision and retain destination PVCs",
		Args:  cobra.NoArgs,
	}
	switch {
	case planOnly:
		command.Use, command.Short = "plan", "Inspect reservation checks without mutations"
	case submit:
		command.Use, command.Short = "create", "Submit a Reservation workflow for controller reconciliation"
	}

	command.RunE = func(cmd *cobra.Command, _ []string) error {
		existing := targetsExistingSession(flags.sessionID, flags.sourcePVCs, flags.podName)
		if err := validateDestinationCapacityFlags(
			domain.OperationReserve,
			existing,
			flags.destinationCapacities,
			flags.allowVolumeShrink,
			flags.skipSourceUsageCheck,
			flags.sourcePaths,
			flags.destinationPaths,
		); err != nil {
			return reportPreSessionError(cmd, err)
		}

		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		if submit && runtime.planner != nil {
			runtime.planner = runtime.planner.ForController()
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		if existing {
			if submit {
				return domain.NewError(
					domain.ErrorValidation,
					"reserve create",
					"an existing session cannot be re-submitted; the controller reconciles submitted workflows automatically",
				)
			}

			return r.reserveExisting(ctx, cmd, runtime, flags.sessionID, dryRun)
		}

		object, err := flags.workflow(r, runtime, submit)
		if err != nil {
			return err
		}

		if submit {
			if err := requireControllerWorkflow(runtime, domain.SessionTypeReserve); err != nil {
				return err
			}

			if !dryRun {
				runtime.waitForController = wait

				return submitReservation(ctx, cmd, runtime, object)
			}
		}

		if object.Spec.SourceNamespace == object.Spec.DestinationNamespace &&
			object.Spec.DestinationNamespace == object.Spec.SessionNamespace {
			local := &v1alpha1.Reservation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      object.Name,
					Namespace: string(object.Spec.SourceNamespace),
				},
				Spec: *object.Spec.ReservationSpec.DeepCopy(),
			}

			return r.createReservation(ctx, cmd, runtime, local, dryRun)
		}

		return r.createClusterReservation(ctx, cmd, runtime, object, dryRun)
	}
	flags.bind(command)

	if !planOnly {
		if submit {
			bindCreateDryRun(command, &dryRun)
			bindCreateWait(command, &wait)
		} else {
			bindDryRun(command, &dryRun)
		}
	}

	return command
}

func (r *rootState) reservationConfig(runtime *commandRuntime) app.ReservationExecutorConfig {
	config := app.ReservationExecutorConfig{
		ToolImageProber: kube.NewToolImageProber(runtime.clients.Kubernetes),
		ProbeTimeout:    r.global.helmTimeout, Logger: runtime.logger, Writer: r.errWriter(),
		StreamToolLogs: r.global.streamToolLogs && false,
		StructuredLogs: r.global.logFormat == string(logFormatJSON),
	}

	return config
}

func (r *rootState) createReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.Reservation,
	dryRun bool,
) error {
	report, err := runtime.planner.PlanReservation(ctx, object, r.global.toolImage)
	if err != nil {
		return reportPlanningError(cmd, err)
	}

	if dryRun {
		if err := printPlanResult(cmd, runtime, report, commonPlanFailureAdvice); err != nil {
			return err
		}
		return requireReady(report)
	}

	if err := requireReadyWithOutput(
		runtime,
		report,
		cmd.ErrOrStderr(),
		commonPlanFailureAdvice,
	); err != nil {
		return err
	}

	if err := r.confirm(ctx, cmd, object.Spec.Volumes[0].SourcePVC.Name); err != nil {
		return reportApprovalError(cmd, err)
	}

	now := metav1.Now()
	object.Status.WorkflowStatus = v1alpha1.WorkflowStatus{
		Phase:     domain.PhasePlanned,
		StartedAt: now,
		UpdatedAt: now,
	}

	store, err := cliWorkflowStore(
		runtime,
		object.Namespace,
		func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
	)
	if err != nil {
		return err
	}

	if err := store.Create(ctx, object); err != nil {
		return reportSessionCreationError(cmd, object.Namespace, object.Name, err)
	}

	executor := app.NewReservationExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLocker(runtime),
		r.reservationConfig(runtime),
	)
	if err := executor.Run(ctx, object); err != nil {
		return reportReservationError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

func (r *rootState) createClusterReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.ClusterReservation,
	dryRun bool,
) error {
	report, err := runtime.planner.PlanReserve(ctx, object, r.global.toolImage)
	if err != nil {
		return reportPlanningError(cmd, err)
	}

	if dryRun {
		if err := printPlanResult(cmd, runtime, report, commonPlanFailureAdvice); err != nil {
			return err
		}
		return requireReady(report)
	}

	if err := requireReadyWithOutput(
		runtime,
		report,
		cmd.ErrOrStderr(),
		commonPlanFailureAdvice,
	); err != nil {
		return err
	}

	if err := r.confirm(ctx, cmd, object.Spec.Volumes[0].SourcePVC.Name); err != nil {
		return reportApprovalError(cmd, err)
	}

	now := metav1.Now()
	object.Status.WorkflowStatus = v1alpha1.WorkflowStatus{
		Phase:     domain.PhasePlanned,
		StartedAt: now,
		UpdatedAt: now,
	}
	namespace := string(object.Spec.SessionNamespace)

	store, err := cliWorkflowStore(
		runtime,
		namespace,
		func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
	)
	if err != nil {
		return err
	}

	if err := store.Create(ctx, object); err != nil {
		return reportSessionCreationError(cmd, namespace, object.Name, err)
	}

	executor := app.NewClusterReservationExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLocker(runtime),
		namespace,
		r.reservationConfig(runtime),
	)
	if err := executor.Run(ctx, object); err != nil {
		return reportReservationError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

func (r *rootState) reserveExisting(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	dryRun bool,
) error {
	object, backend, err := r.loadReservationWithBackend(ctx, cmd, runtime, id)
	if err != nil {
		return err
	}

	switch current := object.(type) {
	case *v1alpha1.Reservation:
		store, err := cliWorkflowStoreForBackend(
			runtime,
			backend,
			r.workflowStorageNamespace(cmd),
			func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
		)
		if err != nil {
			return err
		}

		executor := app.NewReservationExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLockerForBackend(runtime, backend),
			r.reservationConfig(runtime),
		)
		if dryRun {
			if err := executor.Validate(ctx, current); err != nil {
				return reportReservationError(cmd, current.Name, current.Status.Phase, err)
			}
			return runtime.printer.Print(current)
		}

		if err := executor.RequestResume(ctx, current); err != nil {
			return reportReservationError(cmd, current.Name, current.Status.Phase, err)
		}

		if err := executor.Run(ctx, current); err != nil {
			return reportReservationError(cmd, current.Name, current.Status.Phase, err)
		}

		return runtime.printer.Print(current)
	case *v1alpha1.ClusterReservation:
		namespace := string(current.Spec.SessionNamespace)
		if namespace == "" {
			namespace = string(current.Spec.SourceNamespace)
		}

		store, err := cliWorkflowStoreForBackend(
			runtime,
			backend,
			r.workflowStorageNamespace(cmd),
			func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
		)
		if err != nil {
			return err
		}

		executor := app.NewClusterReservationExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLockerForBackend(runtime, backend),
			namespace,
			r.reservationConfig(runtime),
		)
		if dryRun {
			if err := executor.Validate(ctx, current); err != nil {
				return reportReservationError(cmd, current.Name, current.Status.Phase, err)
			}
			return runtime.printer.Print(current)
		}

		if err := executor.RequestResume(ctx, current); err != nil {
			return reportReservationError(cmd, current.Name, current.Status.Phase, err)
		}

		if err := executor.Run(ctx, current); err != nil {
			return reportReservationError(cmd, current.Name, current.Status.Phase, err)
		}

		return runtime.printer.Print(current)
	default:
		return domain.NewError(
			domain.ErrorValidation,
			"reserve",
			"stored workflow is not a reservation",
		)
	}
}
