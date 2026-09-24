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

// newReserveCommand builds the namespaced session reservation family: one
// tenant namespace (-n) addresses every PVC the reservation touches, while
// the session record persists in the session storage namespace.
// Cross-namespace reservations belong to the cluster-reserve family.
func (r *rootState) newReserveCommand() *cobra.Command {
	command := r.reserveSubmissionCommand(false)
	command.AddCommand(r.newReservePlanCommand())
	r.addReserveLifecycle(command)
	return command
}

func (r *rootState) newReservePlanCommand() *cobra.Command {
	return r.reserveSubmissionCommand(true)
}

// newClusterReserveCommand builds the cluster-scoped session reservation
// family: the namespace roles its spec declares, cross-namespace reservations
// included.
func (r *rootState) newClusterReserveCommand() *cobra.Command {
	command := r.clusterReserveSubmissionCommand(false)
	command.AddCommand(r.newClusterReservePlanCommand())
	r.addClusterReserveLifecycle(command)
	return command
}

func (r *rootState) newClusterReservePlanCommand() *cobra.Command {
	return r.clusterReserveSubmissionCommand(true)
}

// reserveSubmissionCommand builds the namespaced session reservation
// entrypoints: the bare run command and its plan preview. Controller
// submissions live under the cr command group; cross-namespace sessions under
// cluster-reserve.
func (r *rootState) reserveSubmissionCommand(planOnly bool) *cobra.Command {
	flags := &reserveFlags{}
	dryRun := planOnly

	var namespace string

	command := &cobra.Command{
		Use:   "reserve",
		Short: "Provision and retain destination PVCs",
		Args:  cobra.NoArgs,
	}
	if planOnly {
		command.Use, command.Short = "plan", "Inspect reservation checks without mutations"
	}

	command.RunE = func(cmd *cobra.Command, _ []string) error {
		// A namespaced reservation derives every namespace role from
		// metadata.namespace; cross-namespace work belongs to the
		// cluster-reserve family.
		flags.setSingleNamespace(namespace)

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

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		if existing {
			// An existing namespaced reservation continues from its persisted
			// checkpoint; re-submission is a cr create concern.
			return r.reserveExisting(
				ctx,
				cmd,
				runtime,
				flags.sessionID,
				dryRun,
				sourceSession,
				namespacedRecords,
			)
		}

		object, err := flags.workflow(r, runtime, false)
		if err != nil {
			return err
		}

		local := &v1alpha1.Reservation{
			ObjectMeta: metav1.ObjectMeta{
				Name:      object.Name,
				Namespace: string(object.Spec.SourceNamespace),
			},
			Spec: *object.Spec.ReservationSpec.DeepCopy(),
		}

		return r.createReservation(ctx, cmd, runtime, local, dryRun)
	}
	flags.bindTransfer(command)
	command.Flags().StringVarP(
		&namespace,
		"namespace",
		"n",
		"default",
		"Tenant namespace of the reservation and every PVC it addresses",
	)

	if !planOnly {
		bindDryRun(command, &dryRun)
	}

	return command
}

// clusterReserveSubmissionCommand builds the cluster-scoped session
// reservation entrypoints: the bare run command and its plan preview. Records
// persist as ClusterReservation sessions addressed by the namespace roles the
// spec declares.
func (r *rootState) clusterReserveSubmissionCommand(planOnly bool) *cobra.Command {
	flags := &reserveFlags{}
	dryRun := planOnly

	command := &cobra.Command{
		Use:   "cluster-reserve",
		Short: "Provision and retain destination PVCs across namespaces",
		Args:  cobra.NoArgs,
	}
	if planOnly {
		command.Use, command.Short = "plan", "Inspect cross-namespace reservation checks without mutations"
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

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		if existing {
			// An existing cluster reservation continues from its persisted
			// checkpoint; re-submission is a cr create concern.
			return r.reserveExisting(
				ctx,
				cmd,
				runtime,
				flags.sessionID,
				dryRun,
				sourceSession,
				clusterRecords,
			)
		}

		object, err := flags.workflow(r, runtime, false)
		if err != nil {
			return err
		}

		return r.createClusterReservation(ctx, cmd, runtime, object, dryRun)
	}
	flags.bind(command)

	if !planOnly {
		bindDryRun(command, &dryRun)
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

	// The session record ConfigMap lives in the session storage namespace,
	// next to every other session family; the Reservation object itself
	// carries the tenant namespace in metadata.namespace.
	namespace := r.migrationRecordNamespace()

	store, err := cliWorkflowStore(
		runtime,
		namespace,
		func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
	)
	if err != nil {
		return err
	}

	if err := store.Create(ctx, object); err != nil {
		return reportSessionCreationError(cmd, namespace, object.Name, err)
	}

	executor := app.NewReservationExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLocker(runtime),
		r.reservationConfig(runtime),
	)
	if err := executor.Run(ctx, object); err != nil {
		return reportReservationError(cmd, "reserve", object.Name, object.Status.Phase, err)
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
		return reportReservationError(cmd, "cluster-reserve", object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

// reserveExisting continues an already-persisted reservation from its
// checkpoint: the reserve command drives namespaced records, the
// cluster-reserve command cluster-scoped ones, and the cr verbs the workflow
// CRs. An existing session id never re-plans; it resumes what was persisted.
func (r *rootState) reserveExisting(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	dryRun bool,
	source workflowSource,
	scope recordScope,
) error {
	object, backend, err := r.loadReservationWithBackend(ctx, cmd, runtime, id, source, scope)
	if err != nil {
		return err
	}

	switch current := object.(type) {
	case *v1alpha1.Reservation:
		// Session records persist in the session storage namespace whatever
		// tenant namespace their object carries; a CRD record ignores the
		// store namespace entirely, so one resolution serves both backends.
		store, err := cliWorkflowStoreForBackend(
			runtime,
			backend,
			r.migrationRecordNamespace(),
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
				return reportReservationError(
					cmd,
					"reserve",
					current.Name,
					current.Status.Phase,
					err,
				)
			}

			if err := runtime.printer.Print(current); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(
						cmd,
						workflowLeaseNamespace(backend, r.migrationRecordNamespace(), current),
					).pvcMigrate,
					"reserve",
					"resume",
					workflowLeaseNamespace(backend, r.migrationRecordNamespace(), current),
					current.Name,
				),
			)
		}

		if err := executor.RequestResume(ctx, current); err != nil {
			return reportReservationError(cmd, "reserve", current.Name, current.Status.Phase, err)
		}

		if err := executor.Run(ctx, current); err != nil {
			return reportReservationError(cmd, "reserve", current.Name, current.Status.Phase, err)
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
				return reportReservationError(
					cmd,
					"cluster-reserve",
					current.Name,
					current.Status.Phase,
					err,
				)
			}

			if err := runtime.printer.Print(current); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"cluster-reserve",
					"resume",
					namespace,
					current.Name,
				),
			)
		}

		if err := executor.RequestResume(ctx, current); err != nil {
			return reportReservationError(
				cmd,
				"cluster-reserve",
				current.Name,
				current.Status.Phase,
				err,
			)
		}

		if err := executor.Run(ctx, current); err != nil {
			return reportReservationError(
				cmd,
				"cluster-reserve",
				current.Name,
				current.Status.Phase,
				err,
			)
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
