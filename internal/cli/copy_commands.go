package cli

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (r *rootState) newCopyCommand() *cobra.Command {
	command := r.copySubmissionCommand(false, false)
	command.AddCommand(r.newCopyCreateCommand(), r.newCopyPlanCommand())
	r.addCopyLifecycle(command)
	return command
}

// newCopyCreateCommand submits a declarative Copy workflow for controller
// reconciliation.
func (r *rootState) newCopyCreateCommand() *cobra.Command {
	return r.copySubmissionCommand(false, true)
}

func (r *rootState) newCopyPlanCommand() *cobra.Command { return r.copySubmissionCommand(true, false) }

func (r *rootState) copySubmissionCommand(planOnly, submit bool) *cobra.Command {
	flags := &copyFlags{}
	dryRun := planOnly
	wait := true

	command := &cobra.Command{
		Use:   "copy",
		Short: "Run a finite copy without workload cutover",
		Args:  cobra.NoArgs,
	}
	switch {
	case planOnly:
		command.Use, command.Short = "plan", "Inspect copy checks without mutations"
	case submit:
		command.Use, command.Short = "create", "Submit a Copy workflow for controller reconciliation"
	}

	command.RunE = func(cmd *cobra.Command, _ []string) error {
		existing := targetsExistingSession(flags.sessionID, flags.sourcePVCs, flags.podName)
		if err := validateDestinationCapacityFlags(
			domain.OperationCopy,
			existing,
			flags.destinationCapacities,
			flags.allowVolumeShrink,
			flags.skipSourceUsageCheck,
			flags.sourcePaths,
			flags.destinationPaths,
		); err != nil {
			return reportPreSessionError(cmd, err)
		}

		if flags.podName != "" && len(flags.sourcePVCs) != 0 {
			return domain.NewError(
				domain.ErrorValidation,
				"copy",
				"--source-pvc cannot be combined with --pod; the Pod PVC set is copied as one unit",
			)
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
			// An existing Reservation is adoptable through create as well:
			// the handoff persists the planned Copy CR and the elected
			// controller executes it. Only a re-submitted Copy is refused.
			return r.copyExisting(ctx, cmd, runtime, flags, dryRun, submit)
		}

		object, err := flags.workflow(r, runtime, submit)
		if err != nil {
			return err
		}

		if submit {
			if err := requireControllerWorkflow(runtime, domain.SessionTypeCopy); err != nil {
				return err
			}

			if !dryRun {
				runtime.waitForController = wait

				return submitCopy(ctx, cmd, runtime, object)
			}
		}

		if object.Spec.SourceNamespace == object.Spec.DestinationNamespace &&
			object.Spec.DestinationNamespace == object.Spec.SessionNamespace {
			local := &v1alpha1.Copy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      object.Name,
					Namespace: string(object.Spec.SourceNamespace),
				},
				Spec: *object.Spec.CopySpec.DeepCopy(),
			}

			return r.createCopy(ctx, cmd, runtime, local, dryRun)
		}

		return r.createClusterCopy(ctx, cmd, runtime, object, dryRun)
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

func (r *rootState) copyConfig(runtime *commandRuntime) app.CopyExecutorConfig {
	return app.CopyExecutorConfig{
		ToolImageProber:     kube.NewToolImageProber(runtime.clients.Kubernetes),
		ProbeTimeout:        r.global.helmTimeout,
		SharedVolumeManager: runtime.openEBSLVMSharedVolumeManager,
		Transfer:            r.volumeCopyConfig(runtime),
	}
}

func (r *rootState) volumeCopyConfig(runtime *commandRuntime) app.VolumeCopyConfig {
	config := app.VolumeCopyConfig{
		KubeconfigPath: r.global.kubeconfig,
		Context:        r.global.kubeContext,
		Retries:        r.global.retries,
		RetryBackoff:   r.global.retryBackoff,
		HelmTimeout:    r.global.helmTimeout,
		CopyTimeout:    r.global.copyTimeout,
		NoCompress:     r.global.noCompress,
		Writer:         r.errWriter(),
		Logger:         runtime.logger,
		StreamToolLogs: r.global.streamToolLogs,
		StructuredLogs: r.global.logFormat == string(logFormatJSON),
	}

	return config
}

// copyApprovalTarget picks the value the operator must retype to approve the
// protected action. A Pod-selected copy leaves Spec.Volumes empty — the
// controller derives the volume set from the Pod — so the Pod name carries
// the approval instead.
func copyApprovalTarget(spec v1alpha1.CopySpec, workflowName string) string {
	if len(spec.Volumes) > 0 {
		return spec.Volumes[0].SourcePVC.Name
	}

	if spec.Pod != nil {
		return spec.Pod.Name
	}

	return workflowName
}

func (r *rootState) createCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.Copy,
	dryRun bool,
) error {
	report, err := runtime.planner.PlanNamespacedCopy(ctx, object, r.global.toolImage)
	if err != nil {
		return reportPlanningError(cmd, err)
	}

	if dryRun {
		if err := printPlanResult(cmd, runtime, report, copyPlanFailureAdvice); err != nil {
			return err
		}
		return requireReady(report)
	}

	if err := requireReadyWithOutput(
		runtime,
		report,
		cmd.ErrOrStderr(),
		copyPlanFailureAdvice,
	); err != nil {
		return err
	}

	if err := r.confirm(ctx, cmd, copyApprovalTarget(object.Spec, object.Name)); err != nil {
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
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		return err
	}

	if err := store.Create(ctx, object); err != nil {
		return reportSessionCreationError(cmd, object.Namespace, object.Name, err)
	}

	executor := app.NewCopyExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLocker(runtime),
		copyengine.NewPVMigrate(),
		r.copyConfig(runtime),
	)
	if err := executor.Run(ctx, object); err != nil {
		return reportCopyError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

func (r *rootState) createClusterCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.ClusterCopy,
	dryRun bool,
) error {
	report, err := runtime.planner.PlanCopy(ctx, object, r.global.toolImage)
	if err != nil {
		return reportPlanningError(cmd, err)
	}

	if dryRun {
		if err := printPlanResult(cmd, runtime, report, copyPlanFailureAdvice); err != nil {
			return err
		}
		return requireReady(report)
	}

	if err := requireReadyWithOutput(
		runtime,
		report,
		cmd.ErrOrStderr(),
		copyPlanFailureAdvice,
	); err != nil {
		return err
	}

	if err := r.confirm(
		ctx,
		cmd,
		copyApprovalTarget(object.Spec.CopySpec, object.Name),
	); err != nil {
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
		func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
	)
	if err != nil {
		return err
	}

	if err := store.Create(ctx, object); err != nil {
		return reportSessionCreationError(cmd, namespace, object.Name, err)
	}

	executor := app.NewClusterCopyExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLocker(runtime),
		namespace,
		copyengine.NewPVMigrate(),
		r.copyConfig(runtime),
	)
	if err := executor.Run(ctx, object); err != nil {
		return reportCopyError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}
