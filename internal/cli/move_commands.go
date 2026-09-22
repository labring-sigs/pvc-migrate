package cli

import (
	"errors"
	"fmt"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (r *rootState) newMoveCommand() *cobra.Command {
	command := r.moveSubmissionCommand(false, false)
	command.AddCommand(r.newMoveCreateCommand(), r.newMovePlanCommand())
	r.addMoveLifecycle(command)
	return command
}

// newMoveCreateCommand submits a declarative Move workflow for controller
// reconciliation.
func (r *rootState) newMoveCreateCommand() *cobra.Command {
	return r.moveSubmissionCommand(false, true)
}

func (r *rootState) newMovePlanCommand() *cobra.Command {
	return r.moveSubmissionCommand(true, false)
}

func (r *rootState) moveSubmissionCommand(planOnly, submit bool) *cobra.Command {
	object := &v1alpha1.Move{
		Spec: v1alpha1.MoveSpec{DestinationPVC: &v1alpha1.LocalResourceReference{}},
	}

	var (
		sourceNamespace      string
		destinationNamespace string
	)

	dryRun := planOnly
	wait := true

	command := &cobra.Command{
		Use:   "move",
		Short: "Move an offline PVC identity within or across namespaces",
		Args:  cobra.NoArgs,
	}
	switch {
	case planOnly:
		command.Use, command.Short = "plan", "Inspect PVC move checks without mutations"
	case submit:
		command.Use, command.Short = "create", "Submit a Move workflow for controller reconciliation"
	}

	if submit {
		bindCreateDryRun(command, &dryRun)
		bindCreateWait(command, &wait)
	} else {
		bindDryRun(command, &dryRun)
	}

	f := command.Flags()
	f.StringVar(&object.Name, "id", "", "Workflow ID; generated when omitted")
	f.StringVar(
		&sourceNamespace,
		"source-namespace",
		"default",
		"Source PVC namespace",
	)
	f.StringVar(
		&destinationNamespace,
		"destination-namespace",
		"",
		"Destination namespace for the PVC identity",
	)
	f.StringVar(&object.Spec.SourcePVC.Name, "source-pvc", "", "Existing offline PVC name")
	f.StringVar(
		&object.Spec.DestinationPVC.Name,
		"destination-pvc",
		"",
		"New PVC name in the destination namespace; defaults to the source name",
	)

	command.RunE = func(cmd *cobra.Command, _ []string) error {
		object.Spec.SourceNamespace = v1alpha1.NamespaceName(sourceNamespace)
		object.Spec.DestinationNamespace = v1alpha1.NamespaceName(destinationNamespace)

		if object.Spec.SourcePVC.Name == "" || object.Spec.DestinationNamespace == "" {
			return domain.NewError(
				domain.ErrorValidation,
				"move",
				"--source-pvc and --destination-namespace are required",
			)
		}

		current := object.DeepCopy()

		current.Spec.SessionNamespace = v1alpha1.NamespaceName(r.global.sessionNamespace)
		if current.Spec.DestinationPVC.Name == "" {
			current.Spec.DestinationPVC = nil
		}

		if current.Name == "" {
			id, err := domain.NewSessionID(time.Now())
			if err != nil {
				return err
			}

			current.Name = id
		}

		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		// Declarative submission: the Move CR is handed to the elected
		// controller, which owns planning and execution. The session path
		// below plans and executes in this process.
		if submit {
			if err := requireControllerWorkflow(runtime, domain.SessionTypeMove); err != nil {
				return err
			}

			if err := kube.CheckWorkflowIdentityCollision(
				ctx,
				runtime.clients.Runtime,
				runtime.clients.Kubernetes,
				runtime.controllerKinds,
				current.Name,
				domain.ControllerKindMove,
				[]string{
					string(current.Spec.SourceNamespace),
					string(current.Spec.DestinationNamespace),
					r.global.sessionNamespace,
				},
				true,
			); err != nil {
				return err
			}

			if dryRun {
				return runtime.printer.Print(current)
			}

			if err := r.confirm(ctx, cmd, current.Spec.SourcePVC.Name); err != nil {
				return reportApprovalError(cmd, err)
			}

			runtime.waitForController = wait

			return submitMove(ctx, cmd, runtime, current)
		}

		report, err := runtime.planner.PlanMove(ctx, current, r.global.sessionNamespace)
		if err != nil {
			return reportPlanningError(cmd, err)
		}

		if dryRun || !report.Ready {
			if err := runtime.printer.Print(report); err != nil {
				return err
			}
		}

		if !report.Ready {
			return domain.NewError(
				domain.ErrorPrecondition,
				"plan move",
				"planning checks failed; resolve the reported checks before execution",
			)
		}

		if dryRun {
			_, err := fmt.Fprintln(
				cmd.ErrOrStderr(),
				"\nPlanning completed without cluster mutations. Run move with the same inputs and --dry-run=false to execute.",
			)

			return err
		}

		now := metav1.Now()
		current.Status.WorkflowStatus = v1alpha1.WorkflowStatus{
			Phase:     domain.PhasePlanned,
			StartedAt: now,
			UpdatedAt: now,
		}

		if err := r.confirm(ctx, cmd, current.Spec.SourcePVC.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		store, err := kube.NewConfigMapWorkflowStore(
			runtime.clients.Kubernetes,
			r.global.sessionNamespace,
			func() *v1alpha1.Move { return &v1alpha1.Move{} },
		)
		if err != nil {
			return err
		}

		executor := app.NewMoveExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLocker(runtime),
			r.global.sessionNamespace,
		)

		if err := kube.CheckConfigMapIdentityCollision(
			ctx,
			runtime.clients.Kubernetes,
			current.Name,
			[]string{
				string(current.Spec.SourceNamespace),
				string(current.Spec.DestinationNamespace),
				r.global.sessionNamespace,
			},
		); err != nil {
			return err
		}

		if err := store.Create(ctx, current); err != nil {
			return reportSessionCreationError(cmd, r.global.sessionNamespace, current.Name, err)
		}

		if err := executor.Run(ctx, current); err != nil {
			return reportMoveError(cmd, current, err)
		}

		return runtime.printer.Print(current)
	}

	return command
}

func reportMoveError(cmd *cobra.Command, object *v1alpha1.Move, cause error) error {
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Move %s stopped in phase %s. Inspect move status %s before resume, rollback or cleanup.\n",
		object.Name,
		object.Status.Phase,
		object.Name,
	)

	return errors.Join(cause, err)
}
