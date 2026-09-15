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

func (r *rootState) newRenameCommand() *cobra.Command {
	command := r.renameSubmissionCommand(false, false)
	command.AddCommand(r.newRenameCreateCommand(), r.newRenamePlanCommand())
	r.addRenameLifecycle(command)
	return command
}

// newRenameCreateCommand submits a declarative Rename workflow for controller
// reconciliation.
func (r *rootState) newRenameCreateCommand() *cobra.Command {
	return r.renameSubmissionCommand(false, true)
}

func (r *rootState) newRenamePlanCommand() *cobra.Command {
	return r.renameSubmissionCommand(true, false)
}

func (r *rootState) renameSubmissionCommand(planOnly, submit bool) *cobra.Command {
	object := &v1alpha1.Rename{}
	dryRun := planOnly
	wait := true

	command := &cobra.Command{
		Use:   "rename",
		Short: "Rebind an offline PVC name within its namespace",
		Args:  cobra.NoArgs,
	}
	switch {
	case planOnly:
		command.Use, command.Short = "plan", "Inspect PVC rename checks without mutations"
	case submit:
		command.Use, command.Short = "create", "Submit a Rename workflow for controller reconciliation"
	}

	if submit {
		bindCreateDryRun(command, &dryRun)
		bindCreateWait(command, &wait)
	} else {
		bindDryRun(command, &dryRun)
	}

	f := command.Flags()
	f.StringVar(&object.Name, "id", "", "Workflow ID; generated when omitted")
	f.StringVarP(
		&object.Namespace,
		"namespace",
		"n",
		"default",
		"Source and destination PVC namespace",
	)
	f.StringVar(&object.Spec.SourcePVC.Name, "source-pvc", "", "Existing offline PVC name")
	f.StringVar(
		&object.Spec.DestinationPVC.Name,
		"destination-pvc",
		"",
		"New PVC name in the source namespace",
	)

	command.RunE = func(cmd *cobra.Command, _ []string) error {
		if object.Spec.SourcePVC.Name == "" {
			return domain.NewError(domain.ErrorValidation, "rename", "--source-pvc is required")
		}

		if object.Spec.DestinationPVC.Name == "" {
			return domain.NewError(
				domain.ErrorValidation,
				"rename",
				"--destination-pvc is required",
			)
		}

		current := object.DeepCopy()
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

		if submit && runtime.planner != nil {
			runtime.planner = runtime.planner.ForController()
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		storageNamespace := current.Namespace

		if submit {
			if err := requireControllerWorkflow(runtime, domain.SessionTypeRename); err != nil {
				return err
			}

			if err := kube.CheckWorkflowIdentityCollision(
				ctx,
				runtime.clients.Runtime,
				runtime.clients.Kubernetes,
				runtime.controllerKinds,
				current.Name,
				domain.ControllerKindRename,
				[]string{current.Namespace},
				true,
			); err != nil {
				return err
			}
		}

		report, err := runtime.planner.PlanRename(ctx, current, storageNamespace)
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
				"plan rename",
				"planning checks failed; resolve the reported checks before execution",
			)
		}

		if dryRun {
			_, err = fmt.Fprintln(
				cmd.ErrOrStderr(),
				"\nPlanning completed without cluster mutations. Run rename with the same inputs and --dry-run=false to execute.",
			)

			return err
		}

		now := metav1.Now()
		current.Status.WorkflowStatus = v1alpha1.WorkflowStatus{
			Phase:     domain.PhasePlanned,
			StartedAt: now,
			UpdatedAt: now,
		}

		if submit {
			if err := r.confirm(ctx, cmd, current.Spec.SourcePVC.Name); err != nil {
				return reportApprovalError(cmd, err)
			}

			runtime.waitForController = wait

			return submitRename(ctx, cmd, runtime, current)
		}

		if err := r.confirm(ctx, cmd, current.Spec.SourcePVC.Name); err != nil {
			return reportApprovalError(cmd, err)
		}

		store, err := renameStore(runtime, storageNamespace)
		if err != nil {
			return err
		}

		if err := kube.CheckConfigMapIdentityCollision(
			ctx,
			runtime.clients.Kubernetes,
			current.Name,
			[]string{current.Namespace},
		); err != nil {
			return err
		}

		executor := app.NewRenameExecutor(
			runtime.clients.Kubernetes,
			store,
			cliWorkflowLocker(runtime),
			storageNamespace,
		)

		if err := store.Create(ctx, current); err != nil {
			return reportSessionCreationError(cmd, current.Namespace, current.Name, err)
		}

		if err := executor.Run(ctx, current); err != nil {
			return reportRenameError(cmd, current, err)
		}

		return runtime.printer.Print(current)
	}

	return command
}

// renameStore persists rename sessions in the session ConfigMap store.
func renameStore(
	runtime *commandRuntime,
	namespace string,
) (kube.WorkflowStore[*v1alpha1.Rename], error) {
	return kube.NewConfigMapWorkflowStore(
		runtime.clients.Kubernetes,
		namespace,
		func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
	)
}

func reportRenameError(cmd *cobra.Command, object *v1alpha1.Rename, cause error) error {
	if _, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Rename %s/%s stopped in phase %s. Inspect rename status %s before resume, rollback or cleanup.\n",
		object.Namespace,
		object.Name,
		object.Status.Phase,
		object.Name,
	); err != nil {
		return errors.Join(cause, err)
	}

	return cause
}
