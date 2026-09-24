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
	command := r.renameSubmissionCommand(false)
	command.AddCommand(r.newRenamePlanCommand())
	r.addRenameLifecycle(command)
	return command
}

func (r *rootState) newRenamePlanCommand() *cobra.Command {
	return r.renameSubmissionCommand(true)
}

// renameSubmissionCommand runs or plans a session rename: the ConfigMap record
// is created and executed in this process. Declarative Rename CRs belong to
// the cr rename create command.
// buildRenameWorkflow validates the typed rename inputs and returns the
// workflow with a generated id when omitted; the session executor and the cr
// create assemble it identically.
func buildRenameWorkflow(
	object *v1alpha1.Rename,
	commandLabel string,
) (*v1alpha1.Rename, error) {
	if object.Spec.SourcePVC.Name == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			commandLabel,
			"--source-pvc is required",
		)
	}

	if object.Spec.DestinationPVC.Name == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			commandLabel,
			"--destination-pvc is required",
		)
	}

	current := object.DeepCopy()
	if current.Name == "" {
		id, err := domain.NewSessionID(time.Now())
		if err != nil {
			return nil, err
		}

		current.Name = id
	}

	return current, nil
}

func (r *rootState) renameSubmissionCommand(planOnly bool) *cobra.Command {
	object := &v1alpha1.Rename{}
	dryRun := planOnly

	command := &cobra.Command{
		Use:   "rename",
		Short: "Rebind an offline PVC name within its namespace",
		Args:  cobra.NoArgs,
	}
	if planOnly {
		command.Use, command.Short = "plan", "Inspect PVC rename checks without mutations"
	}

	bindDryRun(command, &dryRun)

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
		current, err := buildRenameWorkflow(object, "rename")
		if err != nil {
			return err
		}

		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		// Session records persist in the session storage namespace — the
		// create command's -n is tenant semantics and must not become the
		// storage location. The lifecycle resolves records the same way
		// (renameStorageNamespace on verbs that carry no -n).
		storageNamespace := r.migrationRecordNamespace()

		report, err := runtime.planner.PlanRename(ctx, current, current.Namespace)
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
			// The record is persisted in the session storage namespace; the
			// inspection hint must point there, not at the tenant namespace.
			return reportSessionCreationError(cmd, storageNamespace, current.Name, err)
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
