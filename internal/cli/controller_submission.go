package cli

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// submitControllerObject owns persistence and progress output at the CLI boundary.
func submitControllerObject[T crclient.Object](
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object T,
	newObject func() T,
	kind domain.ControllerKind,
	resourceName string,
	namespaces []string,
	successPhase v1alpha1.WorkflowPhase,
	statusOf func(T) v1alpha1.WorkflowStatus,
) error {
	if runtime.clients == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"submit workflow",
			"Kubernetes clients are required",
		)
	}

	store, err := kube.NewCRDWorkflowStore(runtime.clients.Runtime, newObject)
	if err != nil {
		return err
	}

	if err := kube.CheckWorkflowIdentityCollision(ctx, runtime.clients.Runtime,
		runtime.clients.Kubernetes, runtime.controllerKinds, object.GetName(), kind,
		namespaces, false,
	); err != nil {
		return err
	}

	if err := store.Create(ctx, object); err != nil {
		// A retry after an unconfirmed create can collide with the workflow
		// the previous attempt actually created (cached collision checks lag
		// the API server). Adopt it when the identity matches ours.
		if !apierrors.IsAlreadyExists(err) {
			return reportSessionCreationError(cmd, object.GetNamespace(), object.GetName(), err)
		}

		existing := newObject()
		if err := runtime.clients.Runtime.Get(
			ctx,
			crclient.ObjectKey{Namespace: object.GetNamespace(), Name: object.GetName()},
			existing,
		); err != nil {
			return reportSessionCreationError(cmd, object.GetNamespace(), object.GetName(), err)
		}

		if existing.GetLabels()[kube.ManagedByLabel] != kube.ManagedByValue ||
			existing.GetLabels()[kube.SessionKey] != object.GetName() {
			return reportSessionCreationError(cmd, object.GetNamespace(), object.GetName(), err)
		}

		// Continue with the server-side state; the workflow is already owned.
		if err := kube.CopyWorkflowObject(existing, object); err != nil {
			return reportSessionCreationError(cmd, object.GetNamespace(), object.GetName(), err)
		}
	}

	return waitForControllerObject(
		ctx,
		cmd,
		runtime,
		object,
		newObject,
		kind,
		resourceName,
		successPhase,
		statusOf,
	)
}

// waitForControllerObject is shared by creation and explicit lifecycle requests.
// It owns watch/output mechanics; the operation supplies its concrete status.
func waitForControllerObject[T crclient.Object](
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object T,
	newObject func() T,
	kind domain.ControllerKind,
	resourceName string,
	successPhase v1alpha1.WorkflowPhase,
	statusOf func(T) v1alpha1.WorkflowStatus,
) error {
	inspect := fmt.Sprintf("kubectl get %s %s -o yaml", resourceName, object.GetName())
	if object.GetNamespace() != "" {
		inspect = fmt.Sprintf(
			"kubectl -n %s get %s %s -o yaml",
			object.GetNamespace(),
			resourceName,
			object.GetName(),
		)
	}

	if _, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"%s %s is managed by the controller; inspect it with `%s`\n",
		kind,
		object.GetName(),
		inspect,
	); err != nil {
		return err
	}

	if !runtime.waitForController {
		return runtime.printer.Print(object)
	}

	if runtime.clients.Dynamic == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"wait for workflow",
			"dynamic Kubernetes client is required",
		)
	}

	resource := runtime.clients.Dynamic.Resource(v1alpha1.GroupVersion.WithResource(resourceName)).
		Namespace(object.GetNamespace())
	lastPhase := v1alpha1.WorkflowPhase("")

	final, err := kube.WaitForWorkflow(
		ctx,
		resource,
		object,
		newObject,
		func(current T) (bool, error) {
			status := statusOf(current)
			if status.Phase != lastPhase {
				lastPhase = status.Phase
				if _, err := fmt.Fprintf(
					cmd.ErrOrStderr(),
					"%s %s: %s %s\n",
					kind,
					current.GetName(),
					status.Phase,
					status.Message,
				); err != nil {
					return false, err
				}
			}

			switch status.Phase {
			case successPhase,
				domain.PhaseCompleted,
				domain.PhaseFailed,
				domain.PhaseAborted,
				domain.PhaseRolledBack:
				return true, nil
			default:
				return false, nil
			}
		},
	)
	if err != nil {
		return err
	}

	if err := runtime.printer.Print(final); err != nil {
		return err
	}

	status := statusOf(final)
	if status.Phase == domain.PhaseFailed {
		category := domain.ErrorCategory(status.ErrorCategory)
		if category == "" {
			category = domain.ErrorPrecondition
		}

		return domain.NewError(category, "controller workflow", status.Message)
	}

	return nil
}
