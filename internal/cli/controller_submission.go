package cli

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
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

		// Adoption exists for a retry after an unconfirmed create. It must not
		// silently replay a different request: a failed workflow resubmitted
		// with new flags would otherwise keep executing the old spec while the
		// operator believes the new flags took effect.
		specsMatch, specErr := workflowSpecsMatch(existing, object)
		if specErr != nil {
			return reportSessionCreationError(cmd, object.GetNamespace(), object.GetName(), specErr)
		}

		if !specsMatch {
			return reportSessionCreationError(
				cmd,
				object.GetNamespace(),
				object.GetName(),
				domain.NewError(
					domain.ErrorConflict,
					"submit workflow",
					fmt.Sprintf(
						"workflow %s/%s already exists with a different spec; delete it or submit under a new --id",
						object.GetNamespace(),
						object.GetName(),
					),
				),
			)
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

// planningFollowBudget bounds how long a --wait command keeps following a
// workflow whose planning retries have not resolved. The controller's retry
// window spans ten minutes and most planning failures need operator action,
// so the terminal follows only the early retries and hands the rest to the
// background with an inspection hint.
const planningFollowBudget = 2 * time.Minute

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
	inspect := fmt.Sprintf(
		"%s %s status %s",
		guidancePrefixesForCommand(cmd, object.GetNamespace()).pvcMigrate,
		crFamilyPathForResource(resourceName),
		workflowHintAddress(cmd, object.GetNamespace(), object.GetName()),
	)

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

	reporter := newControllerWaitReporter(cmd.ErrOrStderr(), string(kind), object.GetName())

	keepAliveCtx, cancelKeepAlive := context.WithCancel(ctx)
	defer cancelKeepAlive()

	go reporter.run(keepAliveCtx)
	defer reporter.stop()

	notedPlanningRetry := false

	var planningFollowStart time.Time

	planningFollowExhausted := false

	final, err := kube.WaitForWorkflow(
		ctx,
		resource,
		object,
		newObject,
		func(current T) (bool, error) {
			status := statusOf(current)
			reporter.observe(status.Phase, status.Message)

			switch status.Phase {
			case successPhase,
				domain.PhaseCompleted,
				domain.PhaseAborted,
				domain.PhaseRolledBack:
				return true, nil
			case domain.PhaseFailed:
				// A planning failure inside the bounded retry window may still
				// clear on its own (workload appearing, transient API error),
				// but the window spans minutes and most planning failures need
				// operator action. Follow the retries for a short budget, then
				// stop subjecting the terminal to the whole window.
				if controller.PlanningRetryActive(&status, time.Now()) {
					if planningFollowStart.IsZero() {
						planningFollowStart = time.Now()
					}

					if !notedPlanningRetry {
						notedPlanningRetry = true

						reporter.notef(
							"%s: planning retries continue in the background; waiting up to %s for an outcome\n",
							reporter.prefix,
							planningFollowBudget,
						)
					}

					if time.Since(planningFollowStart) >= planningFollowBudget {
						planningFollowExhausted = true

						return true, nil
					}

					return false, nil
				}

				return true, nil
			default:
				return false, nil
			}
		},
	)
	if err != nil {
		return err
	}

	if planningFollowExhausted {
		reporter.notef(
			"%s: planning is still retrying in the background; not waiting further — inspect it with `%s`\n",
			reporter.prefix,
			inspect,
		)
	}

	if err := runtime.printer.Print(final); err != nil {
		return err
	}

	status := statusOf(final)
	if status.Phase == domain.PhaseFailed && !planningFollowExhausted {
		category := domain.ErrorCategory(status.ErrorCategory)
		if category == "" {
			category = domain.ErrorPrecondition
		}

		return domain.NewError(category, "controller workflow", status.Message)
	}

	// The controller owns this workflow from here: point inspection and
	// finalization at the cr command group.
	return writeControllerWorkflowNextSteps(
		cmd.ErrOrStderr(),
		cmd,
		guidancePrefixesForCommand(cmd, final.GetNamespace()).pvcMigrate,
		resourceName,
		final.GetNamespace(),
		final.GetName(),
		status.Phase,
	)
}

// workflowSpecsMatch compares the spec of two workflow objects through their
// unstructured form so every operation type shares one adoption rule.
func workflowSpecsMatch(left, right crclient.Object) (bool, error) {
	leftSpec, err := workflowObjectSpec(left)
	if err != nil {
		return false, err
	}

	rightSpec, err := workflowObjectSpec(right)
	if err != nil {
		return false, err
	}

	return apiequality.Semantic.DeepEqual(leftSpec, rightSpec), nil
}

func workflowObjectSpec(object crclient.Object) (map[string]any, error) {
	data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil, err
	}

	spec, _ := data["spec"].(map[string]any)
	if spec == nil {
		spec = map[string]any{}
	}

	return spec, nil
}
