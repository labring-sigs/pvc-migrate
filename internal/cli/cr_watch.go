package cli

import (
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// crWatchTarget describes the workflow CRD one cr watch command follows.
type crWatchTarget struct {
	kind     domain.ControllerKind
	resource string
	cluster  bool
}

// newCRWatchCommand streams workflow phase transitions until the workflow
// reaches a terminal phase, then prints the final object and its follow-ups.
func (r *rootState) newCRWatchCommand(target crWatchTarget) *cobra.Command {
	return &cobra.Command{
		Use:   "watch NAME",
		Short: "Stream workflow phase changes until a terminal phase",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if runtime.clients == nil || runtime.clients.Dynamic == nil {
				return domain.NewError(
					domain.ErrorInternal,
					"watch workflow",
					"dynamic Kubernetes client is required",
				)
			}

			namespace := crNamespaceForCommand(cmd)
			if !target.cluster && namespace == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"watch workflow",
					"-n/--namespace is required to address a namespaced workflow CR",
				)
			}

			resource := runtime.clients.Dynamic.
				Resource(v1alpha1.GroupVersion.WithResource(target.resource)).
				Namespace(namespace)

			initial, err := resource.Get(ctx, args[0], metav1.GetOptions{})
			if err != nil {
				return reportSessionLookupError(cmd, namespace, args[0], err)
			}

			// WaitForWorkflow decodes into typed workflow objects; seed it
			// with the live object converted to this kind's typed form.
			typed := newWorkflowObject(target.kind)
			if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructured(
				initial.Object,
				typed,
			); err != nil {
				return err
			}

			lastPhase := domain.Phase("")

			printTransition := func(current crclient.Object) error {
				phase := workflowObjectPhase(current)
				if phase == "" || phase == lastPhase {
					return nil
				}

				lastPhase = phase

				_, reportErr := fmt.Fprintf(
					cmd.ErrOrStderr(),
					"%s %s: %s %s\n",
					target.kind,
					current.GetName(),
					phase,
					workflowObjectMessage(current),
				)

				return reportErr
			}

			if err := printTransition(typed); err != nil {
				return err
			}

			final, err := kube.WaitForWorkflow(
				ctx,
				resource,
				typed,
				func() crclient.Object { return newWorkflowObject(target.kind) },
				func(current crclient.Object) (bool, error) {
					if err := printTransition(current); err != nil {
						return false, err
					}

					return workflowTerminalPhase(workflowObjectPhase(current)), nil
				},
			)
			if err != nil {
				return err
			}

			if err := runtime.printer.Print(final); err != nil {
				return err
			}

			return writeControllerWorkflowNextSteps(
				cmd.ErrOrStderr(),
				cmd,
				guidancePrefixesForCommand(cmd, final.GetNamespace()).pvcMigrate,
				target.resource,
				final.GetNamespace(),
				final.GetName(),
				workflowObjectPhase(final),
			)
		},
	}
}
