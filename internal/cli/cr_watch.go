package cli

import (
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// crWatchTarget describes the workflow CRD one cr watch command follows.
type crWatchTarget struct {
	kind     domain.ControllerKind
	resource string
	cluster  bool
}

func (t crWatchTarget) candidates() map[domain.ControllerKind]crclient.Object {
	return map[domain.ControllerKind]crclient.Object{t.kind: newWorkflowObject(t.kind)}
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

			lastPhase := ""
			printTransition := func(current *unstructured.Unstructured) error {
				status, _, err := unstructured.NestedMap(current.Object, "status", "workflowStatus")
				if err != nil {
					return err
				}

				phase, _ := status["phase"].(string)
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
					status["message"],
				)

				return reportErr
			}

			if err := printTransition(initial); err != nil {
				return err
			}

			if _, err := kube.WaitForWorkflow(
				ctx,
				resource,
				initial,
				func() *unstructured.Unstructured { return &unstructured.Unstructured{} },
				func(current *unstructured.Unstructured) (bool, error) {
					if err := printTransition(current); err != nil {
						return false, err
					}

					status, _, err := unstructured.NestedMap(
						current.Object,
						"status",
						"workflowStatus",
					)
					if err != nil {
						return false, err
					}

					phase, _ := status["phase"].(string)

					return workflowTerminalPhase(v1alpha1.WorkflowPhase(phase)), nil
				},
			); err != nil {
				return err
			}

			// Print the typed object so table, JSON, and YAML output stay
			// consistent with the rest of the cr verbs.
			object, err := lookupControllerObjects(
				ctx,
				runtime,
				namespace,
				args[0],
				target.candidates(),
			)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}

				return err
			}

			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			phase := workflowObjectPhase(object)

			return writeControllerWorkflowNextSteps(
				cmd.ErrOrStderr(),
				cmd,
				guidancePrefixesForCommand(cmd, object.GetNamespace()).pvcMigrate,
				target.resource,
				object.GetNamespace(),
				object.GetName(),
				phase,
			)
		},
	}
}
