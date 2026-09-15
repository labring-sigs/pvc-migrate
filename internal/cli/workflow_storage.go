package cli

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) workflowStorageNamespace(cmd *cobra.Command) string {
	return workflowNamespaceForCommand(r, cmd)
}

func cliWorkflowStore[T crclient.Object](
	runtime *commandRuntime,
	namespace string,
	factory func() T,
) (kube.WorkflowStore[T], error) {
	if runtime.clients == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"workflow storage",
			"Kubernetes clients are required",
		)
	}

	// Top-level commands are session-local: workflows persist as concrete CRD
	// objects inside a ConfigMap, never as API-server CRDs.
	return kube.NewConfigMapWorkflowStore(runtime.clients.Kubernetes, namespace, factory)
}

func cliWorkflowLocker(runtime *commandRuntime) kube.SessionLocker {
	return kube.NewConfigMapWorkflowLocker(runtime.clients.Kubernetes)
}

// cliCRDWorkflowStore persists workflows as API-server CRs for the declarative
// paths that must hand a workflow to the elected controller.
func cliCRDWorkflowStore[T crclient.Object](
	runtime *commandRuntime,
	factory func() T,
) (kube.WorkflowStore[T], error) {
	if runtime.clients == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"workflow storage",
			"Kubernetes clients are required",
		)
	}

	return kube.NewCRDWorkflowStore(runtime.clients.Runtime, factory)
}

// loadWorkflowWithBackend resolves one workflow identity from ConfigMap session
// storage first and the workflow CRDs second, reporting which backend served
// the lookup so callers can bind matching stores and handoff callbacks. CRD
// lookups probe every namespace: flag-driven storage namespaces differ per
// operation (copy names its flag --source-namespace, not --namespace).
func (r *rootState) loadWorkflowWithBackend(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	namespace, id string,
	candidates map[domain.ControllerKind]crclient.Object,
) (crclient.Object, string, error) {
	object, err := kube.LoadConfigMapWorkflow(ctx, runtime.clients.Kubernetes, namespace, id)
	if err == nil {
		return object, backendConfigMap, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, "", err
	}

	probes := []string{namespace}
	if extra := r.crdProbeNamespaces(cmd); len(extra) != 0 {
		probes = append(probes, extra...)
	}

	for _, probeNamespace := range probes {
		if probeNamespace == "" {
			continue
		}

		crdObject, crdErr := lookupControllerObjects(ctx, runtime, probeNamespace, id, candidates)
		if crdErr != nil {
			if apierrors.IsNotFound(crdErr) {
				continue
			}
			return nil, "", crdErr
		}

		return crdObject, backendCRD, nil
	}

	return nil, "", err
}

// crdProbeNamespaces lists the additional tenant namespaces a CRD lookup must
// cover: namespaced workflow CRs live in their source namespace while the
// ConfigMap sessions live in the session namespace.
func (r *rootState) crdProbeNamespaces(cmd *cobra.Command) []string {
	namespaces := make([]string, 0, 3)

	if cmd != nil {
		for _, name := range []string{"namespace", "source-namespace", "workflow-namespace"} {
			flag := cmd.Flags().Lookup(name)
			if flag == nil {
				continue
			}

			value, err := cmd.Flags().GetString(name)
			if err != nil || strings.TrimSpace(value) == "" ||
				value == "default" {
				continue
			}

			namespaces = append(namespaces, strings.TrimSpace(value))
		}
	}

	if r != nil && strings.TrimSpace(r.global.workflowNamespace) != "" {
		namespaces = append(namespaces, strings.TrimSpace(r.global.workflowNamespace))
	}

	return namespaces
}

const (
	backendConfigMap = "configmap"
	backendCRD       = "crd"
)

// lookupControllerObjects is an input boundary. It detects ambiguity before an
// operation receives its concrete object; it never reconstructs execution state.
func lookupControllerObjects(
	ctx context.Context,
	runtime *commandRuntime,
	namespace, name string,
	candidates map[domain.ControllerKind]crclient.Object,
) (crclient.Object, error) {
	var found crclient.Object
	for kind, object := range candidates {
		if len(runtime.controllerKinds) != 0 && !slices.Contains(runtime.controllerKinds, kind) {
			continue
		}

		resource, ok := domain.ControllerResourceForKind(kind)
		if !ok {
			return nil, fmt.Errorf("unknown workflow kind %q", kind)
		}

		key := crclient.ObjectKey{Name: name, Namespace: namespace}
		if resource.Cluster {
			key.Namespace = ""
		}

		if err := runtime.clients.Runtime.Get(ctx, key, object); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}

		if found != nil {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"workflow lookup",
				"the name matches multiple API scopes; resolve the duplicate workflow identities",
			)
		}

		found = object
	}

	if found == nil {
		return nil, apierrors.NewNotFound(
			schema.GroupResource{Group: "migrate.sealos.io", Resource: "workflows"},
			name,
		)
	}

	return found, nil
}
