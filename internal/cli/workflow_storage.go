package cli

import (
	"context"
	"fmt"
	"slices"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// saveCLIPlannedWorkflow freezes the same execution intent that the controller
// records before it starts a planned CRD. Lifecycle commands can drive a CRD
// directly during recovery, so they must leave the same tamper-evident status.
func saveCLIPlannedWorkflow[T crclient.Object](
	ctx context.Context,
	store kube.WorkflowStore[T],
	object T,
	status *v1alpha1.WorkflowStatus,
) error {
	if status == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"plan workflow",
			"workflow status is required",
		)
	}

	hash, err := kube.WorkflowExecutionIntentHash(object)
	if err != nil {
		return err
	}
	status.ExecutionIntentHash = hash

	return store.Save(ctx, object)
}

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

// cliWorkflowLockerForBackend keeps the fencing protocol paired with the
// persistence backend that owns the workflow. CRD workflows must re-check
// ConfigMap identity collisions while acquiring their Lease; session
// workflows must be allowed to lock the ConfigMap-backed record itself.
func cliWorkflowLockerForBackend(
	runtime *commandRuntime,
	backend string,
) kube.SessionLocker {
	if backend == backendCRD {
		return kube.NewCRDWorkflowLocker(runtime.clients.Kubernetes)
	}

	return cliWorkflowLocker(runtime)
}

// workflowLeaseNamespace resolves the namespace in which a workflow's Lease
// belongs. ConfigMap sessions use their configured storage namespace; a
// namespaced CRD uses the namespace carried by the API object.
func workflowLeaseNamespace(
	backend string,
	fallback string,
	object crclient.Object,
) string {
	if backend == backendCRD && object != nil && object.GetNamespace() != "" {
		return object.GetNamespace()
	}

	return fallback
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

// cliWorkflowStoreForBackend binds the store family that owns the workflow
// identity the loader resolved. Lifecycle mutations fence through their owning
// store, so a CRD-submitted workflow must never be driven through the
// ConfigMap session storage or the other way around. The backend decision
// itself lives in kube.NewWorkflowStoreForBackend so every entrypoint shares
// one definition.
func cliWorkflowStoreForBackend[T crclient.Object](
	runtime *commandRuntime,
	backend, namespace string,
	factory func() T,
) (kube.WorkflowStore[T], error) {
	if runtime == nil || runtime.clients == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"workflow storage",
			"Kubernetes clients are required",
		)
	}

	return kube.NewWorkflowStoreForBackend(runtime.clients, backend, namespace, factory)
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

	// A session-only runtime may intentionally omit the controller-runtime
	// client. Once ConfigMap storage misses, there is no CRD backend to probe;
	// return the original not-found instead of dereferencing a nil client.
	if !crdListable(runtime) {
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
			if err != nil || strings.TrimSpace(value) == "" {
				continue
			}

			value = strings.TrimSpace(value)
			if !slices.Contains(namespaces, value) {
				namespaces = append(namespaces, value)
			}
		}
	}

	if r != nil && strings.TrimSpace(r.global.workflowNamespace) != "" {
		value := strings.TrimSpace(r.global.workflowNamespace)
		if !slices.Contains(namespaces, value) {
			namespaces = append(namespaces, value)
		}
	}

	return namespaces
}

// Workflow storage backends, mirrored from kube for load-reporting call sites.
const (
	backendConfigMap = kube.BackendConfigMap
	backendCRD       = kube.BackendCRD
)

// crdListable reports whether the runtime can enumerate workflow CRs. A
// session-only runtime (no client or no discovered workflow CRD) still lists
// ConfigMap sessions; it simply has no CRD records to add.
func crdListable(runtime *commandRuntime) bool {
	return runtime != nil && runtime.clients != nil && runtime.clients.Runtime != nil &&
		(!runtime.controllerDiscoveryComplete || len(runtime.controllerKinds) != 0)
}

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
