package cli

import (
	"context"
	"fmt"
	"slices"

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

// loadWorkflowWithBackend resolves one workflow identity from the single
// backend its command family addresses: ConfigMap session storage for the
// session commands, the workflow CRDs for the cr commands. The modes never
// probe each other's storage.
func (r *rootState) loadWorkflowWithBackend(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	namespace, id string,
	candidates map[domain.ControllerKind]crclient.Object,
	source workflowSource,
) (crclient.Object, string, error) {
	if source != sourceSession {
		if !crdListable(runtime) {
			return nil, "", apierrors.NewNotFound(
				schema.GroupResource{Group: "migrate.sealos.io", Resource: "workflows"},
				id,
			)
		}

		namespace := crNamespaceForCommand(cmd)
		if source == sourceController && namespace == "" {
			return nil, "", domain.NewError(
				domain.ErrorValidation,
				"workflow lookup",
				"-n/--namespace is required to address a namespaced workflow CR",
			)
		}

		crdObject, err := lookupControllerObjects(ctx, runtime, namespace, id, candidates)
		if err != nil {
			return nil, "", err
		}

		return crdObject, backendCRD, nil
	}

	object, err := kube.LoadConfigMapWorkflow(ctx, runtime.clients.Kubernetes, namespace, id)
	if err != nil {
		return nil, "", err
	}

	return object, backendConfigMap, nil
}

// Workflow storage backends, mirrored from kube for load-reporting call sites.
const (
	backendConfigMap = kube.BackendConfigMap
	backendCRD       = kube.BackendCRD
)

// newWorkflowObject returns an empty object for one workflow kind.
func newWorkflowObject(kind domain.ControllerKind) crclient.Object {
	switch kind {
	case domain.ControllerKindMigration:
		return &v1alpha1.Migration{}
	case domain.ControllerKindClusterMigration:
		return &v1alpha1.ClusterMigration{}
	case domain.ControllerKindPodMigration:
		return &v1alpha1.PodMigration{}
	case domain.ControllerKindCopy:
		return &v1alpha1.Copy{}
	case domain.ControllerKindClusterCopy:
		return &v1alpha1.ClusterCopy{}
	case domain.ControllerKindReservation:
		return &v1alpha1.Reservation{}
	case domain.ControllerKindClusterReservation:
		return &v1alpha1.ClusterReservation{}
	case domain.ControllerKindRename:
		return &v1alpha1.Rename{}
	case domain.ControllerKindMove:
		return &v1alpha1.Move{}
	case domain.ControllerKindBackup:
		return &v1alpha1.Backup{}
	case domain.ControllerKindRestore:
		return &v1alpha1.Restore{}
	default:
		return nil
	}
}

// listControllerWorkflows lists every workflow CR of one kind across all
// namespaces (cluster-scoped kinds list cluster-wide) for the cr status
// listings. An unserved kind yields an empty result, not an error.
func listControllerWorkflows(
	ctx context.Context,
	runtime *commandRuntime,
	kind domain.ControllerKind,
) ([]crclient.Object, error) {
	switch kind {
	case domain.ControllerKindMigration:
		return listCRDWorkflows(
			ctx,
			runtime,
			func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
		)
	case domain.ControllerKindClusterMigration:
		return listCRDWorkflows(ctx, runtime,
			func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} })
	case domain.ControllerKindPodMigration:
		return listCRDWorkflows(
			ctx,
			runtime,
			func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
		)
	case domain.ControllerKindCopy:
		return listCRDWorkflows(ctx, runtime, func() *v1alpha1.Copy { return &v1alpha1.Copy{} })
	case domain.ControllerKindClusterCopy:
		return listCRDWorkflows(
			ctx,
			runtime,
			func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
		)
	case domain.ControllerKindReservation:
		return listCRDWorkflows(
			ctx,
			runtime,
			func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
		)
	case domain.ControllerKindClusterReservation:
		return listCRDWorkflows(ctx, runtime,
			func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} })
	case domain.ControllerKindRename:
		return listCRDWorkflows(ctx, runtime, func() *v1alpha1.Rename { return &v1alpha1.Rename{} })
	case domain.ControllerKindMove:
		return listCRDWorkflows(ctx, runtime, func() *v1alpha1.Move { return &v1alpha1.Move{} })
	case domain.ControllerKindBackup:
		return listCRDWorkflows(ctx, runtime, func() *v1alpha1.Backup { return &v1alpha1.Backup{} })
	case domain.ControllerKindRestore:
		return listCRDWorkflows(
			ctx,
			runtime,
			func() *v1alpha1.Restore { return &v1alpha1.Restore{} },
		)
	default:
		return nil, fmt.Errorf("unknown workflow kind %q", kind)
	}
}

func listCRDWorkflows[T crclient.Object](
	ctx context.Context,
	runtime *commandRuntime,
	factory func() T,
) ([]crclient.Object, error) {
	store, err := cliCRDWorkflowStore(runtime, factory)
	if err != nil {
		return nil, err
	}

	items, err := store.List(ctx, "")
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	objects := make([]crclient.Object, len(items))
	for i, item := range items {
		objects[i] = item
	}

	return objects, nil
}

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

		// Namespaced kinds are only addressable through an explicit -n; an
		// empty namespace restricts the lookup to cluster-scoped kinds.
		if namespace == "" && !resource.Cluster {
			continue
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
