package kube

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// SessionDataKey is the ConfigMap key that persists one workflow object in
// the ConfigMap-backed workflow store.
const SessionDataKey = "session.json"

// controllerWatchReconnectDelay avoids a hot watch-reconnect loop.
const controllerWatchReconnectDelay = 200 * time.Millisecond

type crdResource struct {
	kind    domain.ControllerKind
	cluster bool
	new     func() crclient.Object
	newList func() crclient.ObjectList
}

// workflowCRDResourceRegistry constructs fresh descriptors for each call.
// Descriptors contain function values and are process-wide policy; returning a
// new slice keeps filters and tests from mutating shared routing state.
func workflowCRDResourceRegistry() []crdResource {
	return []crdResource{
		{
			kind:    domain.ControllerKindMigration,
			new:     func() crclient.Object { return &v1alpha1.Migration{} },
			newList: func() crclient.ObjectList { return &v1alpha1.MigrationList{} },
		},
		{
			kind:    domain.ControllerKindClusterMigration,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.ClusterMigration{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ClusterMigrationList{} },
		},
		{
			kind:    domain.ControllerKindPodMigration,
			new:     func() crclient.Object { return &v1alpha1.PodMigration{} },
			newList: func() crclient.ObjectList { return &v1alpha1.PodMigrationList{} },
		},
		{
			kind:    domain.ControllerKindClusterPodMigration,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.ClusterPodMigration{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ClusterPodMigrationList{} },
		},
		{
			kind:    domain.ControllerKindReservation,
			new:     func() crclient.Object { return &v1alpha1.Reservation{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ReservationList{} },
		},
		{
			kind:    domain.ControllerKindClusterReservation,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.ClusterReservation{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ClusterReservationList{} },
		},
		{
			kind:    domain.ControllerKindCopy,
			new:     func() crclient.Object { return &v1alpha1.Copy{} },
			newList: func() crclient.ObjectList { return &v1alpha1.CopyList{} },
		},
		{
			kind:    domain.ControllerKindClusterCopy,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.ClusterCopy{} },
			newList: func() crclient.ObjectList { return &v1alpha1.ClusterCopyList{} },
		},
		{
			kind:    domain.ControllerKindBackup,
			new:     func() crclient.Object { return &v1alpha1.Backup{} },
			newList: func() crclient.ObjectList { return &v1alpha1.BackupList{} },
		},
		{
			kind:    domain.ControllerKindRestore,
			new:     func() crclient.Object { return &v1alpha1.Restore{} },
			newList: func() crclient.ObjectList { return &v1alpha1.RestoreList{} },
		},
		{
			kind:    domain.ControllerKindRename,
			new:     func() crclient.Object { return &v1alpha1.Rename{} },
			newList: func() crclient.ObjectList { return &v1alpha1.RenameList{} },
		},
		{
			kind:    domain.ControllerKindMove,
			cluster: true,
			new:     func() crclient.Object { return &v1alpha1.Move{} },
			newList: func() crclient.ObjectList { return &v1alpha1.MoveList{} },
		},
	}
}

func workflowCRDResource(kind domain.ControllerKind) (crdResource, bool) {
	for _, resource := range workflowCRDResourceRegistry() {
		if resource.kind == kind {
			return resource, true
		}
	}

	return crdResource{}, false
}

// WorkflowObjectForKind returns a fresh prototype object for one workflow kind.
func WorkflowObjectForKind(kind domain.ControllerKind) crclient.Object {
	resource, ok := workflowCRDResource(kind)
	if !ok {
		return nil
	}

	return resource.new()
}

func resourceKey(resource crdResource, namespace, name string) crclient.ObjectKey {
	if resource.cluster {
		namespace = ""
	}
	return crclient.ObjectKey{Namespace: namespace, Name: name}
}

func workflowKind(object crclient.Object) domain.ControllerKind {
	if object == nil {
		return "Workflow"
	}

	switch object.(type) {
	case *v1alpha1.Migration:
		return domain.ControllerKindMigration
	case *v1alpha1.ClusterMigration:
		return domain.ControllerKindClusterMigration
	case *v1alpha1.PodMigration:
		return domain.ControllerKindPodMigration
	case *v1alpha1.ClusterPodMigration:
		return domain.ControllerKindClusterPodMigration
	case *v1alpha1.Reservation:
		return domain.ControllerKindReservation
	case *v1alpha1.ClusterReservation:
		return domain.ControllerKindClusterReservation
	case *v1alpha1.Copy:
		return domain.ControllerKindCopy
	case *v1alpha1.ClusterCopy:
		return domain.ControllerKindClusterCopy
	case *v1alpha1.Backup:
		return domain.ControllerKindBackup
	case *v1alpha1.Restore:
		return domain.ControllerKindRestore
	case *v1alpha1.Rename:
		return domain.ControllerKindRename
	case *v1alpha1.Move:
		return domain.ControllerKindMove
	default:
		return domain.ControllerKind(object.GetObjectKind().GroupVersionKind().Kind)
	}
}

// WorkflowOwner is a deliberately non-executable summary of the workflow that
// owns a set of resources. It cannot be used to execute or mutate a workflow.
type WorkflowOwner struct {
	ID               string
	SessionNamespace string
	Backend          string
	Resource         domain.ControllerResource
	Phase            v1alpha1.WorkflowPhase
	ResumeFrom       v1alpha1.WorkflowPhase
}

// workflowOwner decodes the read-only ownership summary of one workflow CR.
func workflowOwner(
	object *unstructured.Unstructured,
	id, namespace, backend string,
) *WorkflowOwner {
	resource, _ := domain.ControllerResourceForKind(workflowKind(object))

	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	resumeFrom, _, _ := unstructured.NestedString(object.Object, "status", "resumeFrom")

	// Cluster workflows carry their durable session namespace in the spec.
	if namespace == "" {
		namespace, _, _ = unstructured.NestedString(object.Object, "spec", "sessionNamespace")
	}

	return &WorkflowOwner{
		ID:               id,
		SessionNamespace: namespace,
		Backend:          backend,
		Resource:         resource,
		Phase:            v1alpha1.WorkflowPhase(phase),
		ResumeFrom:       v1alpha1.WorkflowPhase(resumeFrom),
	}
}

func ensureSessionFinalizer(finalizers []string) []string {
	finalizers = append([]string(nil), finalizers...)
	if !containsString(finalizers, SessionFinalizer) {
		finalizers = append(finalizers, SessionFinalizer)
	}

	return finalizers
}

func removeSessionFinalizer(values []string) []string {
	result := values[:0]
	for _, item := range values {
		if item != SessionFinalizer {
			result = append(result, item)
		}
	}

	return result
}

func containsString(values []string, value string) bool {
	return slices.Contains(values, value)
}

func sessionLabels(id string) map[string]string {
	return map[string]string{
		ManagedByLabel: ManagedByValue,
		SessionKey:     id,
	}
}

func controllerWaitError(ctx context.Context, action string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return domain.WrapError(
			domain.ErrorTimeout,
			"wait for controller workflow",
			"operation timed out while attempting to "+action+
				"; verify that the controller is running and that --timeout exceeds the expected workflow duration",
			context.DeadlineExceeded,
		)
	}

	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return fmt.Errorf(
			"wait for controller workflow: waiting canceled; the submitted workflow continues under controller control: %w",
			context.Canceled,
		)
	}

	if err == nil {
		err = context.Canceled
	}

	return domain.WrapError(
		domain.ErrorKubernetes,
		"wait for controller workflow",
		"failed to "+action,
		err,
	)
}

func waitControllerWatchReconnect(ctx context.Context) bool {
	timer := time.NewTimer(controllerWatchReconnectDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
