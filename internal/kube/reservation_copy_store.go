package kube

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// HandoffConfigMapReservationToCopy replaces the stored CRD in one conditional
// ConfigMap update. Storage UID is preserved; source and target never coexist.
// The caller owns the workflow Lease and validates live resources before calling.
func HandoffConfigMapReservationToCopy(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	reservation *v1alpha1.ClusterReservation,
	destination *v1alpha1.ClusterCopy,
) error {
	if reservation == nil || destination == nil || reservation.Name != destination.Name ||
		reservation.Namespace != "" || destination.Namespace != "" ||
		destination.UID != "" || destination.ResourceVersion != "" {
		return workflowStoreConflict(
			"handoff",
			"a persisted reservation and a new copy with the same name are required",
		)
	}

	if err := requireWorkflowStorageVersion(reservation); err != nil {
		return err
	}

	source, err := NewConfigMapWorkflowStore(
		client,
		namespace,
		func() *v1alpha1.ClusterReservation {
			return &v1alpha1.ClusterReservation{}
		},
	)
	if err != nil {
		return err
	}

	target, err := NewConfigMapWorkflowStore(client, namespace, func() *v1alpha1.ClusterCopy {
		return &v1alpha1.ClusterCopy{}
	})
	if err != nil {
		return err
	}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	current, err := client.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, SessionConfigMapName(reservation.Name), metav1.GetOptions{})
	if err != nil {
		return err
	}

	if err := checkWorkflowStorageVersion(reservation, current); err != nil {
		return err
	}

	previous, err := source.decode(current, crclient.ObjectKeyFromObject(reservation))
	if err != nil {
		return err
	}

	if !apiequality.Semantic.DeepEqual(previous.Spec, reservation.Spec) ||
		!apiequality.Semantic.DeepEqual(previous.Status, reservation.Status) ||
		current.DeletionTimestamp != nil {
		return workflowStoreConflict("handoff", "reservation changed before copy handoff")
	}

	snapshot := destination.DeepCopy()

	snapshot.Status.ExecutionIntentHash, err = WorkflowExecutionIntentHash(snapshot)
	if err != nil {
		return err
	}

	copyWorkflowStorageVersion(snapshot, current)

	data, err := target.encode(snapshot)
	if err != nil {
		return err
	}

	updated := current.DeepCopy()
	updated.Data = map[string]string{SessionDataKey: string(data)}
	// The routing label must follow the payload's kind: the bare status
	// lists select records by workflow-kind, so a graduated record that
	// keeps the reservation label disappears from every list.
	if updated.Labels == nil {
		updated.Labels = map[string]string{}
	}

	updated.Labels[WorkflowKindLabel] = target.gvk.Kind

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	updated, err = client.CoreV1().
		ConfigMaps(namespace).
		Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	snapshot.GetObjectKind().SetGroupVersionKind(target.gvk)
	copyWorkflowStorageVersion(snapshot, updated)
	*destination = *snapshot

	return nil
}
