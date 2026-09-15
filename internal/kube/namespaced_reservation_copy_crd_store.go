package kube

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// NamespacedHandoffCRDReservationToCopy persists a recoverable handoff under the caller's
// Lease. Both resources remain protected until Copy owns the durable checkpoint.
func NamespacedHandoffCRDReservationToCopy(
	ctx context.Context,
	client crclient.Client,
	reservation *v1alpha1.Reservation,
	destination *v1alpha1.Copy,
) error {
	if reservation == nil || destination == nil || reservation.Name != destination.Name ||
		reservation.Namespace == "" || destination.Namespace != reservation.Namespace ||
		destination.UID != "" || destination.ResourceVersion != "" || destination.Status.Plan == nil {
		return workflowStoreConflict(
			"handoff",
			"a persisted reservation and a planned new copy with the same name are required",
		)
	}

	if err := requireWorkflowStorageVersion(reservation); err != nil {
		return err
	}

	source := &v1alpha1.Reservation{}
	if err := client.Get(ctx, crclient.ObjectKeyFromObject(reservation), source); err != nil {
		return err
	}

	if err := checkWorkflowStorageVersion(reservation, source); err != nil {
		return err
	}

	if source.DeletionTimestamp != nil ||
		!apiequality.Semantic.DeepEqual(source.Spec, reservation.Spec) ||
		!apiequality.Semantic.DeepEqual(source.Status, reservation.Status) {
		return workflowStoreConflict("handoff", "reservation changed before copy handoff")
	}

	token, err := namespacedReservationCopyToken(source)
	if err != nil {
		return err
	}

	target, err := namespacedPrepareCRDCopyHandoff(ctx, client, source, destination, token)
	if err != nil {
		return err
	}

	copyWorkflowStorageVersion(reservation, source)
	reservation.Annotations = maps.Clone(source.Annotations)

	if err := namespacedInitializeCRDCopyHandoff(
		ctx,
		client,
		target,
		destination.Status,
	); err != nil {
		return err
	}

	if err := handoffFenceError(ctx); err != nil {
		return err
	}

	withoutProtection := source.DeepCopy()

	withoutProtection.Finalizers = removeSessionFinalizer(withoutProtection.Finalizers)
	if err := client.Update(ctx, withoutProtection); err != nil {
		return err
	}

	copyWorkflowStorageVersion(reservation, withoutProtection)
	reservation.Finalizers = append([]string(nil), withoutProtection.Finalizers...)

	if err := handoffFenceError(ctx); err != nil {
		return err
	}

	uid, version := withoutProtection.UID, withoutProtection.ResourceVersion
	if err := client.Delete(ctx, withoutProtection, &crclient.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version},
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if err := NamespacedActivateCRDCopyHandoff(ctx, client, target); err != nil {
		return err
	}

	*destination = *target.DeepCopy()

	return nil
}

func namespacedReservationCopyToken(source *v1alpha1.Reservation) (string, error) {
	data, err := json.Marshal(struct {
		Spec   v1alpha1.ReservationSpec   `json:"spec"`
		Status v1alpha1.ReservationStatus `json:"status"`
	}{source.Spec, source.Status})
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/%x", source.UID, sha256.Sum256(data)), nil
}

func namespacedPrepareCRDCopyHandoff(
	ctx context.Context,
	client crclient.Client,
	source *v1alpha1.Reservation,
	desired *v1alpha1.Copy,
	token string,
) (*v1alpha1.Copy, error) {
	target := &v1alpha1.Copy{}

	err := client.Get(ctx, crclient.ObjectKeyFromObject(desired), target)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}

	pending := source.Annotations[reservationCopyPendingAnnotation]
	if pending != "" && pending != token {
		return nil, workflowStoreConflict(
			"handoff",
			"reservation content changed during a pending handoff",
		)
	}

	if err == nil {
		if pending != token || target.Annotations[reservationCopyPendingAnnotation] != token ||
			target.Annotations[reservationCopyOriginAnnotation] != string(source.UID) ||
			!apiequality.Semantic.DeepEqual(
				target.Spec,
				desired.Spec,
			) || target.DeletionTimestamp != nil {
			return nil, workflowStoreConflict(
				"handoff",
				"existing copy does not belong to this reservation handoff",
			)
		}

		return target, nil
	}

	if pending == "" {
		marked := source.DeepCopy()
		if marked.Annotations == nil {
			marked.Annotations = map[string]string{}
		}

		marked.Annotations[reservationCopyPendingAnnotation] = token
		marked.Finalizers = ensureSessionFinalizer(marked.Finalizers)

		if err := handoffFenceError(ctx); err != nil {
			return nil, err
		}

		if err := client.Update(ctx, marked); err != nil {
			return nil, err
		}

		*source = *marked
	}

	target = desired.DeepCopy()
	target.Status = v1alpha1.CopyStatus{}
	target.Labels = MergeSessionLabels(source.Labels, source.Name)
	target.Annotations = maps.Clone(source.Annotations)
	target.Annotations[reservationCopyOriginAnnotation] = string(source.UID)
	target.OwnerReferences = append([]metav1.OwnerReference(nil), source.OwnerReferences...)
	target.Finalizers = ensureSessionFinalizer(target.Finalizers)
	target.TypeMeta = metav1.TypeMeta{
		APIVersion: v1alpha1.GroupVersion.String(),
		Kind:       "Copy",
	}

	if err := handoffFenceError(ctx); err != nil {
		return nil, err
	}

	if err := client.Create(ctx, target); err != nil {
		return nil, err
	}

	if !apiequality.Semantic.DeepEqual(target.Spec, desired.Spec) {
		return nil, workflowStoreConflict(
			"handoff",
			"copy admission changed the prepared input; reload before retrying",
		)
	}

	return target, nil
}

func namespacedInitializeCRDCopyHandoff(
	ctx context.Context,
	client crclient.Client,
	target *v1alpha1.Copy,
	checkpoint v1alpha1.CopyStatus,
) error {
	hash, err := WorkflowExecutionIntentHash(target)
	if err != nil {
		return err
	}

	next := target.DeepCopy()
	next.Status = *checkpoint.DeepCopy()
	next.Status.ObservedGeneration = target.Generation
	next.Status.ExecutionIntentHash = hash

	if target.Status.Plan != nil {
		if !apiequality.Semantic.DeepEqual(target.Status, next.Status) {
			return workflowStoreConflict("handoff", "pending copy checkpoint changed")
		}

		return nil
	}

	if !apiequality.Semantic.DeepEqual(target.Status, v1alpha1.CopyStatus{}) {
		return workflowStoreConflict("handoff", "uninitialized copy contains unexpected progress")
	}

	if err := handoffFenceError(ctx); err != nil {
		return err
	}

	if err := client.Status().Update(ctx, next); err != nil {
		return err
	}

	*target = *next

	return nil
}

// NamespacedActivateCRDCopyHandoff also recovers a crash after the source was deleted.
// It only removes the execution barrier once the old resource is absent.
func NamespacedActivateCRDCopyHandoff(
	ctx context.Context,
	client crclient.Client,
	target *v1alpha1.Copy,
) error {
	if target == nil || target.Status.Plan == nil || target.Status.Phase != "Reserved" ||
		target.Annotations[reservationCopyOriginAnnotation] == "" {
		return workflowStoreConflict("handoff", "copy has no initialized reservation handoff")
	}

	if err := requireWorkflowStorageVersion(target); err != nil {
		return err
	}

	previous := &v1alpha1.Copy{}
	if err := client.Get(ctx, crclient.ObjectKeyFromObject(target), previous); err != nil {
		return err
	}

	if err := checkWorkflowStorageVersion(target, previous); err != nil {
		return err
	}

	if !apiequality.Semantic.DeepEqual(previous.Spec, target.Spec) ||
		!apiequality.Semantic.DeepEqual(previous.Status, target.Status) ||
		previous.Annotations[reservationCopyOriginAnnotation] != target.Annotations[reservationCopyOriginAnnotation] {
		return workflowStoreConflict("handoff", "copy content differs from the loaded handoff")
	}

	source := &v1alpha1.Reservation{}

	err := client.Get(ctx, crclient.ObjectKeyFromObject(target), source)
	if err == nil {
		return workflowStoreConflict("handoff", "reservation removal is still pending")
	}

	if !apierrors.IsNotFound(err) {
		return err
	}

	if previous.Annotations[reservationCopyPendingAnnotation] == "" {
		*target = *previous
		return nil
	}

	delete(previous.Annotations, reservationCopyPendingAnnotation)

	if err := handoffFenceError(ctx); err != nil {
		return err
	}

	if err := client.Update(ctx, previous); err != nil {
		return err
	}

	*target = *previous

	return nil
}
