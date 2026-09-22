package app

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AdoptReservation validates Copy's live resource rules while holding the
// reservation's storage fence. Persistence commits both the operation identity
// and existing volume checkpoints before normal Copy execution may begin.
func (c *ClusterCopyExecutor) AdoptReservation(
	ctx context.Context,
	reservationStore kube.WorkflowStore[*v1alpha1.ClusterReservation],
	reservation *v1alpha1.ClusterReservation,
	spec v1alpha1.ClusterCopySpec,
	handoff func(context.Context, *v1alpha1.ClusterReservation, *v1alpha1.ClusterCopy) error,
) (*v1alpha1.ClusterCopy, error) {
	if err := validateClusterReservationObject(reservation); err != nil {
		return nil, err
	}

	if handoff == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"copy reservation",
			"handoff storage is not configured",
		)
	}

	var object *v1alpha1.ClusterCopy

	err := withStoredWorkflowLease(ctx, reservationStore, c.locker, c.storageNamespace, reservation,
		func(ctx context.Context) error {
			prepared, err := CopyFromReservation(reservation, spec)
			if err != nil {
				return err
			}

			if err := c.Validate(ctx, prepared); err != nil {
				return err
			}

			if err := checkpointFenceError(ctx); err != nil {
				return err
			}

			if err := handoff(ctx, reservation, prepared); err != nil {
				return err
			}

			object = prepared

			return nil
		})
	if err != nil {
		return nil, err
	}

	return object, nil
}

// ActivateReservationHandoff recovers the final metadata step after the original
// Reservation has been removed. The storage callback verifies that absence.
func (c *ClusterCopyExecutor) ActivateReservationHandoff(
	ctx context.Context,
	object *v1alpha1.ClusterCopy,
	activate func(context.Context, *v1alpha1.ClusterCopy) error,
) error {
	if err := validateClusterCopyObject(object); err != nil {
		return err
	}

	if activate == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"copy handoff",
			"handoff storage is not configured",
		)
	}

	if object.DeletionTimestamp != nil {
		ctx = context.WithValue(ctx, workflowDeletionContextKey{}, true)
	}

	return withStoredWorkflowLease(ctx, c.store, c.locker, c.storageNamespace, object,
		func(ctx context.Context) error { return activate(ctx, object) })
}

// CancelCopyHandoff restores the reservation's authority before its normal
// lifecycle, including deletion, may continue.
func (r *ClusterReservationExecutor) CancelCopyHandoff(
	ctx context.Context,
	object *v1alpha1.ClusterReservation,
	cancelHandoff func(context.Context, *v1alpha1.ClusterReservation) error,
) error {
	if err := validateClusterReservationObject(object); err != nil {
		return err
	}

	if cancelHandoff == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"reservation handoff",
			"handoff storage is not configured",
		)
	}

	if object.DeletionTimestamp != nil {
		ctx = context.WithValue(ctx, workflowDeletionContextKey{}, true)
	}

	return withStoredWorkflowLease(ctx, r.store, r.locker, r.storageNamespace, object,
		func(ctx context.Context) error { return cancelHandoff(ctx, object) })
}

// CopySpecFromReservation gives the Copy entrypoint its own CRD input before
// applying explicitly requested copy options. No resolved storage is rediscovered.
func CopySpecFromReservation(spec v1alpha1.ClusterReservationSpec) v1alpha1.ClusterCopySpec {
	owned := spec.DeepCopy()

	return v1alpha1.ClusterCopySpec{
		SourceNamespace:      owned.SourceNamespace,
		DestinationNamespace: owned.DestinationNamespace,
		SessionNamespace:     owned.SessionNamespace,
		CopySpec: v1alpha1.CopySpec{
			TransferOptions: owned.TransferOptions,
			Volumes:         owned.Volumes,
			Pod:             owned.Pod,
		},
	}
}

// CopyFromReservation prepares an independently owned Copy CRD. The caller must
// validate live Copy resources and persist the handoff under the reservation lock.
func CopyFromReservation(
	reservation *v1alpha1.ClusterReservation,
	spec v1alpha1.ClusterCopySpec,
) (*v1alpha1.ClusterCopy, error) {
	if err := validateClusterReservationObject(reservation); err != nil {
		return nil, err
	}

	if reservation.Status.Phase != domain.PhaseReserved || reservation.DeletionTimestamp != nil {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"copy reservation",
			"a completed, non-deleting reservation is required",
		)
	}

	if err := validateReservedCopyInput(reservation.Spec, spec); err != nil {
		return nil, err
	}

	plan := reservation.Status.Plan.DeepCopy()
	if spec.SourceNode != reservation.Spec.SourceNode {
		plan.SourceNode = spec.SourceNode
	}

	if len(plan.Strategies) == 0 || !slices.Equal(spec.Strategies, reservation.Spec.Strategies) {
		plan.Strategies = copyengine.ResolveStrategies(
			string(spec.SourceNamespace),
			string(spec.DestinationNamespace),
			spec.Strategies,
		)
	}

	if err := copyengine.ValidateStrategies(plan.Strategies); err != nil {
		return nil, err
	}

	object := &v1alpha1.ClusterCopy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "ClusterCopy",
		},
		ObjectMeta: metav1.ObjectMeta{Name: reservation.Name},
		Spec:       *spec.DeepCopy(),
		Status: v1alpha1.ClusterCopyStatus{
			WorkflowStatus: *reservation.Status.WorkflowStatus.DeepCopy(),
			Plan: &v1alpha1.ClusterCopyPlan{
				SourceNamespace:      plan.SourceNamespace,
				DestinationNamespace: plan.DestinationNamespace,
				SessionNamespace:     plan.SessionNamespace,
				Volumes:              plan.Volumes,
				SourceNode:           plan.SourceNode,
				TargetNode:           plan.TargetNode,
				ToolImage:            plan.ToolImage,
				Strategies:           plan.Strategies,
				VerifyChecksum:       spec.VerifyChecksum,
				DeleteExtraneous:     spec.DeleteExtraneousValue(),
				SkipSourceUsageCheck: plan.SkipSourceUsageCheck,
				UnusedStoragePolicy:  spec.UnusedStoragePolicy,
				Online:               spec.Online,
			},
		},
	}
	object.Status.ObservedGeneration = 0
	object.Status.ExecutionIntentHash = ""

	object.Status.Message = "Reserved storage is ready for copy"
	for _, checkpoint := range reservation.Status.Volumes {
		object.Status.Volumes = append(object.Status.Volumes, v1alpha1.ClusterCopyVolumeStatus{
			ClusterVolumeReservationStatus: *checkpoint.ClusterVolumeReservationStatus.DeepCopy(),
		})
	}

	if err := validateClusterCopyObject(object); err != nil {
		return nil, err
	}

	return object, nil
}

func validateReservedCopyInput(
	source v1alpha1.ClusterReservationSpec,
	destination v1alpha1.ClusterCopySpec,
) error {
	if err := domain.ValidateUnusedStoragePolicy(
		destination.UnusedStoragePolicy,
	); err != nil {
		return err
	}

	expected := CopySpecFromReservation(source)
	storage := destination.DeepCopy()
	storage.Online = false
	storage.SourceNode = expected.SourceNode
	storage.Strategies = slices.Clone(expected.Strategies)
	storage.VerifyChecksum = expected.VerifyChecksum
	storage.DeleteExtraneous = expected.DeleteExtraneous

	storage.UnusedStoragePolicy = expected.UnusedStoragePolicy
	if !apiequality.Semantic.DeepEqual(*storage, expected) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"copy reservation",
			"copy must preserve reserved volume selection, storage placement, capacity and paths",
		)
	}

	return nil
}
