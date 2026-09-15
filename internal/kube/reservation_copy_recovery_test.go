package kube

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestCancelCRDCopyHandoffRestoresSourceBeforeDeletingTarget(t *testing.T) {
	base, source, destination := crdHandoffFixture(t)
	failure := errors.New("handoff interrupted")
	failing := true

	client := failingHandoffClient(base, "release", &failing, failure)
	if err := HandoffCRDReservationToCopy(
		t.Context(),
		client,
		source,
		destination,
	); !errors.Is(
		err,
		failure,
	) {
		t.Fatalf("error = %v", err)
	}

	key := crclient.ObjectKey{Name: source.Name}
	if err := base.Get(t.Context(), key, source); err != nil {
		t.Fatal(err)
	}

	before := source.Status.DeepCopy()

	client = failingHandoffClient(base, "activate", &failing, failure)
	if err := CancelCRDReservationCopyHandoff(
		t.Context(),
		client,
		source,
	); !errors.Is(
		err,
		failure,
	) {
		t.Fatalf("error = %v", err)
	}

	if RequireWorkflowHandoffComplete(source) != nil ||
		!slices.Contains(source.Finalizers, SessionFinalizer) ||
		!reflect.DeepEqual(source.Status, *before) {
		t.Fatal("cancellation failed to restore source protection and progress")
	}

	if err := base.Get(t.Context(), key, destination); err != nil {
		t.Fatal(err)
	}

	if RequireWorkflowHandoffComplete(destination) == nil {
		t.Fatal("cancellation activated Copy")
	}

	failing = false

	if err := CancelCRDReservationCopyHandoff(t.Context(), client, source); err != nil {
		t.Fatal(err)
	}

	if err := base.Get(t.Context(), key, destination); !apierrors.IsNotFound(err) {
		t.Fatalf("target still exists: %v", err)
	}

	if err := base.Get(t.Context(), key, source); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(source.Status, *before) {
		t.Fatal("canceling handoff changed reserved storage checkpoints")
	}
}

func TestCRDCopyHandoffCanTransferCleanupOwnershipToDeletingCopy(t *testing.T) {
	base, source, destination := crdHandoffFixture(t)
	failure := errors.New("activation interrupted")
	failing := true

	client := failingHandoffClient(base, "activate", &failing, failure)
	if err := HandoffCRDReservationToCopy(
		t.Context(),
		client,
		source,
		destination,
	); !errors.Is(
		err,
		failure,
	) {
		t.Fatalf("error = %v", err)
	}

	key := crclient.ObjectKey{Name: source.Name}
	if err := base.Get(t.Context(), key, destination); err != nil {
		t.Fatal(err)
	}

	if err := base.Delete(t.Context(), destination); err != nil {
		t.Fatal(err)
	}

	if err := base.Get(t.Context(), key, destination); err != nil {
		t.Fatal(err)
	}

	if destination.DeletionTimestamp == nil {
		t.Fatal("expected protected Copy deletion")
	}

	if err := ActivateCRDCopyHandoff(t.Context(), base, destination); err != nil {
		t.Fatal(err)
	}

	if RequireWorkflowHandoffComplete(destination) != nil || destination.DeletionTimestamp == nil ||
		!slices.Contains(destination.Finalizers, SessionFinalizer) {
		t.Fatal("deleting Copy did not receive cleanup responsibility")
	}
}

func TestCRDReservationHandoffRejectsForeignTargetWithoutFreezingSource(t *testing.T) {
	base, source, destination := crdHandoffFixture(t)
	foreign := destination.DeepCopy()

	foreign.UID = "foreign-copy"
	if err := base.Create(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}

	if err := HandoffCRDReservationToCopy(t.Context(), base, source, destination); err == nil {
		t.Fatal("foreign Copy was adopted")
	}

	stored := &v1alpha1.ClusterReservation{}
	if err := base.Get(t.Context(), crclient.ObjectKeyFromObject(source), stored); err != nil {
		t.Fatal(err)
	}

	if RequireWorkflowHandoffComplete(stored) != nil {
		t.Fatal("foreign target froze the reservation")
	}
}

func TestCRDReservationHandoffFenceLossPreservesBothProtectedRecords(t *testing.T) {
	base, source, destination := crdHandoffFixture(t)
	lost := errors.New("lease lost after checkpoint")
	failing := false
	fence := &testLeaseFence{}
	client := interceptor.NewClient(
		failingHandoffClient(base, "", &failing, nil),
		interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, underlying crclient.Client, name string, object crclient.Object, options ...crclient.SubResourceUpdateOption) error {
				err := underlying.SubResource(name).Update(ctx, object, options...)
				if err == nil {
					fence.err = lost
				}

				return err
			},
		},
	)

	if err := HandoffCRDReservationToCopy(
		WithLeaseFence(t.Context(), fence),
		client,
		source,
		destination,
	); !errors.Is(
		err,
		lost,
	) {
		t.Fatalf("error = %v", err)
	}

	key := crclient.ObjectKey{Name: source.Name}
	if err := base.Get(t.Context(), key, source); err != nil {
		t.Fatal(err)
	}

	if err := base.Get(t.Context(), key, destination); err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(source.Finalizers, SessionFinalizer) ||
		!slices.Contains(destination.Finalizers, SessionFinalizer) ||
		RequireWorkflowHandoffComplete(source) == nil ||
		RequireWorkflowHandoffComplete(destination) == nil ||
		destination.Status.Plan == nil {
		t.Fatal("fence loss released protection or lost the durable checkpoint")
	}
}
