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

func TestNamespacedPendingHandoffRejectsDifferentResourceIdentity(t *testing.T) {
	base, source, target := namespacedCRDHandoffFixture(t)
	failure := errors.New("stop before reservation release")
	failing := true

	client := failingNamespacedHandoffClient(base, "release", &failing, failure)
	if err := NamespacedHandoffCRDReservationToCopy(
		t.Context(),
		client,
		source,
		target,
	); !errors.Is(
		err,
		failure,
	) {
		t.Fatalf("error = %v", err)
	}

	if err := base.Get(t.Context(), crclient.ObjectKeyFromObject(source), target); err != nil {
		t.Fatal(err)
	}

	if err := NamespacedValidatePendingReservationCopy(source, target); err != nil {
		t.Fatal(err)
	}

	for _, change := range []func(*v1alpha1.Copy){
		func(object *v1alpha1.Copy) { object.Namespace = "foreign" },
		func(object *v1alpha1.Copy) { object.Name = "foreign" },
	} {
		foreign := target.DeepCopy()
		change(foreign)

		if err := NamespacedValidatePendingReservationCopy(source, foreign); err == nil {
			t.Fatal("foreign resource matched the pending handoff")
		}
	}
}

func TestNamespacedCancelCRDCopyHandoffRestoresSourceBeforeDeletingTarget(t *testing.T) {
	base, source, destination := namespacedCRDHandoffFixture(t)
	failure := errors.New("handoff interrupted")
	failing := true

	client := failingNamespacedHandoffClient(base, "release", &failing, failure)
	if err := NamespacedHandoffCRDReservationToCopy(
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

	key := crclient.ObjectKeyFromObject(source)
	if err := base.Get(t.Context(), key, source); err != nil {
		t.Fatal(err)
	}

	before := source.Status.DeepCopy()

	client = failingNamespacedHandoffClient(base, "activate", &failing, failure)
	if err := NamespacedCancelCRDReservationCopyHandoff(
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

	if err := NamespacedCancelCRDReservationCopyHandoff(t.Context(), client, source); err != nil {
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

func TestNamespacedCRDCopyHandoffCanTransferCleanupOwnershipToDeletingCopy(t *testing.T) {
	base, source, destination := namespacedCRDHandoffFixture(t)
	failure := errors.New("activation interrupted")
	failing := true

	client := failingNamespacedHandoffClient(base, "activate", &failing, failure)
	if err := NamespacedHandoffCRDReservationToCopy(
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

	key := crclient.ObjectKeyFromObject(source)
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

	if err := NamespacedActivateCRDCopyHandoff(t.Context(), base, destination); err != nil {
		t.Fatal(err)
	}

	if RequireWorkflowHandoffComplete(destination) != nil || destination.DeletionTimestamp == nil ||
		!slices.Contains(destination.Finalizers, SessionFinalizer) {
		t.Fatal("deleting Copy did not receive cleanup responsibility")
	}
}

func TestNamespacedCRDReservationHandoffRejectsForeignTargetWithoutFreezingSource(t *testing.T) {
	base, source, destination := namespacedCRDHandoffFixture(t)
	foreign := destination.DeepCopy()

	foreign.UID = "foreign-copy"
	if err := base.Create(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}

	if err := NamespacedHandoffCRDReservationToCopy(
		t.Context(),
		base,
		source,
		destination,
	); err == nil {
		t.Fatal("foreign Copy was adopted")
	}

	stored := &v1alpha1.Reservation{}
	if err := base.Get(t.Context(), crclient.ObjectKeyFromObject(source), stored); err != nil {
		t.Fatal(err)
	}

	if RequireWorkflowHandoffComplete(stored) != nil {
		t.Fatal("foreign target froze the reservation")
	}
}

func TestNamespacedCRDReservationHandoffFenceLossPreservesBothProtectedRecords(t *testing.T) {
	base, source, destination := namespacedCRDHandoffFixture(t)
	lost := errors.New("lease lost after checkpoint")
	failing := false
	fence := &testLeaseFence{}
	client := interceptor.NewClient(
		failingNamespacedHandoffClient(base, "", &failing, nil),
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

	if err := NamespacedHandoffCRDReservationToCopy(
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

	key := crclient.ObjectKeyFromObject(source)
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
