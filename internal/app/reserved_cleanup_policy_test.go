package app

import (
	"reflect"
	"testing"
)

func TestReservationCleanupUsesCurrentSpecPolicyWithoutReplanning(t *testing.T) {
	executor, object, _, client := reservationCleanupFixture(t)
	object.Status.Plan.DestinationPVCReclaimPolicy = "Retain"
	object.Spec.DestinationPVCReclaimPolicy = "Delete"
	before := object.DeepCopy()

	_, volumes, _, err := executor.prepareCleanup(t.Context(), object, ReservationCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if !volume.delete {
			t.Fatal("cleanup ignored the current spec policy")
		}
	}

	_, volumes, _, err = executor.prepareCleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{DestinationPVCReclaimPolicy: "Retain"},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if volume.delete {
			t.Fatal("explicit cleanup policy did not override the spec")
		}
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("cleanup policy selection changed the workflow")
	}

	assertReservationCleanupReadOnly(t, client)
}

func TestCopyCleanupUsesCurrentSpecPolicyWithoutReplanning(t *testing.T) {
	executor, object, _, client := copyCleanupFixture(t)
	object.Status.Plan.DestinationPVCReclaimPolicy = "Retain"
	object.Spec.DestinationPVCReclaimPolicy = "Delete"
	before := object.DeepCopy()

	_, volumes, _, err := executor.prepareCleanup(t.Context(), object, CopyCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if !volume.delete {
			t.Fatal("cleanup ignored the current spec policy")
		}
	}

	_, volumes, _, err = executor.prepareCleanup(
		t.Context(),
		object,
		CopyCleanupOptions{DestinationPVCReclaimPolicy: "Retain"},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if volume.delete {
			t.Fatal("explicit cleanup policy did not override the spec")
		}
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("cleanup policy selection changed the workflow")
	}

	assertReservationCleanupReadOnly(t, client)
}
