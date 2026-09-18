package app

import (
	"reflect"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestReservationCleanupPolicyAppliesOnlyAfterAbort(t *testing.T) {
	executor, object, _, client := reservationCleanupFixture(t)

	// A held reservation always keeps its deliverable, whatever the policy says.
	_, volumes, _, err := executor.prepareCleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{UnusedStoragePolicy: "Delete"},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if volume.delete {
			t.Fatal("held reservation authorized deleting the deliverable")
		}
	}

	object.Status.Phase = domain.PhaseAborted
	before := object.DeepCopy()

	// After an abort the staged destination is the unused copy.
	_, volumes, _, err = executor.prepareCleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{UnusedStoragePolicy: "Delete"},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if !volume.delete {
			t.Fatal("aborted reservation ignored the delete policy")
		}
	}

	_, volumes, _, err = executor.prepareCleanup(t.Context(), object, ReservationCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if volume.delete {
			t.Fatal("cleanup deleted storage without an explicit policy")
		}
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("cleanup policy selection changed the workflow")
	}

	assertReservationCleanupReadOnly(t, client)
}

func TestCopyCleanupPolicyAppliesOnlyAfterAbort(t *testing.T) {
	executor, object, _, client := copyCleanupFixture(t)

	// A completed copy keeps its deliverable, whatever the policy says.
	_, volumes, _, err := executor.prepareCleanup(
		t.Context(),
		object,
		CopyCleanupOptions{UnusedStoragePolicy: "Delete"},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if volume.delete {
			t.Fatal("completed copy authorized deleting the deliverable")
		}
	}

	object.Status.Phase = domain.PhaseAborted
	before := object.DeepCopy()

	// After an abort the staged destination is the unused copy.
	_, volumes, _, err = executor.prepareCleanup(
		t.Context(),
		object,
		CopyCleanupOptions{UnusedStoragePolicy: "Delete"},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if !volume.delete {
			t.Fatal("aborted copy ignored the delete policy")
		}
	}

	_, volumes, _, err = executor.prepareCleanup(t.Context(), object, CopyCleanupOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for _, volume := range volumes {
		if volume.delete {
			t.Fatal("cleanup deleted storage without an explicit policy")
		}
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("cleanup policy selection changed the workflow")
	}

	assertReservationCleanupReadOnly(t, client)
}
