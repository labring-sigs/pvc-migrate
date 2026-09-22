package crosscluster_test

import (
	"encoding/json"
	"strings"
	"testing"

	. "github.com/labring-sigs/pvc-migrate/internal/crosscluster"
)

// markReserved fills the destination checkpoints the reserve flow persists,
// moving the session to the phase promotion requires.
func markReserved(t *testing.T, reservation *ReservationSession) {
	t.Helper()

	for i := range reservation.Spec.Volumes {
		destination := &reservation.Spec.Volumes[i].Destination
		destination.PVC.UID = "destination-pvc-uid"
		destination.PV = ClusterResourceRef{
			ClusterID:  destination.PVC.ClusterID,
			APIVersion: "v1",
			Kind:       "PersistentVolume",
			Name:       destination.PVC.Name + "-pv",
			UID:        "destination-pv-uid",
		}

		checkpoint := &reservation.Status.Volumes[i].Reservation
		checkpoint.PVC = destination.PVC
		checkpoint.PV = destination.PV
	}

	reservation.Status.Phase = PhaseReserved
}

func cloneReservationSession(
	t *testing.T,
	reservation *ReservationSession,
) *ReservationSession {
	t.Helper()

	raw, err := json.Marshal(reservation)
	if err != nil {
		t.Fatal(err)
	}

	var clone ReservationSession
	if err := json.Unmarshal(raw, &clone); err != nil {
		t.Fatal(err)
	}

	// ResourceVersion is deliberately not serialized; a loaded session keeps
	// it in memory, so the clone must keep the stale value too.
	clone.ResourceVersion = reservation.ResourceVersion

	return &clone
}

// TestPromoteReservationCarriesReservationCheckpointsIntoCopySession pins the
// promotion contract: the copy session inherits every reservation checkpoint,
// carries no fabricated transfer progress, and replaces the persisted record so
// the typed reservation loader no longer accepts it.
func TestPromoteReservationCarriesReservationCheckpointsIntoCopySession(t *testing.T) {
	service, options, reservation := reservationFixture(t)
	markReserved(t, reservation)

	promoted, err := service.PromoteReservation(t.Context(), reservation)
	if err != nil {
		t.Fatal(err)
	}

	if promoted.Kind != CopyKind || promoted.ID != reservation.ID ||
		promoted.ResourceVersion == "" {
		t.Fatalf("promoted session envelope=%+v", promoted.SessionEnvelope)
	}

	if len(promoted.Status.Volumes) != len(reservation.Status.Volumes) {
		t.Fatalf(
			"promoted volume statuses=%d, want %d",
			len(promoted.Status.Volumes),
			len(reservation.Status.Volumes),
		)
	}

	for i := range reservation.Status.Volumes {
		checkpoint := &promoted.Status.Volumes[i]
		if checkpoint.SourcePVCName != reservation.Status.Volumes[i].SourcePVCName ||
			checkpoint.Reservation.PVC != reservation.Status.Volumes[i].Reservation.PVC ||
			checkpoint.Reservation.PV != reservation.Status.Volumes[i].Reservation.PV {
			t.Fatalf(
				"promoted volume %d lost its reservation checkpoint: %+v",
				i,
				checkpoint.ReservationVolumeStatus,
			)
		}

		if checkpoint.Transfer.Attempts != 0 || checkpoint.Transfer.CompletedAt != nil ||
			checkpoint.Transfer.LastError != "" {
			t.Fatalf("promoted volume %d fabricated transfer progress: %+v", i, checkpoint.Transfer)
		}
	}

	loaded, err := service.Get(t.Context(), options.SessionNamespace, options.SessionID)
	if err != nil {
		t.Fatalf("typed copy loader rejected the promoted record: %v", err)
	}

	if loaded.Kind != CopyKind || loaded.Status.Phase != PhaseReserved {
		t.Fatalf("promoted record kind=%q phase=%q", loaded.Kind, loaded.Status.Phase)
	}

	if _, err := service.GetReservation(
		t.Context(), options.SessionNamespace, options.SessionID,
	); err == nil || !strings.Contains(err.Error(), "ownership does not match") {
		t.Fatalf("reservation loader accepted a promoted copy record: err=%v", err)
	}
}

func TestPromoteReservationRejectsUnreservedSession(t *testing.T) {
	service, _, reservation := reservationFixture(t)

	if _, err := service.PromoteReservation(t.Context(), reservation); err == nil ||
		!strings.Contains(err.Error(), "reserve all destination PVCs before copying") {
		t.Fatalf("promote error=%v, want unreserved rejection", err)
	}
}

// TestPromoteReservationFencesStaleReservationAfterPromotion pins the window
// after a completed promotion: a stale in-memory reservation must not be able
// to re-promote, resume reserving, or clean up over the new copy record.
func TestPromoteReservationFencesStaleReservationAfterPromotion(t *testing.T) {
	service, _, reservation := reservationFixture(t)
	markReserved(t, reservation)

	stale := cloneReservationSession(t, reservation)

	if _, err := service.PromoteReservation(t.Context(), reservation); err != nil {
		t.Fatal(err)
	}

	if _, err := service.PromoteReservation(t.Context(), stale); err == nil {
		t.Fatal("re-promotion with a stale reservation succeeded")
	}

	// A stale reserved reservation is an idempotent no-op: it must neither
	// rewrite the promoted record nor fail the reserve command.
	if err := service.Reserve(t.Context(), stale); err != nil {
		t.Fatalf("stale reserve failed without writing: %v", err)
	}

	if err := service.CleanupReservation(t.Context(), stale, "Keep", true); err == nil {
		t.Fatal("stale reservation cleaned up the promoted record")
	}

	loaded, err := service.Get(t.Context(), reservation.Spec.SessionNamespace, reservation.ID)
	if err != nil || loaded.Kind != CopyKind {
		t.Fatalf("promoted record after stale writes: kind=%q err=%v", loaded.Kind, err)
	}
}
