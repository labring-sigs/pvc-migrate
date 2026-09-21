package app

import "testing"

func TestFinalizeDeletedRejectsNilWorkflow(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
	}{
		{name: "copy", run: func() error { return (&CopyExecutor{}).FinalizeDeleted(t.Context(), nil) }},
		{name: "cluster copy", run: func() error {
			return (&ClusterCopyExecutor{}).FinalizeDeleted(t.Context(), nil)
		}},
		{name: "migration", run: func() error {
			return (&MigrationExecutor{}).FinalizeDeleted(t.Context(), nil)
		}},
		{name: "cluster migration", run: func() error {
			return (&ClusterMigrationExecutor{}).FinalizeDeleted(t.Context(), nil)
		}},
		{name: "pod migration", run: func() error {
			return (&PodMigrationExecutor{}).FinalizeDeleted(t.Context(), nil)
		}},
		{name: "cluster pod migration", run: func() error {
			return (&ClusterPodMigrationExecutor{}).FinalizeDeleted(t.Context(), nil)
		}},
		{name: "reservation", run: func() error {
			return (&ReservationExecutor{}).FinalizeDeleted(t.Context(), nil)
		}},
		{name: "cluster reservation", run: func() error {
			return (&ClusterReservationExecutor{}).FinalizeDeleted(t.Context(), nil)
		}},
		{name: "move", run: func() error { return (&MoveExecutor{}).FinalizeDeleted(t.Context(), nil) }},
		{name: "rename", run: func() error {
			return (&RenameExecutor{}).FinalizeDeleted(t.Context(), nil)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err == nil {
				t.Fatal("nil workflow unexpectedly finalized")
			}
		})
	}
}
