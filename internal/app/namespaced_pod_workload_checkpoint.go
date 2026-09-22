package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func (m *PodMigrationExecutor) saveWorkloadCheckpoint(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	checkpoint *v1alpha1.PodMigrationWorkloadStatus,
) error {
	if checkpoint == nil {
		return nil
	}

	previous := object.Status.Workload.DeepCopy()

	object.Status.Workload = checkpoint.DeepCopy()
	if err := persistCheckpoint(
		ctx,
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
	); err != nil {
		object.Status.Workload = previous
		return err
	}

	return nil
}
