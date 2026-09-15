package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func (m *ClusterPodMigrationExecutor) saveWorkloadCheckpoint(
	ctx context.Context,
	object *v1alpha1.ClusterPodMigration,
	checkpoint *v1alpha1.PodMigrationWorkloadStatus,
) error {
	if checkpoint == nil {
		return nil
	}

	previous := object.Status.Workload.DeepCopy()
	plan := object.Status.Plan

	object.Status.Workload = qualifiedPodWorkloadCheckpoint(
		checkpoint,
		string(plan.SourceNamespace),
	)
	if err := persistCheckpoint(
		ctx,
		func(ctx context.Context) error { return m.store.Save(ctx, object) },
	); err != nil {
		object.Status.Workload = previous
		return err
	}

	return nil
}
