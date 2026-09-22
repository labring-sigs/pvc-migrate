package cli

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func (r *rootState) submitBackup(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.Backup,
) error {
	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		object,
		func() *v1alpha1.Backup { return &v1alpha1.Backup{} },
		"Backup",
		"backups",
		[]string{object.Namespace},
		domain.PhaseCompleted,
		func(current *v1alpha1.Backup) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}

func (r *rootState) submitRestore(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.Restore,
) error {
	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		object,
		func() *v1alpha1.Restore { return &v1alpha1.Restore{} },
		"Restore",
		"restores",
		[]string{object.Namespace},
		domain.PhaseCompleted,
		func(current *v1alpha1.Restore) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}
