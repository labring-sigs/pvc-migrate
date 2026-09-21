package cli

import (
	"context"
	"errors"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) loadMigrationWithBackend(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
) (crclient.Object, string, error) {
	if runtime.clients == nil {
		return nil, "", domain.NewError(
			domain.ErrorInternal,
			"migration",
			"Kubernetes clients are required",
		)
	}

	namespace := r.workflowStorageNamespace(cmd)

	object, backend, err := r.loadWorkflowWithBackend(
		ctx,
		cmd,
		runtime,
		namespace,
		id,
		map[domain.ControllerKind]crclient.Object{
			domain.ControllerKindMigration:        &v1alpha1.Migration{},
			domain.ControllerKindClusterMigration: &v1alpha1.ClusterMigration{},
		},
	)
	if err != nil {
		return nil, "", reportSessionLookupError(cmd, namespace, id, err)
	}

	switch object.(type) {
	case *v1alpha1.Migration, *v1alpha1.ClusterMigration:
		return object, backend, nil
	default:
		return nil, "", domain.NewError(
			domain.ErrorValidation,
			"migration",
			"stored workflow is not an offline migration",
		)
	}
}

func (r *rootState) migrationConfig(runtime *commandRuntime) app.MigrationExecutorConfig {
	return app.MigrationExecutorConfig{
		Transfer:        r.volumeCopyConfig(runtime),
		ToolImageProber: kube.NewToolImageProber(runtime.clients.Kubernetes),
		ProbeTimeout:    r.global.helmTimeout,
	}
}

func reportMigrationError(
	cmd *cobra.Command,
	name string,
	phase v1alpha1.WorkflowPhase,
	cause error,
) error {
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Migration %s stopped in phase %s. Inspect migrate status %s before resume, rollback or cleanup.\n",
		name,
		phase,
		name,
	)

	return errors.Join(cause, err)
}

func (r *rootState) migrationExecutor(
	runtime *commandRuntime,
	cmd *cobra.Command,
	backend string,
) (*app.MigrationExecutor, error) {
	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		r.workflowStorageNamespace(cmd),
		func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
	)
	if err != nil {
		return nil, err
	}

	return app.NewMigrationExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLockerForBackend(runtime, backend),
		copyengine.NewPVMigrate(),
		r.migrationConfig(runtime),
	), nil
}

func (r *rootState) executeMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.Migration,
	dryRun bool,
	backend string,
) error {
	executor, err := r.migrationExecutor(runtime, cmd, backend)
	if err != nil {
		return err
	}

	if dryRun {
		if err := executor.Validate(ctx, object); err != nil {
			return reportMigrationError(cmd, object.Name, object.Status.Phase, err)
		}
		return runtime.printer.Print(object)
	}

	phase := copyResumePhase(object.Status.WorkflowStatus)
	if requiresResumeApproval(phase) ||
		requiresOperationResumeApproval(domain.OperationMigrate, phase) {
		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}
	}

	if err := executor.RequestResume(ctx, object); err != nil {
		return reportMigrationError(cmd, object.Name, object.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportMigrationError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

func (r *rootState) clusterMigrationExecutor(
	runtime *commandRuntime,
	cmd *cobra.Command,
	object *v1alpha1.ClusterMigration,
	backend string,
) (*app.ClusterMigrationExecutor, error) {
	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		r.workflowStorageNamespace(cmd),
		func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} },
	)
	if err != nil {
		return nil, err
	}

	namespace := clusterMigrationStorageNamespace(object)

	return app.NewClusterMigrationExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLockerForBackend(runtime, backend),
		namespace,
		copyengine.NewPVMigrate(),
		r.migrationConfig(runtime),
	), nil
}

func clusterMigrationStorageNamespace(object *v1alpha1.ClusterMigration) string {
	if object == nil {
		return ""
	}

	if object.Spec.SessionNamespace != "" {
		return string(object.Spec.SessionNamespace)
	}

	return string(object.Spec.SourceNamespace)
}

func (r *rootState) executeClusterMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.ClusterMigration,
	dryRun bool,
	backend string,
) error {
	executor, err := r.clusterMigrationExecutor(runtime, cmd, object, backend)
	if err != nil {
		return err
	}

	if dryRun {
		if err := executor.Validate(ctx, object); err != nil {
			return reportMigrationError(cmd, object.Name, object.Status.Phase, err)
		}
		return runtime.printer.Print(object)
	}

	phase := copyResumePhase(object.Status.WorkflowStatus)
	if requiresResumeApproval(phase) ||
		requiresOperationResumeApproval(domain.OperationMigrate, phase) {
		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}
	}

	if err := executor.RequestResume(ctx, object); err != nil {
		return reportMigrationError(cmd, object.Name, object.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportMigrationError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

func (r *rootState) resumeMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	dryRun bool,
) error {
	object, backend, err := r.loadMigrationWithBackend(ctx, cmd, runtime, id)
	if err != nil {
		return err
	}

	switch current := object.(type) {
	case *v1alpha1.Migration:
		return r.executeMigration(ctx, cmd, runtime, current, dryRun, backend)
	case *v1alpha1.ClusterMigration:
		return r.executeClusterMigration(ctx, cmd, runtime, current, dryRun, backend)
	default:
		return domain.NewError(domain.ErrorValidation, "migration", "unsupported migration input")
	}
}

func (r *rootState) validateMigrationReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
) error {
	object, backend, err := r.loadMigrationWithBackend(ctx, cmd, runtime, id)
	if err != nil {
		return err
	}

	switch current := object.(type) {
	case *v1alpha1.Migration:
		executor, err := r.migrationExecutor(runtime, cmd, backend)
		if err != nil {
			return err
		}

		if err := executor.ValidateReservation(ctx, current); err != nil {
			return reportMigrationError(cmd, current.Name, current.Status.Phase, err)
		}
	case *v1alpha1.ClusterMigration:
		executor, err := r.clusterMigrationExecutor(runtime, cmd, current, backend)
		if err != nil {
			return err
		}

		if err := executor.ValidateReservation(ctx, current); err != nil {
			return reportMigrationError(cmd, current.Name, current.Status.Phase, err)
		}
	}

	return runtime.printer.Print(object)
}
