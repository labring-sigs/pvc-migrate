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

// migrationRecordNamespace resolves the namespace migration session records
// live in: the configured session namespace. The -n the namespaced migrate
// family binds addresses the tenant namespace of the workflow, never the
// ConfigMap storage location.
func (r *rootState) migrationRecordNamespace() string {
	return r.global.sessionNamespace
}

// loadMigrationWithBackend resolves one migration identity from the single
// backend its command family addresses — ConfigMap session records for the
// session commands, workflow CRs for the cr commands — restricted to the one
// record scope the family owns: migrate addresses Migration records,
// cluster-migrate and cr cluster-migrate address ClusterMigration records,
// cr migrate addresses Migration CRs.
func (r *rootState) loadMigrationWithBackend(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	source workflowSource,
	scope recordScope,
) (crclient.Object, string, error) {
	if runtime.clients == nil {
		return nil, "", domain.NewError(
			domain.ErrorInternal,
			"migration",
			"Kubernetes clients are required",
		)
	}

	namespace := r.workflowStorageNamespace(cmd)
	if source == sourceSession {
		namespace = r.migrationRecordNamespace()
	}

	kind := domain.ControllerKindMigration
	if scope == clusterRecords {
		kind = domain.ControllerKindClusterMigration
	}

	object, backend, err := r.loadWorkflowWithBackend(
		ctx,
		cmd,
		runtime,
		namespace,
		id,
		map[domain.ControllerKind]crclient.Object{kind: newWorkflowObject(kind)},
		source,
	)
	if err != nil {
		return nil, "", reportSessionLookupError(cmd, namespace, id, err)
	}

	switch current := object.(type) {
	case *v1alpha1.Migration:
		if scope == namespacedRecords {
			return current, backend, nil
		}

		return nil, "", domain.NewError(
			domain.ErrorValidation,
			"migration",
			"stored workflow is a namespaced Migration; address it with migrate",
		)
	case *v1alpha1.ClusterMigration:
		if scope == clusterRecords {
			return current, backend, nil
		}

		return nil, "", domain.NewError(
			domain.ErrorValidation,
			"migration",
			"stored workflow is a ClusterMigration; address it with cluster-migrate",
		)
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
	family string,
	name string,
	phase v1alpha1.WorkflowPhase,
	cause error,
) error {
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Migration %s stopped in phase %s. Inspect with `%s %s status %s` before resume, rollback or cleanup.\n",
		name,
		phase,
		guidancePrefixesForCommand(cmd, "").pvcMigrate,
		workflowCommandPath(cmd, family),
		name,
	)

	return errors.Join(cause, err)
}

func (r *rootState) migrationExecutor(
	runtime *commandRuntime,
	backend string,
) (*app.MigrationExecutor, error) {
	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		r.migrationRecordNamespace(),
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
	executor, err := r.migrationExecutor(runtime, backend)
	if err != nil {
		return err
	}

	if dryRun {
		if err := executor.Validate(ctx, object); err != nil {
			return reportMigrationError(cmd, "migrate", object.Name, object.Status.Phase, err)
		}

		if err := runtime.printer.Print(object); err != nil {
			return err
		}

		return writeDryRunNotice(
			cmd.ErrOrStderr(),
			lifecycleExecuteCommand(
				cmd,
				guidancePrefixesForCommand(
					cmd,
					workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object),
				).pvcMigrate,
				"migrate",
				"resume",
				workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object),
				object.Name,
			),
		)
	}

	phase := copyResumePhase(object.Status.WorkflowStatus)
	if requiresResumeApproval(phase) ||
		requiresOperationResumeApproval(domain.OperationMigrate, phase) {
		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}
	}

	if err := executor.RequestResume(ctx, object); err != nil {
		return reportMigrationError(cmd, "migrate", object.Name, object.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportMigrationError(cmd, "migrate", object.Name, object.Status.Phase, err)
	}

	if err := runtime.printer.Print(object); err != nil {
		return err
	}

	return writeWorkflowNextSteps(
		cmd.ErrOrStderr(),
		cmd,
		guidancePrefixesForCommand(
			cmd,
			workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object),
		).pvcMigrate,
		"migrate",
		workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object),
		object.Name,
		object.Status.Phase,
		true,
	)
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
			return reportMigrationError(
				cmd,
				"cluster-migrate",
				object.Name,
				object.Status.Phase,
				err,
			)
		}

		if err := runtime.printer.Print(object); err != nil {
			return err
		}

		return writeDryRunNotice(
			cmd.ErrOrStderr(),
			lifecycleExecuteCommand(
				cmd,
				guidancePrefixesForCommand(
					cmd,
					clusterMigrationStorageNamespace(object),
				).pvcMigrate,
				"cluster-migrate",
				"resume",
				clusterMigrationStorageNamespace(object),
				object.Name,
			),
		)
	}

	phase := copyResumePhase(object.Status.WorkflowStatus)
	if requiresResumeApproval(phase) ||
		requiresOperationResumeApproval(domain.OperationMigrate, phase) {
		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}
	}

	if err := executor.RequestResume(ctx, object); err != nil {
		return reportMigrationError(cmd, "cluster-migrate", object.Name, object.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportMigrationError(cmd, "cluster-migrate", object.Name, object.Status.Phase, err)
	}

	if err := runtime.printer.Print(object); err != nil {
		return err
	}

	return writeWorkflowNextSteps(
		cmd.ErrOrStderr(),
		cmd,
		guidancePrefixesForCommand(cmd, clusterMigrationStorageNamespace(object)).pvcMigrate,
		"cluster-migrate",
		clusterMigrationStorageNamespace(object),
		object.Name,
		object.Status.Phase,
		true,
	)
}

func (r *rootState) resumeMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	dryRun bool,
	source workflowSource,
	scope recordScope,
) error {
	object, backend, err := r.loadMigrationWithBackend(ctx, cmd, runtime, id, source, scope)
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
	scope recordScope,
) error {
	object, backend, err := r.loadMigrationWithBackend(ctx, cmd, runtime, id, sourceSession, scope)
	if err != nil {
		return err
	}

	switch current := object.(type) {
	case *v1alpha1.Migration:
		executor, err := r.migrationExecutor(runtime, backend)
		if err != nil {
			return err
		}

		if err := executor.ValidateReservation(ctx, current); err != nil {
			return reportMigrationError(cmd, "migrate", current.Name, current.Status.Phase, err)
		}
	case *v1alpha1.ClusterMigration:
		executor, err := r.clusterMigrationExecutor(runtime, cmd, current, backend)
		if err != nil {
			return err
		}

		if err := executor.ValidateReservation(ctx, current); err != nil {
			return reportMigrationError(
				cmd,
				"cluster-migrate",
				current.Name,
				current.Status.Phase,
				err,
			)
		}
	}

	return runtime.printer.Print(object)
}
