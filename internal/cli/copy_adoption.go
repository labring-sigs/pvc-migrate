package cli

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
)

// adoptCRDReservation hands a controller-owned Reservation CR to its same-named
// Copy CR under the reservation's storage fence. The handoff persists the
// planned Copy CR with pending-handoff annotations; the elected controller
// finishes the pair and executes the copy, so the CLI must not run it locally.
func (r *rootState) adoptCRDReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	source *v1alpha1.Reservation,
	flags *copyFlags,
	dryRun bool,
) error {
	spec := app.NamespacedCopySpecFromReservation(source.Spec)
	if err := applyCopyOverrides(cmd, &spec, flags); err != nil {
		return err
	}

	reservationStore, err := cliCRDWorkflowStore(
		runtime,
		func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
	)
	if err != nil {
		return err
	}

	copyStore, err := cliCRDWorkflowStore(
		runtime,
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		return err
	}

	executor := app.NewCopyExecutor(
		runtime.clients.Kubernetes,
		copyStore,
		cliWorkflowLockerForBackend(runtime, backendCRD),
		copyengine.NewPVMigrate(),
		r.copyConfig(runtime),
	)

	if dryRun {
		preview, err := app.NamespacedCopyFromReservation(source, spec)
		if err != nil {
			return err
		}

		if err := executor.Validate(ctx, preview); err != nil {
			return reportCopyError(cmd, preview.Name, preview.Status.Phase, err)
		}

		return printCopyDryRunResult(cmd, runtime, preview, source.Namespace)
	}

	object, err := executor.AdoptReservation(
		ctx,
		reservationStore,
		source,
		spec,
		func(ctx context.Context, reservation *v1alpha1.Reservation, destination *v1alpha1.Copy) error {
			return kube.NamespacedHandoffCRDReservationToCopy(
				ctx,
				runtime.clients.Runtime,
				reservation,
				destination,
			)
		},
	)
	if err != nil {
		return reportCopyError(cmd, source.Name, source.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

// adoptCRDClusterReservation is the cluster-scoped variant of adoptCRDReservation.
func (r *rootState) adoptCRDClusterReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	source *v1alpha1.ClusterReservation,
	flags *copyFlags,
	dryRun bool,
) error {
	spec := app.CopySpecFromReservation(source.Spec)
	if err := applyCopyOverrides(cmd, &spec.CopySpec, flags); err != nil {
		return err
	}

	reservationStore, err := cliCRDWorkflowStore(
		runtime,
		func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
	)
	if err != nil {
		return err
	}

	copyStore, err := cliCRDWorkflowStore(
		runtime,
		func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
	)
	if err != nil {
		return err
	}

	lockNamespace := string(source.Spec.SessionNamespace)
	if lockNamespace == "" {
		lockNamespace = string(source.Spec.SourceNamespace)
	}

	executor := app.NewClusterCopyExecutor(
		runtime.clients.Kubernetes,
		copyStore,
		cliWorkflowLockerForBackend(runtime, backendCRD),
		lockNamespace,
		copyengine.NewPVMigrate(),
		r.copyConfig(runtime),
	)

	if dryRun {
		preview, err := app.CopyFromReservation(source, spec)
		if err != nil {
			return err
		}

		if err := executor.Validate(ctx, preview); err != nil {
			return reportCopyError(cmd, preview.Name, preview.Status.Phase, err)
		}

		return printCopyDryRunResult(
			cmd,
			runtime,
			preview,
			lockNamespace,
		)
	}

	object, err := executor.AdoptReservation(
		ctx,
		reservationStore,
		source,
		spec,
		func(ctx context.Context, reservation *v1alpha1.ClusterReservation, destination *v1alpha1.ClusterCopy) error {
			return kube.HandoffCRDReservationToCopy(
				ctx,
				runtime.clients.Runtime,
				reservation,
				destination,
			)
		},
	)
	if err != nil {
		return reportCopyError(cmd, source.Name, source.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

func (r *rootState) adoptReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	source *v1alpha1.Reservation,
	flags *copyFlags,
	dryRun bool,
	backend string,
) error {
	if backend == backendCRD {
		return r.adoptCRDReservation(ctx, cmd, runtime, source, flags, dryRun)
	}

	namespace := r.workflowStorageNamespace(cmd)

	spec := app.NamespacedCopySpecFromReservation(source.Spec)
	if err := applyCopyOverrides(cmd, &spec, flags); err != nil {
		return err
	}

	store, err := cliWorkflowStore(
		runtime,
		namespace,
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		return err
	}

	executor := app.NewCopyExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLockerForBackend(runtime, backend),
		copyengine.NewPVMigrate(),
		r.copyConfig(runtime),
	)
	if dryRun {
		preview, err := app.NamespacedCopyFromReservation(source, spec)
		if err != nil {
			return err
		}

		if err := executor.Validate(ctx, preview); err != nil {
			return reportCopyError(cmd, preview.Name, preview.Status.Phase, err)
		}

		return printCopyDryRunResult(cmd, runtime, preview, namespace)
	}

	sourceStore, err := cliWorkflowStore(
		runtime,
		namespace,
		func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
	)
	if err != nil {
		return err
	}

	object, err := executor.AdoptReservation(
		ctx,
		sourceStore,
		source,
		spec,
		func(ctx context.Context, reservation *v1alpha1.Reservation, destination *v1alpha1.Copy) error {
			return kube.NamespacedHandoffConfigMapReservationToCopy(
				ctx,
				runtime.clients.Kubernetes,
				reservation,
				destination,
			)
		},
	)
	if err != nil {
		return reportCopyError(cmd, source.Name, source.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportCopyError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

func (r *rootState) adoptClusterReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	source *v1alpha1.ClusterReservation,
	flags *copyFlags,
	dryRun bool,
	backend string,
) error {
	if backend == backendCRD {
		return r.adoptCRDClusterReservation(ctx, cmd, runtime, source, flags, dryRun)
	}

	namespace := r.workflowStorageNamespace(cmd)

	spec := app.CopySpecFromReservation(source.Spec)
	if err := applyCopyOverrides(cmd, &spec.CopySpec, flags); err != nil {
		return err
	}

	store, err := cliWorkflowStore(
		runtime,
		namespace,
		func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
	)
	if err != nil {
		return err
	}

	lockNamespace := string(source.Spec.SessionNamespace)
	if lockNamespace == "" {
		lockNamespace = string(source.Spec.SourceNamespace)
	}

	executor := app.NewClusterCopyExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLockerForBackend(runtime, backend),
		lockNamespace,
		copyengine.NewPVMigrate(),
		r.copyConfig(runtime),
	)
	if dryRun {
		preview, err := app.CopyFromReservation(source, spec)
		if err != nil {
			return err
		}

		if err := executor.Validate(ctx, preview); err != nil {
			return reportCopyError(cmd, preview.Name, preview.Status.Phase, err)
		}

		return printCopyDryRunResult(cmd, runtime, preview, namespace)
	}

	sourceStore, err := cliWorkflowStore(
		runtime,
		namespace,
		func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
	)
	if err != nil {
		return err
	}

	object, err := executor.AdoptReservation(
		ctx,
		sourceStore,
		source,
		spec,
		func(ctx context.Context, reservation *v1alpha1.ClusterReservation, destination *v1alpha1.ClusterCopy) error {
			return kube.HandoffConfigMapReservationToCopy(
				ctx,
				runtime.clients.Kubernetes,
				namespace,
				reservation,
				destination,
			)
		},
	)
	if err != nil {
		return reportCopyError(cmd, source.Name, source.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportCopyError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}
