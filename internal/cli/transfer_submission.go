package cli

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

// applyClusterMigrationDefaults fills the namespace defaults the controller
// contract expects: every unset namespace collapses to the source namespace.
func applyClusterMigrationDefaults(spec *v1alpha1.ClusterMigrationSpec) {
	if spec.TemporaryNamespace == "" {
		spec.TemporaryNamespace = spec.SourceNamespace
	}

	if spec.SessionNamespace == "" {
		spec.SessionNamespace = spec.SourceNamespace
	}

	if spec.DestinationNamespace == "" {
		spec.DestinationNamespace = spec.SourceNamespace
	}
}

// submitMigration submits the namespaced Migration a create command
// assembled; metadata.namespace is the workflow's whole namespace story.
func submitMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	workflow *v1alpha1.Migration,
) error {
	if err := domain.ValidateUnusedStoragePolicy(workflow.Spec.UnusedStoragePolicy); err != nil {
		return err
	}

	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		workflow,
		func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
		"Migration",
		"migrations",
		[]string{workflow.Namespace},
		domain.PhaseCompleted,
		func(current *v1alpha1.Migration) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}

// submitClusterMigration submits the cluster-scoped variant explicitly.
func submitClusterMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	workflow *v1alpha1.ClusterMigration,
) error {
	spec := workflow.Spec.DeepCopy()
	applyClusterMigrationDefaults(spec)
	workflow.Spec = *spec

	if err := domain.ValidateUnusedStoragePolicy(spec.UnusedStoragePolicy); err != nil {
		return err
	}

	namespaces := []string{
		string(spec.SourceNamespace),
		string(spec.DestinationNamespace),
		string(spec.TemporaryNamespace),
		string(spec.SessionNamespace),
	}

	return submitControllerObject(ctx, cmd, runtime, workflow,
		func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} },
		"ClusterMigration", "clustermigrations", namespaces, domain.PhaseCompleted,
		func(current *v1alpha1.ClusterMigration) v1alpha1.WorkflowStatus {
			return current.Status.WorkflowStatus
		},
	)
}

func submitPodMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.PodMigration,
) error {
	if err := domain.ValidateUnusedStoragePolicy(object.Spec.UnusedStoragePolicy); err != nil {
		return err
	}

	workflow := object.DeepCopy()

	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		workflow,
		func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
		"PodMigration",
		"podmigrations",
		[]string{object.Namespace},
		domain.PhaseCompleted,
		func(current *v1alpha1.PodMigration) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}

// applyClusterCopyDefaults fills the namespace defaults the copy controller
// contract expects.
func applyClusterCopyDefaults(spec *v1alpha1.ClusterCopySpec) {
	if spec.DestinationNamespace == "" {
		spec.DestinationNamespace = spec.SourceNamespace
	}

	if spec.SessionNamespace == "" {
		spec.SessionNamespace = spec.SourceNamespace
	}
}

// submitCopy submits the namespaced Copy a create command assembled;
// metadata.namespace is the workflow's whole namespace story.
func submitCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	workflow *v1alpha1.Copy,
) error {
	if err := domain.ValidateUnusedStoragePolicy(workflow.Spec.UnusedStoragePolicy); err != nil {
		return err
	}

	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		workflow,
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
		"Copy",
		"copies",
		[]string{workflow.Namespace},
		domain.PhaseWarmCopied,
		func(current *v1alpha1.Copy) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}

// submitClusterCopy submits the cluster-scoped variant explicitly.
func submitClusterCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	workflow *v1alpha1.ClusterCopy,
) error {
	spec := workflow.Spec.DeepCopy()
	applyClusterCopyDefaults(spec)
	workflow.Spec = *spec

	if err := domain.ValidateUnusedStoragePolicy(spec.UnusedStoragePolicy); err != nil {
		return err
	}

	namespaces := []string{
		string(spec.SourceNamespace),
		string(spec.DestinationNamespace),
		string(spec.SessionNamespace),
	}

	return submitControllerObject(ctx, cmd, runtime, workflow,
		func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
		"ClusterCopy", "clustercopies", namespaces, domain.PhaseWarmCopied,
		func(current *v1alpha1.ClusterCopy) v1alpha1.WorkflowStatus {
			return current.Status.WorkflowStatus
		},
	)
}

// applyClusterReservationDefaults fills the namespace defaults the
// reservation controller contract expects.
func applyClusterReservationDefaults(spec *v1alpha1.ClusterReservationSpec) {
	if spec.DestinationNamespace == "" {
		spec.DestinationNamespace = spec.SourceNamespace
	}

	if spec.SessionNamespace == "" {
		spec.SessionNamespace = spec.SourceNamespace
	}
}

// submitReservation submits the namespaced Reservation a create command
// assembled; metadata.namespace is the workflow's whole namespace story.
func submitReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	workflow *v1alpha1.Reservation,
) error {
	if err := domain.ValidateUnusedStoragePolicy(workflow.Spec.UnusedStoragePolicy); err != nil {
		return err
	}

	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		workflow,
		func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
		"Reservation",
		"reservations",
		[]string{workflow.Namespace},
		domain.PhaseReserved,
		func(current *v1alpha1.Reservation) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}

// submitClusterReservation submits the cluster-scoped variant explicitly.
func submitClusterReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	workflow *v1alpha1.ClusterReservation,
) error {
	spec := workflow.Spec.DeepCopy()
	applyClusterReservationDefaults(spec)
	workflow.Spec = *spec

	if err := domain.ValidateUnusedStoragePolicy(spec.UnusedStoragePolicy); err != nil {
		return err
	}

	namespaces := []string{
		string(spec.SourceNamespace),
		string(spec.DestinationNamespace),
		string(spec.SessionNamespace),
	}

	return submitControllerObject(ctx, cmd, runtime, workflow,
		func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
		"ClusterReservation", "clusterreservations", namespaces, domain.PhaseReserved,
		func(current *v1alpha1.ClusterReservation) v1alpha1.WorkflowStatus {
			return current.Status.WorkflowStatus
		},
	)
}

func submitRename(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.Rename,
) error {
	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		object,
		func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
		"Rename",
		"renames",
		[]string{object.Namespace},
		domain.PhaseCompleted,
		func(current *v1alpha1.Rename) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}

func submitMove(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.Move,
) error {
	spec := object.Spec
	namespaces := []string{
		string(spec.SourceNamespace),
		string(spec.DestinationNamespace),
		string(spec.SessionNamespace),
	}

	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		object,
		func() *v1alpha1.Move { return &v1alpha1.Move{} },
		"Move",
		"moves",
		namespaces,
		domain.PhaseCompleted,
		func(current *v1alpha1.Move) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}
