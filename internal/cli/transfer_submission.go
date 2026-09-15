package cli

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func submitMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.ClusterMigration,
) error {
	spec := object.Spec.DeepCopy()
	if spec.TemporaryNamespace == "" {
		spec.TemporaryNamespace = spec.SourceNamespace
	}

	if spec.SessionNamespace == "" {
		spec.SessionNamespace = spec.SourceNamespace
	}

	if err := domain.ValidateReclaimPolicies(
		spec.SourcePVReclaimPolicy,
		spec.DestinationPVCReclaimPolicy,
	); err != nil {
		return err
	}

	namespaces := []string{
		string(spec.SourceNamespace),
		string(spec.TemporaryNamespace),
		string(spec.SessionNamespace),
	}
	if spec.SourceNamespace == spec.TemporaryNamespace &&
		spec.TemporaryNamespace == spec.SessionNamespace {
		workflow := &v1alpha1.Migration{
			ObjectMeta: metav1.ObjectMeta{
				Name:      object.Name,
				Namespace: string(spec.SourceNamespace),
			},
			Spec: spec.MigrationSpec,
		}

		return submitControllerObject(
			ctx,
			cmd,
			runtime,
			workflow,
			func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
			"Migration",
			"migrations",
			namespaces,
			domain.PhaseCompleted,
			func(current *v1alpha1.Migration) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
		)
	}

	workflow := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: object.Name},
		Spec:       *spec,
	}

	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		workflow,
		func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} },
		"ClusterMigration",
		"clustermigrations",
		namespaces,
		domain.PhaseCompleted,
		func(current *v1alpha1.ClusterMigration) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}

func submitPodMigration(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.ClusterPodMigration,
) error {
	spec := object.Spec.DeepCopy()
	if spec.TemporaryNamespace == "" {
		spec.TemporaryNamespace = spec.SourceNamespace
	}

	if spec.SessionNamespace == "" {
		spec.SessionNamespace = spec.SourceNamespace
	}

	if err := domain.ValidateReclaimPolicies(
		spec.SourcePVReclaimPolicy,
		spec.DestinationPVCReclaimPolicy,
	); err != nil {
		return err
	}

	namespaces := []string{
		string(spec.SourceNamespace),
		string(spec.TemporaryNamespace),
		string(spec.SessionNamespace),
	}
	if spec.SourceNamespace == spec.TemporaryNamespace &&
		spec.TemporaryNamespace == spec.SessionNamespace {
		workflow := &v1alpha1.PodMigration{
			ObjectMeta: metav1.ObjectMeta{
				Name:      object.Name,
				Namespace: string(spec.SourceNamespace),
			},
			Spec: spec.PodMigrationSpec,
		}

		return submitControllerObject(
			ctx,
			cmd,
			runtime,
			workflow,
			func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
			"PodMigration",
			"podmigrations",
			namespaces,
			domain.PhaseCompleted,
			func(current *v1alpha1.PodMigration) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
		)
	}

	workflow := &v1alpha1.ClusterPodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: object.Name},
		Spec:       *spec,
	}

	return submitControllerObject(ctx, cmd, runtime, workflow,
		func() *v1alpha1.ClusterPodMigration { return &v1alpha1.ClusterPodMigration{} },
		"ClusterPodMigration", "clusterpodmigrations", namespaces, domain.PhaseCompleted,
		func(current *v1alpha1.ClusterPodMigration) v1alpha1.WorkflowStatus {
			return current.Status.WorkflowStatus
		},
	)
}

func submitCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.ClusterCopy,
) error {
	spec := object.Spec.DeepCopy()
	if spec.DestinationNamespace == "" {
		spec.DestinationNamespace = spec.SourceNamespace
	}

	if spec.SessionNamespace == "" {
		spec.SessionNamespace = spec.SourceNamespace
	}

	if err := domain.ValidateReclaimPolicies("", spec.DestinationPVCReclaimPolicy); err != nil {
		return err
	}

	namespaces := []string{
		string(spec.SourceNamespace),
		string(spec.DestinationNamespace),
		string(spec.SessionNamespace),
	}
	if spec.SourceNamespace == spec.DestinationNamespace &&
		spec.DestinationNamespace == spec.SessionNamespace {
		workflow := &v1alpha1.Copy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      object.Name,
				Namespace: string(spec.SourceNamespace),
			},
			Spec: spec.CopySpec,
		}

		return submitControllerObject(
			ctx,
			cmd,
			runtime,
			workflow,
			func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
			"Copy",
			"copies",
			namespaces,
			domain.PhaseWarmCopied,
			func(current *v1alpha1.Copy) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
		)
	}

	workflow := &v1alpha1.ClusterCopy{ObjectMeta: metav1.ObjectMeta{Name: object.Name}, Spec: *spec}

	return submitControllerObject(
		ctx,
		cmd,
		runtime,
		workflow,
		func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
		"ClusterCopy",
		"clustercopies",
		namespaces,
		domain.PhaseWarmCopied,
		func(current *v1alpha1.ClusterCopy) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
	)
}

func submitReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.ClusterReservation,
) error {
	spec := object.Spec.DeepCopy()
	if spec.DestinationNamespace == "" {
		spec.DestinationNamespace = spec.SourceNamespace
	}

	if spec.SessionNamespace == "" {
		spec.SessionNamespace = spec.SourceNamespace
	}

	if err := domain.ValidateReclaimPolicies("", spec.DestinationPVCReclaimPolicy); err != nil {
		return err
	}

	namespaces := []string{
		string(spec.SourceNamespace),
		string(spec.DestinationNamespace),
		string(spec.SessionNamespace),
	}
	if spec.SourceNamespace == spec.DestinationNamespace &&
		spec.DestinationNamespace == spec.SessionNamespace {
		workflow := &v1alpha1.Reservation{
			ObjectMeta: metav1.ObjectMeta{
				Name:      object.Name,
				Namespace: string(spec.SourceNamespace),
			},
			Spec: spec.ReservationSpec,
		}

		return submitControllerObject(
			ctx,
			cmd,
			runtime,
			workflow,
			func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
			"Reservation",
			"reservations",
			namespaces,
			domain.PhaseReserved,
			func(current *v1alpha1.Reservation) v1alpha1.WorkflowStatus { return current.Status.WorkflowStatus },
		)
	}

	workflow := &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{Name: object.Name},
		Spec:       *spec,
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
