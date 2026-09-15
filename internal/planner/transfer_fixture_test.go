package planner

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Existing storage fixtures exercise the same concrete entrypoints as callers.
func (p *Planner) plan(ctx context.Context, input planOptions) (*domain.TransferPlan, error) {
	options := applyDefaults(input)
	transfer := options.TransferOptions
	transfer.Strategies = input.Strategies

	volumes := options.Volumes

	metadata := metav1.ObjectMeta{Name: options.SessionID}
	source := v1alpha1.NamespaceName(options.SourceNamespace)
	destination := v1alpha1.NamespaceName(options.DestinationNamespace)
	temporary := v1alpha1.NamespaceName(options.TemporaryNamespace)
	session := v1alpha1.NamespaceName(options.SessionNamespace)

	switch options.operation() {
	case domain.OperationMigrate:
		return p.PlanOfflineMigration(ctx, &v1alpha1.ClusterMigration{
			ObjectMeta: metadata,
			Spec: v1alpha1.ClusterMigrationSpec{
				SourceNamespace: source, TemporaryNamespace: temporary, SessionNamespace: session,
				MigrationSpec: v1alpha1.MigrationSpec{
					TransferOptions: transfer, Volumes: volumes,
				},
			},
		}, options.ToolImage)
	case domain.OperationCopy:
		return p.PlanCopy(ctx, &v1alpha1.ClusterCopy{
			ObjectMeta: metadata,
			Spec: v1alpha1.ClusterCopySpec{
				SourceNamespace:      source,
				DestinationNamespace: destination,
				SessionNamespace:     session,
				CopySpec: v1alpha1.CopySpec{
					TransferOptions: transfer,
					Volumes:         volumes,
				},
			},
		}, options.ToolImage)
	case domain.OperationReserve:
		return p.PlanReserve(ctx, &v1alpha1.ClusterReservation{
			ObjectMeta: metadata,
			Spec: v1alpha1.ClusterReservationSpec{
				SourceNamespace:      source,
				DestinationNamespace: destination,
				SessionNamespace:     session,
				ReservationSpec: v1alpha1.ReservationSpec{
					TransferOptions: transfer,
					Volumes:         volumes,
				},
			},
		}, options.ToolImage)
	default:
		return nil, domain.NewError(domain.ErrorInternal, "test fixture", "unsupported operation")
	}
}

func testSourceVolumes(names ...string) []v1alpha1.VolumeRequest {
	volumes := make([]v1alpha1.VolumeRequest, 0, len(names))
	for _, name := range names {
		volumes = append(volumes, v1alpha1.VolumeRequest{
			SourcePVC: v1alpha1.LocalResourceReference{Name: name},
		})
	}

	return volumes
}
