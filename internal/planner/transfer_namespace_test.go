package planner

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTransferPlannerDefaultsMatchCRDNamespaces(t *testing.T) {
	for _, submission := range []bool{false, true} {
		client := plannerClient(plannerObjects("2Gi")...)
		p := New(
			client,
			controller.NewManager(client, nil, nil),
		).WithControllerSubmission(submission)

		checks := []struct {
			name      string
			plan      func() (*domain.TransferPlan, error)
			temporary string
		}{
			{"pod migration", func() (*domain.TransferPlan, error) {
				return p.PlanPodMigration(t.Context(), &v1alpha1.ClusterPodMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-migration"},
					Spec: v1alpha1.ClusterPodMigrationSpec{
						SourceNamespace: "app",
						PodMigrationSpec: v1alpha1.PodMigrationSpec{
							Pod: v1alpha1.LocalResourceReference{Name: "database"},
						},
					},
				}, "")
			}, "app"},
			{"migration", func() (*domain.TransferPlan, error) {
				return p.PlanOfflineMigration(t.Context(), &v1alpha1.ClusterMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "migration"},
					Spec: v1alpha1.ClusterMigrationSpec{
						SourceNamespace: "app",
						MigrationSpec: v1alpha1.MigrationSpec{
							Volumes: []v1alpha1.VolumeRequest{
								{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
							},
						},
					},
				}, "")
			}, "app"},
			{"copy", func() (*domain.TransferPlan, error) {
				return p.PlanCopy(t.Context(), &v1alpha1.ClusterCopy{
					ObjectMeta: metav1.ObjectMeta{Name: "copy"},
					Spec: v1alpha1.ClusterCopySpec{
						SourceNamespace:      "app",
						DestinationNamespace: "system",
						CopySpec: v1alpha1.CopySpec{
							Volumes: []v1alpha1.VolumeRequest{
								{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
							},
						},
					},
				}, "")
			}, "system"},
			{"reservation", func() (*domain.TransferPlan, error) {
				return p.PlanReserve(t.Context(), &v1alpha1.ClusterReservation{
					ObjectMeta: metav1.ObjectMeta{Name: "reservation"},
					Spec: v1alpha1.ClusterReservationSpec{
						SourceNamespace:      "app",
						DestinationNamespace: "system",
						ReservationSpec: v1alpha1.ReservationSpec{
							Volumes: []v1alpha1.VolumeRequest{
								{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
							},
						},
					},
				}, "")
			}, "system"},
		}
		for _, check := range checks {
			plan, err := check.plan()
			if err != nil {
				t.Fatalf("%s submission=%t: %v", check.name, submission, err)
			}

			if plan.SessionNamespace != "app" || plan.TemporaryNamespace != check.temporary {
				t.Fatalf(
					"%s submission=%t: session=%s temporary=%s",
					check.name,
					submission,
					plan.SessionNamespace,
					plan.TemporaryNamespace,
				)
			}
		}
	}
}
