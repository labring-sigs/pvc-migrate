package app

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func validatePodMigrationSnapshot(
	namespace string,
	workload v1alpha1.WorkloadSpec,
	digest string,
) error {
	if workload.Adapter != v1alpha1.WorkloadStandalone {
		if digest != "" {
			return domain.NewError(domain.ErrorValidation, "pod migration",
				"standalone Pod snapshot digest does not belong to a controller-managed workload")
		}

		return nil
	}

	if workload.Pod == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"pod migration",
			"standalone Pod identity is required",
		)
	}

	return kube.VerifyStandalonePodSnapshot(
		qualifiedResourceReference(*workload.Pod, namespace), workload.OriginalObject, digest,
	)
}
