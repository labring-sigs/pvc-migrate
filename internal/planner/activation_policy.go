package planner

import (
	"context"
	"fmt"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (p *Planner) checkActivationPVCPolicies(
	ctx context.Context,
	plan checkRecorder,
	namespace string,
	volumes []v1alpha1.VolumeSpec,
) {
	changes := make([]kube.PVCAdmissionChange, 0, len(volumes))
	for _, volume := range volumes {
		requested, err := resource.ParseQuantity(volume.Capacity)
		if err != nil || requested.Sign() <= 0 {
			plan.AddCheck(
				failed(
					domain.CheckNameActivationPolicy,
					fmt.Sprintf(
						"PVC %s/%s has invalid destination capacity %q",
						namespace,
						volume.SourcePVC.Name,
						volume.Capacity,
					),
				),
			)

			continue
		}

		existing := volume.SourcePVCSpec.Resources.Requests[corev1.ResourceStorage]

		sourceClass := ""
		if volume.SourcePVCSpec.StorageClassName != nil {
			sourceClass = *volume.SourcePVCSpec.StorageClassName
		}

		volumeAttributesClasses := kube.RequestedVolumeAttributesClassNames(volume.SourcePVCSpec)

		changes = append(
			changes,
			kube.PVCAdmissionChange{
				Namespace:                           namespace,
				Name:                                volume.SourcePVC.Name,
				RequestedStorage:                    requested,
				RequestedStorageClass:               volume.StorageClass,
				Existing:                            true,
				ExistingUID:                         volume.SourcePVC.UID,
				ExistingStorage:                     existing,
				ExistingStorageClass:                sourceClass,
				RequestedVolumeAttributesClassNames: volumeAttributesClasses,
				ExistingVolumeAttributesClassNames:  volumeAttributesClasses,
			},
		)
	}

	report, err := kube.CheckPVCAdmissionPolicies(ctx, p.client, changes)
	if err != nil {
		plan.AddCheck(
			failed(
				domain.CheckNameActivationPolicy,
				fmt.Sprintf("check application PVC admission in %s: %v", namespace, err),
			),
		)

		return
	}

	if len(report.QuotaViolations) > 0 {
		plan.AddCheck(
			failed(
				domain.CheckNameResourceQuota,
				"activation PVC: "+strings.Join(report.QuotaViolations, "; "),
			),
		)
	}

	if len(report.LimitRangeViolations) > 0 {
		plan.AddCheck(
			failed(
				domain.CheckNameLimitRange,
				"activation PVC: "+strings.Join(report.LimitRangeViolations, "; "),
			),
		)
	}
}
