package planner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (p *Planner) checkActivationPVCPolicies(ctx context.Context, plan *domain.MigrationPlan, volumes []domain.VolumeSpec) {
	groups := make(map[string][]kube.PVCAdmissionChange)
	for _, volume := range volumes {
		requested, err := resource.ParseQuantity(volume.Capacity)
		if err != nil || requested.Sign() <= 0 {
			plan.AddCheck(failed("activation-policy", fmt.Sprintf("PVC %s/%s has invalid destination capacity %q", volume.SourcePVC.Namespace, volume.SourcePVC.Name, volume.Capacity)))
			continue
		}
		existing := volume.SourcePVCSpec.Resources.Requests[corev1.ResourceStorage]
		sourceClass := ""
		if volume.SourcePVCSpec.StorageClassName != nil {
			sourceClass = *volume.SourcePVCSpec.StorageClassName
		}
		groups[volume.SourcePVC.Namespace] = append(groups[volume.SourcePVC.Namespace], kube.PVCAdmissionChange{
			Namespace:             volume.SourcePVC.Namespace,
			Name:                  volume.SourcePVC.Name,
			RequestedStorage:      requested,
			RequestedStorageClass: volume.StorageClass,
			Existing:              true,
			ExistingStorage:       existing,
			ExistingStorageClass:  sourceClass,
		})
	}
	namespaces := make([]string, 0, len(groups))
	for namespace := range groups {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	for _, namespace := range namespaces {
		report, err := kube.CheckPVCAdmissionPolicies(ctx, p.client, groups[namespace])
		if err != nil {
			plan.AddCheck(failed("activation-policy", fmt.Sprintf("check application PVC admission in %s: %v", namespace, err)))
			continue
		}
		if len(report.QuotaViolations) > 0 {
			plan.AddCheck(failed("resource-quota", "activation PVC: "+strings.Join(report.QuotaViolations, "; ")))
		}
		if len(report.LimitRangeViolations) > 0 {
			plan.AddCheck(failed("limit-range", "activation PVC: "+strings.Join(report.LimitRangeViolations, "; ")))
		}
	}
}
