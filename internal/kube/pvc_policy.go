package kube

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// PVCAdmissionChange describes the net PVC replacement that will be admitted
// in one namespace. Existing is true while the old claim still contributes to
// namespace quota usage.
type PVCAdmissionChange struct {
	Namespace             string
	Name                  string
	RequestedStorage      resource.Quantity
	RequestedStorageClass string
	Existing              bool
	ExistingStorage       resource.Quantity
	ExistingStorageClass  string
}

// PVCAdmissionReport contains policy violations for a projected PVC
// replacement. The projection subtracts the old claim before adding the new
// claim, which matches activation after the source claim is deleted.
type PVCAdmissionReport struct {
	QuotaViolations      []string
	LimitRangeViolations []string
}

// CheckPVCAdmissionPolicies checks PVC-specific ResourceQuota and LimitRange
// constraints for a batch of replacements in one namespace.
func CheckPVCAdmissionPolicies(ctx context.Context, client kubernetes.Interface, changes []PVCAdmissionChange) (PVCAdmissionReport, error) {
	if client == nil {
		return PVCAdmissionReport{}, fmt.Errorf("kubernetes client is required")
	}
	if len(changes) == 0 {
		return PVCAdmissionReport{}, nil
	}
	namespace := strings.TrimSpace(changes[0].Namespace)
	if namespace == "" {
		return PVCAdmissionReport{}, fmt.Errorf("PVC namespace is required")
	}
	for _, change := range changes {
		if strings.TrimSpace(change.Namespace) != namespace || strings.TrimSpace(change.Name) == "" {
			return PVCAdmissionReport{}, fmt.Errorf("PVC admission changes must identify one namespace and a name for every claim")
		}
		if change.RequestedStorage.Sign() <= 0 {
			return PVCAdmissionReport{}, fmt.Errorf("PVC %s/%s requested storage must be positive", namespace, change.Name)
		}
	}
	quotas, err := client.CoreV1().ResourceQuotas(namespace).List(ctx, metav1.ListOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return PVCAdmissionReport{}, err
	}
	limitRanges, limitRangeErr := client.CoreV1().LimitRanges(namespace).List(ctx, metav1.ListOptions{})
	if limitRangeErr != nil && !apierrors.IsNotFound(limitRangeErr) {
		return PVCAdmissionReport{}, limitRangeErr
	}
	report := PVCAdmissionReport{}
	if quotas != nil {
		for _, quota := range quotas.Items {
			for resourceName, hard := range quota.Spec.Hard {
				projected, ok := projectedQuotaResource(resourceName, quota.Status.Used[resourceName], changes)
				if !ok {
					continue
				}
				if projected.Cmp(hard) > 0 {
					report.QuotaViolations = append(report.QuotaViolations, fmt.Sprintf("%s/%s %s: projected %s exceeds hard %s", namespace, quota.Name, resourceName, projected.String(), hard.String()))
				}
			}
		}
	}
	if limitRanges != nil {
		for _, limitRange := range limitRanges.Items {
			for _, item := range limitRange.Spec.Limits {
				if item.Type != corev1.LimitTypePersistentVolumeClaim {
					continue
				}
				for _, change := range changes {
					if minimum, ok := item.Min[corev1.ResourceStorage]; ok && change.RequestedStorage.Cmp(minimum) < 0 {
						report.LimitRangeViolations = append(report.LimitRangeViolations, fmt.Sprintf("%s/%s requires %s below %s minimum %s", namespace, change.Name, change.RequestedStorage.String(), limitRange.Name, minimum.String()))
					}
					if maximum, ok := item.Max[corev1.ResourceStorage]; ok && change.RequestedStorage.Cmp(maximum) > 0 {
						report.LimitRangeViolations = append(report.LimitRangeViolations, fmt.Sprintf("%s/%s requires %s above %s maximum %s", namespace, change.Name, change.RequestedStorage.String(), limitRange.Name, maximum.String()))
					}
				}
			}
		}
	}
	sort.Strings(report.QuotaViolations)
	sort.Strings(report.LimitRangeViolations)
	return report, nil
}

func projectedQuotaResource(resourceName corev1.ResourceName, used resource.Quantity, changes []PVCAdmissionChange) (resource.Quantity, bool) {
	projected := used.DeepCopy()
	name := string(resourceName)
	switch resourceName {
	case corev1.ResourceRequestsStorage:
		for _, change := range changes {
			projected.Add(change.RequestedStorage)
			if change.Existing {
				projected.Sub(change.ExistingStorage)
			}
		}
		return clampNonNegative(projected), true
	case corev1.ResourcePersistentVolumeClaims, corev1.ResourceName("count/persistentvolumeclaims"):
		for _, change := range changes {
			if !change.Existing {
				projected.Add(*resource.NewQuantity(1, resource.DecimalSI))
			}
		}
		return clampNonNegative(projected), true
	}
	const classPrefix = ".storageclass.storage.k8s.io/"
	class, suffix, ok := strings.Cut(name, classPrefix)
	if !ok {
		return resource.Quantity{}, false
	}
	var delta resource.Quantity
	for _, change := range changes {
		if change.Existing && (class == "" || change.ExistingStorageClass == class) {
			delta.Sub(change.ExistingStorage)
		}
		if class == "" || change.RequestedStorageClass == class {
			delta.Add(change.RequestedStorage)
		}
	}
	if suffix == "requests.storage" {
		projected.Add(delta)
		return clampNonNegative(projected), true
	}
	if suffix == "persistentvolumeclaims" {
		if class == "" {
			for _, change := range changes {
				if !change.Existing {
					projected.Add(*resource.NewQuantity(1, resource.DecimalSI))
				}
			}
			return clampNonNegative(projected), true
		}
		for _, change := range changes {
			if change.Existing && change.ExistingStorageClass == class && change.RequestedStorageClass != class {
				projected.Sub(*resource.NewQuantity(1, resource.DecimalSI))
			}
			if change.RequestedStorageClass == class && (!change.Existing || change.ExistingStorageClass != class) {
				projected.Add(*resource.NewQuantity(1, resource.DecimalSI))
			}
		}
		return clampNonNegative(projected), true
	}
	return resource.Quantity{}, false
}

func clampNonNegative(value resource.Quantity) resource.Quantity {
	if value.Sign() < 0 {
		return *resource.NewQuantity(0, resource.DecimalSI)
	}
	return value
}
