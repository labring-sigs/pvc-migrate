package kube

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	ExistingUID           types.UID
	ExistingStorage       resource.Quantity
	ExistingStorageClass  string
	// These sets include every VAC name that can make the respective PVC
	// match a VolumeAttributesClass-scoped quota.
	RequestedVolumeAttributesClassNames []string
	ExistingVolumeAttributesClassNames  []string
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
func CheckPVCAdmissionPolicies(
	ctx context.Context,
	client kubernetes.Interface,
	changes []PVCAdmissionChange,
) (PVCAdmissionReport, error) {
	if client == nil {
		return PVCAdmissionReport{}, errors.New("kubernetes client is required")
	}

	if len(changes) == 0 {
		return PVCAdmissionReport{}, nil
	}

	namespace := strings.TrimSpace(changes[0].Namespace)
	if namespace == "" {
		return PVCAdmissionReport{}, errors.New("PVC namespace is required")
	}

	for _, change := range changes {
		if strings.TrimSpace(change.Namespace) != namespace ||
			strings.TrimSpace(change.Name) == "" {
			return PVCAdmissionReport{}, errors.New(
				"PVC admission changes must identify one namespace and a name for every claim",
			)
		}

		if change.RequestedStorage.Sign() <= 0 {
			return PVCAdmissionReport{}, fmt.Errorf(
				"PVC %s/%s requested storage must be positive",
				namespace,
				change.Name,
			)
		}
	}

	policies := ReadNamespaceResourcePolicies(ctx, client, namespace, true)
	if policies.ResourceQuotaErr != nil && !apierrors.IsNotFound(policies.ResourceQuotaErr) {
		return PVCAdmissionReport{}, policies.ResourceQuotaErr
	}

	if policies.LimitRangeErr != nil && !apierrors.IsNotFound(policies.LimitRangeErr) {
		return PVCAdmissionReport{}, policies.LimitRangeErr
	}

	report := PVCAdmissionReport{}
	if policies.ResourceQuotas != nil {
		resolved, err := resolveExistingPVCAdmissionState(
			ctx,
			client,
			policies.ResourceQuotas.Items,
			changes,
		)
		if err != nil {
			return PVCAdmissionReport{}, err
		}

		changes = resolved

		for _, quota := range policies.ResourceQuotas.Items {
			if !quotaCanMatchRequestedPVC(quota, changes) {
				continue
			}

			for resourceName, hard := range quota.Status.Hard {
				if !isPVCQuotaResource(resourceName) {
					continue
				}

				used, known := quota.Status.Used[resourceName]
				if !known {
					report.QuotaViolations = append(
						report.QuotaViolations,
						fmt.Sprintf(
							"%s/%s %s: quota status has no current usage",
							namespace,
							quota.Name,
							resourceName,
						),
					)

					continue
				}

				projected, ok := projectedQuotaResource(
					resourceName,
					used,
					quota,
					changes,
				)
				if !ok {
					continue
				}

				if projected.Cmp(hard) > 0 {
					report.QuotaViolations = append(
						report.QuotaViolations,
						fmt.Sprintf(
							"%s/%s %s: projected %s exceeds hard %s",
							namespace,
							quota.Name,
							resourceName,
							projected.String(),
							hard.String(),
						),
					)
				}
			}
		}
	}

	if policies.LimitRanges != nil {
		for _, limitRange := range policies.LimitRanges.Items {
			for _, item := range limitRange.Spec.Limits {
				if item.Type != corev1.LimitTypePersistentVolumeClaim {
					continue
				}

				for _, change := range changes {
					if minimum, ok := item.Min[corev1.ResourceStorage]; ok &&
						change.RequestedStorage.Cmp(minimum) < 0 {
						report.LimitRangeViolations = append(
							report.LimitRangeViolations,
							fmt.Sprintf(
								"%s/%s requires %s below %s minimum %s",
								namespace,
								change.Name,
								change.RequestedStorage.String(),
								limitRange.Name,
								minimum.String(),
							),
						)
					}

					if maximum, ok := item.Max[corev1.ResourceStorage]; ok &&
						change.RequestedStorage.Cmp(maximum) > 0 {
						report.LimitRangeViolations = append(
							report.LimitRangeViolations,
							fmt.Sprintf(
								"%s/%s requires %s above %s maximum %s",
								namespace,
								change.Name,
								change.RequestedStorage.String(),
								limitRange.Name,
								maximum.String(),
							),
						)
					}
				}
			}
		}
	}

	sort.Strings(report.QuotaViolations)
	sort.Strings(report.LimitRangeViolations)

	return report, nil
}

func resolveExistingPVCAdmissionState(
	ctx context.Context,
	client kubernetes.Interface,
	quotas []corev1.ResourceQuota,
	changes []PVCAdmissionChange,
) ([]PVCAdmissionChange, error) {
	resolveVolumeAttributesClasses := hasVolumeAttributesClassQuota(quotas)

	resolveStorage := hasPVCStorageQuota(quotas)
	if !resolveVolumeAttributesClasses && !resolveStorage {
		return changes, nil
	}

	resolved := append([]PVCAdmissionChange(nil), changes...)
	errors := make([]error, len(resolved))
	parallel.For(len(resolved), func(index int) {
		change := &resolved[index]
		change.RequestedVolumeAttributesClassNames = uniqueNonEmptyStrings(
			change.RequestedVolumeAttributesClassNames,
		)

		change.ExistingVolumeAttributesClassNames = uniqueNonEmptyStrings(
			change.ExistingVolumeAttributesClassNames,
		)
		if !change.Existing ||
			(change.ExistingUID == "" && !resolveVolumeAttributesClasses) {
			return
		}

		pvc, err := client.CoreV1().PersistentVolumeClaims(change.Namespace).Get(
			ctx,
			change.Name,
			metav1.GetOptions{},
		)
		if err != nil {
			errors[index] = fmt.Errorf(
				"read existing PVC %s/%s for quota projection: %w",
				change.Namespace,
				change.Name,
				err,
			)

			return
		}

		if change.ExistingUID != "" && pvc.UID != change.ExistingUID {
			errors[index] = fmt.Errorf(
				"existing PVC %s/%s UID changed while checking quota",
				change.Namespace,
				change.Name,
			)

			return
		}

		if resolveStorage {
			effectiveStorage := effectivePVCQuotaStorage(pvc)
			if effectiveStorage.Cmp(change.ExistingStorage) > 0 {
				change.ExistingStorage = effectiveStorage
			}
		}

		if resolveVolumeAttributesClasses {
			change.ExistingVolumeAttributesClassNames = referencedVolumeAttributesClassNames(pvc)
		}
	})

	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	return resolved, nil
}

func hasPVCStorageQuota(quotas []corev1.ResourceQuota) bool {
	for _, quota := range quotas {
		for resourceName := range quota.Status.Hard {
			_, scopedResource, storageClassScoped := splitStorageClassQuotaResourceName(
				resourceName,
			)
			if resourceName == corev1.ResourceRequestsStorage ||
				(storageClassScoped && scopedResource == corev1.ResourceRequestsStorage) {
				return true
			}
		}
	}

	return false
}

func effectivePVCQuotaStorage(pvc *corev1.PersistentVolumeClaim) resource.Quantity {
	requested := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

	allocated := pvc.Status.AllocatedResources[corev1.ResourceStorage]
	if allocated.Cmp(requested) > 0 {
		return allocated.DeepCopy()
	}

	return requested.DeepCopy()
}

func hasVolumeAttributesClassQuota(quotas []corev1.ResourceQuota) bool {
	for _, quota := range quotas {
		if slices.Contains(quota.Spec.Scopes, corev1.ResourceQuotaScopeVolumeAttributesClass) {
			return true
		}

		if quota.Spec.ScopeSelector != nil {
			for _, selector := range quota.Spec.ScopeSelector.MatchExpressions {
				if selector.ScopeName == corev1.ResourceQuotaScopeVolumeAttributesClass {
					return true
				}
			}
		}
	}

	return false
}

func quotaCanMatchRequestedPVC(
	quota corev1.ResourceQuota,
	changes []PVCAdmissionChange,
) bool {
	for _, change := range changes {
		if pvcMatchesQuotaScopes(quota, change.RequestedVolumeAttributesClassNames) {
			return true
		}
	}

	return false
}

func pvcMatchesQuotaScopes(quota corev1.ResourceQuota, classNames []string) bool {
	for _, scope := range quota.Spec.Scopes {
		if !pvcMatchesScope(corev1.ScopedResourceSelectorRequirement{
			ScopeName: scope,
			Operator:  corev1.ScopeSelectorOpExists,
		}, classNames) {
			return false
		}
	}

	if quota.Spec.ScopeSelector != nil {
		for _, selector := range quota.Spec.ScopeSelector.MatchExpressions {
			if !pvcMatchesScope(selector, classNames) {
				return false
			}
		}
	}

	return true
}

func pvcMatchesScope(
	selector corev1.ScopedResourceSelectorRequirement,
	classNames []string,
) bool {
	if selector.ScopeName != corev1.ResourceQuotaScopeVolumeAttributesClass {
		return false
	}

	values := make(map[string]struct{}, len(selector.Values))
	for _, value := range selector.Values {
		values[value] = struct{}{}
	}

	switch selector.Operator {
	case corev1.ScopeSelectorOpExists:
		return len(classNames) > 0
	case corev1.ScopeSelectorOpDoesNotExist:
		return len(classNames) == 0
	case corev1.ScopeSelectorOpIn:
		for _, name := range classNames {
			if _, exists := values[name]; exists {
				return true
			}
		}
	case corev1.ScopeSelectorOpNotIn:
		if len(classNames) == 0 {
			return true
		}

		for _, name := range classNames {
			if _, exists := values[name]; !exists {
				return true
			}
		}
	}

	return false
}

func referencedVolumeAttributesClassNames(pvc *corev1.PersistentVolumeClaim) []string {
	if pvc == nil {
		return nil
	}

	names := make([]string, 0, 3)
	if pvc.Spec.VolumeAttributesClassName != nil {
		names = append(names, *pvc.Spec.VolumeAttributesClassName)
	}

	if pvc.Status.CurrentVolumeAttributesClassName != nil {
		names = append(names, *pvc.Status.CurrentVolumeAttributesClassName)
	}

	if pvc.Status.ModifyVolumeStatus != nil {
		names = append(names, pvc.Status.ModifyVolumeStatus.TargetVolumeAttributesClassName)
	}

	return uniqueNonEmptyStrings(names)
}

// RequestedVolumeAttributesClassNames returns the VAC names referenced by a
// newly created PVC. Current and target names only exist on admitted objects.
func RequestedVolumeAttributesClassNames(spec corev1.PersistentVolumeClaimSpec) []string {
	if spec.VolumeAttributesClassName == nil || *spec.VolumeAttributesClassName == "" {
		return nil
	}

	return []string{*spec.VolumeAttributesClassName}
}

func uniqueNonEmptyStrings(values []string) []string {
	result := make([]string, 0, len(values))

	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}

		if _, exists := seen[value]; exists {
			continue
		}

		seen[value] = struct{}{}
		result = append(result, value)
	}

	return result
}

func isPVCQuotaResource(resourceName corev1.ResourceName) bool {
	switch resourceName {
	case corev1.ResourceRequestsStorage,
		corev1.ResourcePersistentVolumeClaims,
		countPersistentVolumeClaimsResource:
		return true
	}

	_, suffix, ok := splitStorageClassQuotaResourceName(resourceName)

	return ok && (suffix == corev1.ResourceRequestsStorage ||
		suffix == corev1.ResourcePersistentVolumeClaims)
}

func projectedQuotaResource(
	resourceName corev1.ResourceName,
	used resource.Quantity,
	quota corev1.ResourceQuota,
	changes []PVCAdmissionChange,
) (resource.Quantity, bool) {
	switch resourceName {
	case corev1.ResourceRequestsStorage:
		return projectedPVCStorage(used, quota, changes, ""), true
	case corev1.ResourcePersistentVolumeClaims, countPersistentVolumeClaimsResource:
		return projectedPVCCount(used, quota, changes, ""), true
	}

	class, suffix, ok := splitStorageClassQuotaResourceName(resourceName)
	if !ok {
		return resource.Quantity{}, false
	}

	switch suffix {
	case corev1.ResourceRequestsStorage:
		return projectedPVCStorage(used, quota, changes, class), true
	case corev1.ResourcePersistentVolumeClaims:
		return projectedPVCCount(used, quota, changes, class), true
	default:
		return resource.Quantity{}, false
	}
}

func projectedPVCStorage(
	used resource.Quantity,
	quota corev1.ResourceQuota,
	changes []PVCAdmissionChange,
	storageClass string,
) resource.Quantity {
	projected := used.DeepCopy()
	for _, change := range changes {
		if existingPVCMatchesQuota(change, quota) &&
			(storageClass == "" || change.ExistingStorageClass == storageClass) {
			projected.Sub(change.ExistingStorage)
		}

		if requestedPVCMatchesQuota(change, quota) &&
			(storageClass == "" || change.RequestedStorageClass == storageClass) {
			projected.Add(change.RequestedStorage)
		}
	}

	return clampNonNegative(projected)
}

func projectedPVCCount(
	used resource.Quantity,
	quota corev1.ResourceQuota,
	changes []PVCAdmissionChange,
	storageClass string,
) resource.Quantity {
	projected := used.DeepCopy()
	one := resource.NewQuantity(1, resource.DecimalSI)

	for _, change := range changes {
		requestedMatches := requestedPVCMatchesQuota(change, quota) &&
			(storageClass == "" || change.RequestedStorageClass == storageClass)
		existingMatches := existingPVCMatchesQuota(change, quota) &&
			(storageClass == "" || change.ExistingStorageClass == storageClass)

		switch {
		case requestedMatches && !existingMatches:
			projected.Add(*one)
		case !requestedMatches && existingMatches:
			projected.Sub(*one)
		}
	}

	return clampNonNegative(projected)
}

func requestedPVCMatchesQuota(change PVCAdmissionChange, quota corev1.ResourceQuota) bool {
	return pvcMatchesQuotaScopes(quota, change.RequestedVolumeAttributesClassNames)
}

func existingPVCMatchesQuota(change PVCAdmissionChange, quota corev1.ResourceQuota) bool {
	return change.Existing &&
		pvcMatchesQuotaScopes(quota, change.ExistingVolumeAttributesClassNames)
}

func clampNonNegative(value resource.Quantity) resource.Quantity {
	if value.Sign() < 0 {
		return *resource.NewQuantity(0, resource.DecimalSI)
	}
	return value
}
