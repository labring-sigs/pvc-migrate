package kube

import (
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ToolResourcesAfterLimitRanges returns the resources admission will see after
// applying container defaults. Explicit zero values are retained, so only
// resources omitted by the tool can be defaulted.
func ToolResourcesAfterLimitRanges(limitRanges []corev1.LimitRange) corev1.ResourceRequirements {
	result := ZeroResourceRequirements()

	for _, limitRange := range limitRanges {
		defaultRequests := corev1.ResourceList{}

		defaultLimits := corev1.ResourceList{}
		for _, item := range limitRange.Spec.Limits {
			if item.Type != corev1.LimitTypeContainer {
				continue
			}

			itemDefaults, itemDefaultRequests := toolLimitRangeDefaults(item)
			for name, value := range itemDefaultRequests {
				defaultRequests[name] = value.DeepCopy()
			}

			for name, value := range itemDefaults {
				defaultLimits[name] = value.DeepCopy()
			}
		}

		for name, value := range defaultRequests {
			if _, exists := result.Requests[name]; !exists {
				result.Requests[name] = value.DeepCopy()
			}
		}

		for name, value := range defaultLimits {
			if _, exists := result.Limits[name]; !exists {
				result.Limits[name] = value.DeepCopy()
			}
		}
	}

	return result
}

// LimitRange objects are normally API-defaulted before a client reads them.
// Reproduce that step so fake clients and direct internal callers see the same
// max -> default -> defaultRequest and min -> defaultRequest behavior.
func toolLimitRangeDefaults(
	item corev1.LimitRangeItem,
) (corev1.ResourceList, corev1.ResourceList) {
	defaultLimits := make(corev1.ResourceList, len(item.Default)+len(item.Max))
	for name, value := range item.Default {
		defaultLimits[name] = value.DeepCopy()
	}

	for name, value := range item.Max {
		if _, exists := defaultLimits[name]; !exists {
			defaultLimits[name] = value.DeepCopy()
		}
	}

	defaultRequests := make(
		corev1.ResourceList,
		len(item.DefaultRequest)+len(defaultLimits)+len(item.Min),
	)
	for name, value := range item.DefaultRequest {
		defaultRequests[name] = value.DeepCopy()
	}

	for name, value := range defaultLimits {
		if _, exists := defaultRequests[name]; !exists {
			defaultRequests[name] = value.DeepCopy()
		}
	}

	for name, value := range item.Min {
		if _, exists := defaultRequests[name]; !exists {
			defaultRequests[name] = value.DeepCopy()
		}
	}

	return defaultLimits, defaultRequests
}

// ToolLimitRangeViolations mirrors the LimitRanger checks relevant to the
// one-container tool Pods emitted by pvc-migrate and the embedded chart.
func ToolLimitRangeViolations(limitRanges []corev1.LimitRange) []string {
	resources := ToolResourcesAfterLimitRanges(limitRanges)
	violations := make([]string, 0)

	for _, limitRange := range limitRanges {
		for _, item := range limitRange.Spec.Limits {
			if item.Type != corev1.LimitTypeContainer && item.Type != corev1.LimitTypePod {
				continue
			}

			scope := "tool Pod"
			if item.Type == corev1.LimitTypeContainer {
				scope = "tool container"
			}

			for name, minimum := range item.Min {
				request, requested := resources.Requests[name]
				switch {
				case !requested:
					violations = append(violations, fmt.Sprintf(
						"%s omits resource %s request required by %s minimum %s",
						scope, name, limitRange.Name, minimum.String(),
					))
				case request.Cmp(minimum) < 0:
					violations = append(violations, fmt.Sprintf(
						"%s resource %s request %s is below %s minimum %s",
						scope, name, request.String(), limitRange.Name, minimum.String(),
					))
				}

				if limit, limited := resources.Limits[name]; limited && limit.Cmp(minimum) < 0 {
					violations = append(violations, fmt.Sprintf(
						"%s resource %s limit %s is below %s minimum %s",
						scope, name, limit.String(), limitRange.Name, minimum.String(),
					))
				}
			}

			for name, maximum := range item.Max {
				limit, limited := resources.Limits[name]
				switch {
				case !limited:
					violations = append(violations, fmt.Sprintf(
						"%s omits resource %s limit required by %s maximum %s",
						scope, name, limitRange.Name, maximum.String(),
					))
				case limit.Cmp(maximum) > 0:
					violations = append(violations, fmt.Sprintf(
						"%s resource %s limit %s exceeds %s maximum %s",
						scope, name, limit.String(), limitRange.Name, maximum.String(),
					))
				}

				if request, requested := resources.Requests[name]; requested &&
					request.Cmp(maximum) > 0 {
					violations = append(violations, fmt.Sprintf(
						"%s resource %s request %s exceeds %s maximum %s",
						scope, name, request.String(), limitRange.Name, maximum.String(),
					))
				}
			}

			for name, ratio := range item.MaxLimitRequestRatio {
				request, requested := resources.Requests[name]

				limit, limited := resources.Limits[name]
				if !requested || request.Sign() == 0 || !limited || limit.Sign() == 0 {
					violations = append(violations, fmt.Sprintf(
						"%s resource %s violates %s maxLimitRequestRatio %s because its request or limit is zero or omitted",
						scope,
						name,
						limitRange.Name,
						ratio.String(),
					))

					continue
				}

				observed, exceeded := toolLimitRequestRatio(request, limit, ratio)
				if exceeded {
					violations = append(violations, fmt.Sprintf(
						"%s resource %s limit/request ratio %g exceeds %s maximum %s",
						scope, name, observed, limitRange.Name, ratio.String(),
					))
				}
			}
		}
	}

	sort.Strings(violations)

	return violations
}

// ToolComputeQuotaDemand returns the Pod compute resources measured by the
// ResourceQuota evaluator. An omitted ephemeral-storage limit is absent from
// the result because Kubernetes does not require or count it.
func ToolComputeQuotaDemand(
	limitRanges []corev1.LimitRange,
	pods int,
) corev1.ResourceList {
	result := corev1.ResourceList{}
	if pods <= 0 {
		return result
	}

	resources := ToolResourcesAfterLimitRanges(limitRanges)
	multiplier := int64(pods)
	add := func(name corev1.ResourceName, quantity resource.Quantity) {
		if quantity.Sign() == 0 {
			return
		}

		value := quantity.DeepCopy()
		value.Mul(multiplier)
		result[name] = value
	}

	for name, request := range resources.Requests {
		switch name {
		case corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage:
			add(name, request)
			add(corev1.ResourceName(corev1.DefaultResourceRequestsPrefix+string(name)), request)
		default:
			if strings.HasPrefix(string(name), corev1.ResourceHugePagesPrefix) {
				add(name, request)
				add(corev1.ResourceName(corev1.DefaultResourceRequestsPrefix+string(name)), request)
			} else if isToolExtendedResource(name) {
				add(corev1.ResourceName(corev1.DefaultResourceRequestsPrefix+string(name)), request)
			}
		}
	}

	for name, limit := range resources.Limits {
		switch name {
		case corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage:
			add(corev1.ResourceName("limits."+string(name)), limit)
		}
	}

	return result
}

// ToolQuotaResourceMatches reports whether a quota constraint is evaluated for
// resources created by a tool run. Tool probes are Terminating Pods; transfer
// and reservation Pods are NotTerminating. Both are BestEffort, have no
// PriorityClass, and do not use cross-namespace Pod affinity.
func ToolQuotaResourceMatches(quota corev1.ResourceQuota, name corev1.ResourceName) bool {
	if !isToolQuotaResource(name) {
		return false
	}

	selectors := toolQuotaSelectors(quota)

	if len(selectors) == 0 {
		return true
	}

	switch {
	case isToolPodQuotaResource(name):
		return toolPodMatchesQuotaScopes(quota, true) ||
			toolPodMatchesQuotaScopes(quota, false)
	case isPVCQuotaResource(name):
		for _, selector := range selectors {
			if !pvcMatchesScope(selector, nil) {
				return false
			}
		}
	default:
		return false
	}

	return true
}

func toolQuotaSelectors(quota corev1.ResourceQuota) []corev1.ScopedResourceSelectorRequirement {
	selectors := make(
		[]corev1.ScopedResourceSelectorRequirement,
		0,
		len(quota.Spec.Scopes),
	)
	for _, scope := range quota.Spec.Scopes {
		selectors = append(selectors, corev1.ScopedResourceSelectorRequirement{
			ScopeName: scope,
			Operator:  corev1.ScopeSelectorOpExists,
		})
	}

	if quota.Spec.ScopeSelector != nil {
		selectors = append(selectors, quota.Spec.ScopeSelector.MatchExpressions...)
	}

	return selectors
}

func toolQuotaPodCount(quota corev1.ResourceQuota, estimate domain.ResourceEstimate) int {
	terminating := toolPodMatchesQuotaScopes(quota, true)
	notTerminating := toolPodMatchesQuotaScopes(quota, false)

	switch {
	case terminating && notTerminating:
		return estimate.Pods
	case terminating:
		return estimate.TerminatingPods
	case notTerminating:
		return estimate.NotTerminatingPods
	default:
		return 0
	}
}

func toolPodMatchesQuotaScopes(quota corev1.ResourceQuota, terminating bool) bool {
	for _, selector := range toolQuotaSelectors(quota) {
		if !toolPodMatchesScope(selector, terminating) {
			return false
		}
	}

	return true
}

func isToolQuotaResource(name corev1.ResourceName) bool {
	if isToolPodQuotaResource(name) {
		return true
	}

	switch name {
	case corev1.ResourceRequestsStorage,
		corev1.ResourcePersistentVolumeClaims,
		corev1.ResourceServices,
		corev1.ResourceServicesNodePorts,
		corev1.ResourceServicesLoadBalancers,
		corev1.ResourceSecrets,
		corev1.ResourceConfigMaps:
		return true
	}

	value := string(name)
	if strings.HasPrefix(value, countResourcePrefix) {
		return len(value) > len(countResourcePrefix)
	}

	class, resourceName, found := splitStorageClassQuotaResourceName(name)
	if !found || class == "" {
		return false
	}

	return resourceName == corev1.ResourceRequestsStorage ||
		resourceName == corev1.ResourcePersistentVolumeClaims
}

var toolPodQuotaResources = map[corev1.ResourceName]bool{
	corev1.ResourcePods:                     true,
	countPodsResource:                       true,
	corev1.ResourceCPU:                      true,
	corev1.ResourceMemory:                   true,
	corev1.ResourceEphemeralStorage:         true,
	corev1.ResourceRequestsCPU:              true,
	corev1.ResourceRequestsMemory:           true,
	corev1.ResourceRequestsEphemeralStorage: true,
	corev1.ResourceLimitsCPU:                true,
	corev1.ResourceLimitsMemory:             true,
	corev1.ResourceLimitsEphemeralStorage:   true,
}

func isToolPodQuotaResource(name corev1.ResourceName) bool {
	if toolPodQuotaResources[name] {
		return true
	}

	value := string(name)

	return strings.HasPrefix(value, corev1.ResourceHugePagesPrefix) ||
		strings.HasPrefix(value, corev1.ResourceRequestsHugePagesPrefix) ||
		isToolQuotaExtendedResource(name)
}

func isToolQuotaExtendedResource(name corev1.ResourceName) bool {
	value := string(name)
	if !strings.HasPrefix(value, corev1.DefaultResourceRequestsPrefix) {
		return false
	}

	return isToolExtendedResource(corev1.ResourceName(strings.TrimPrefix(
		value,
		corev1.DefaultResourceRequestsPrefix,
	)))
}

func isToolExtendedResource(name corev1.ResourceName) bool {
	value := string(name)
	if strings.HasPrefix(value, resourcev1.ResourceDeviceClassPrefix) {
		return true
	}

	return strings.Contains(value, "/") &&
		!strings.Contains(value, corev1.ResourceDefaultNamespacePrefix) &&
		!strings.HasPrefix(value, corev1.DefaultResourceRequestsPrefix)
}

func toolLimitRequestRatio(
	request resource.Quantity,
	limit resource.Quantity,
	ratio resource.Quantity,
) (float64, bool) {
	requestValue := request.Value()
	limitValue := limit.Value()

	ratioValue := ratio.Value()
	if requestValue <= resource.MaxMilliValue &&
		limitValue <= resource.MaxMilliValue &&
		ratioValue <= resource.MaxMilliValue {
		requestValue = request.MilliValue()
		limitValue = limit.MilliValue()
		ratioValue = ratio.MilliValue()
	}

	observed := float64(limitValue) / float64(requestValue)

	maximum := float64(ratioValue)
	if ratio.Value() <= resource.MaxMilliValue {
		observed *= 1000
		maximum = float64(ratio.MilliValue())
	}

	return float64(limitValue) / float64(requestValue), observed > maximum
}

func toolPodMatchesScope(
	selector corev1.ScopedResourceSelectorRequirement,
	terminating bool,
) bool {
	switch selector.ScopeName {
	case corev1.ResourceQuotaScopeTerminating:
		return booleanToolScopeMatches(selector.Operator, terminating)
	case corev1.ResourceQuotaScopeNotTerminating:
		return booleanToolScopeMatches(selector.Operator, !terminating)
	case corev1.ResourceQuotaScopeBestEffort:
		return booleanToolScopeMatches(selector.Operator, true)
	case corev1.ResourceQuotaScopeNotBestEffort,
		corev1.ResourceQuotaScopeCrossNamespacePodAffinity,
		corev1.ResourceQuotaScopeVolumeAttributesClass:
		return booleanToolScopeMatches(selector.Operator, false)
	case corev1.ResourceQuotaScopePriorityClass:
		return selector.Operator == corev1.ScopeSelectorOpNotIn ||
			selector.Operator == corev1.ScopeSelectorOpDoesNotExist
	default:
		return false
	}
}

func booleanToolScopeMatches(operator corev1.ScopeSelectorOperator, present bool) bool {
	switch operator {
	case corev1.ScopeSelectorOpExists:
		return present
	case corev1.ScopeSelectorOpDoesNotExist:
		return !present
	default:
		return false
	}
}
