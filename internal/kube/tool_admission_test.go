package kube_test

import (
	"strings"
	"testing"

	. "github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestToolResourcesAfterLimitRangesDefaultsOnlyOmittedLimit(t *testing.T) {
	resources := ToolResourcesAfterLimitRanges([]corev1.LimitRange{{
		ObjectMeta: metav1.ObjectMeta{Name: "defaults"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Default: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("1"),
				corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
			},
			DefaultRequest: corev1.ResourceList{
				corev1.ResourceCPU:                        resource.MustParse("500m"),
				corev1.ResourceEphemeralStorage:           resource.MustParse("1Gi"),
				corev1.ResourceName("example.com/device"): resource.MustParse("1"),
			},
		}}},
	}})

	zero := resource.MustParse("0")
	if got := resources.Requests[corev1.ResourceEphemeralStorage]; got.Cmp(zero) != 0 {
		t.Fatalf("ephemeral-storage request=%s, want explicit zero", got.String())
	}

	if got := resources.Limits[corev1.ResourceCPU]; got.Cmp(zero) != 0 {
		t.Fatalf("cpu limit=%s, want explicit zero", got.String())
	}

	if got := resources.Limits[corev1.ResourceEphemeralStorage]; got.Cmp(
		resource.MustParse("2Gi"),
	) != 0 {
		t.Fatalf("ephemeral-storage limit=%s, want default 2Gi", got.String())
	}

	if got := resources.Requests[corev1.ResourceName("example.com/device")]; got.Cmp(
		resource.MustParse("1"),
	) != 0 {
		t.Fatalf("device request=%s, want default 1", got.String())
	}
}

func TestToolResourcesAfterLimitRangesDoesNotTurnDefaultRequestIntoLimit(t *testing.T) {
	resources := ToolResourcesAfterLimitRanges([]corev1.LimitRange{{
		ObjectMeta: metav1.ObjectMeta{Name: "request-defaults"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			DefaultRequest: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			},
		}}},
	}})

	zero := resource.MustParse("0")
	if got := resources.Requests[corev1.ResourceEphemeralStorage]; got.Cmp(zero) != 0 {
		t.Fatalf("ephemeral-storage request=%s, want explicit zero", got.String())
	}

	if _, exists := resources.Limits[corev1.ResourceEphemeralStorage]; exists {
		t.Fatal("ephemeral-storage defaultRequest became a limit")
	}

	if demand := ToolComputeQuotaDemand([]corev1.LimitRange{{
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			DefaultRequest: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			},
		}}},
	}}, 1); len(demand) != 0 {
		t.Fatalf("explicit zero request and omitted limit produced quota demand: %v", demand)
	}
}

func TestToolResourcesAfterLimitRangesAppliesAPIDefaults(t *testing.T) {
	resources := ToolResourcesAfterLimitRanges([]corev1.LimitRange{{
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Max: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("4Gi"),
			},
		}}},
	}})

	if got := resources.Limits[corev1.ResourceEphemeralStorage]; got.Cmp(
		resource.MustParse("4Gi"),
	) != 0 {
		t.Fatalf("ephemeral-storage limit=%s, want API-defaulted 4Gi", got.String())
	}

	zero := resource.MustParse("0")
	if got := resources.Requests[corev1.ResourceEphemeralStorage]; got.Cmp(zero) != 0 {
		t.Fatalf("ephemeral-storage request=%s, want explicit zero", got.String())
	}
}

func TestToolComputeQuotaDemandCountsDefaultedEphemeralLimit(t *testing.T) {
	limitRanges := []corev1.LimitRange{{
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Default: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
			},
			DefaultRequest: corev1.ResourceList{
				corev1.ResourceName("example.com/device"): resource.MustParse("1"),
			},
		}}},
	}}

	demand := ToolComputeQuotaDemand(limitRanges, 3)
	if got := demand[corev1.ResourceLimitsEphemeralStorage]; got.Cmp(
		resource.MustParse("6Gi"),
	) != 0 {
		t.Fatalf("ephemeral-storage limit demand=%s, want 6Gi", got.String())
	}

	if _, exists := demand[corev1.ResourceRequestsEphemeralStorage]; exists {
		t.Fatal("zero ephemeral-storage request must not become quota demand")
	}

	if got := demand[corev1.ResourceName("requests.example.com/device")]; got.Cmp(
		resource.MustParse("3"),
	) != 0 {
		t.Fatalf("device request demand=%s, want 3", got.String())
	}
}

func TestToolComputeQuotaDemandMatchesExtendedResourceEvaluator(t *testing.T) {
	const deviceClassResource = corev1.ResourceName("deviceclass.resource.kubernetes.io/gpu")

	defaultRequests := corev1.ResourceList{}
	defaultRequests[corev1.ResourceName("example.com/device")] = resource.MustParse("1")
	defaultRequests[deviceClassResource] = resource.MustParse("1")
	defaultRequests[corev1.ResourceName("kubernetes.io/unknown")] = resource.MustParse("1")

	limitRanges := []corev1.LimitRange{{
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type:           corev1.LimitTypeContainer,
			DefaultRequest: defaultRequests,
		}}},
	}}

	demand := ToolComputeQuotaDemand(limitRanges, 2)
	for _, name := range []corev1.ResourceName{
		"requests.example.com/device",
		"requests.deviceclass.resource.kubernetes.io/gpu",
	} {
		if got := demand[name]; got.Cmp(resource.MustParse("2")) != 0 {
			t.Fatalf("demand[%s]=%s, want 2", name, got.String())
		}
	}

	if _, exists := demand[corev1.ResourceName("requests.kubernetes.io/unknown")]; exists {
		t.Fatalf("unknown native resource became quota demand: %v", demand)
	}
}

func TestToolLimitRangeViolationsHonorAPIDefaultsAndRatio(t *testing.T) {
	violations := ToolLimitRangeViolations([]corev1.LimitRange{{
		ObjectMeta: metav1.ObjectMeta{Name: "policy"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Max: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("4Gi"),
			},
			MaxLimitRequestRatio: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("2"),
			},
		}}},
	}})

	joined := strings.Join(violations, "; ")
	if !strings.Contains(joined, "maxLimitRequestRatio") {
		t.Fatalf("violations %q omit maxLimitRequestRatio", joined)
	}

	if strings.Contains(joined, "ephemeral-storage") {
		t.Fatalf("API-defaulted ephemeral-storage limit was rejected: %q", joined)
	}
}

func TestToolQuotaResourceMatchesScopesAndEvaluators(t *testing.T) {
	tests := []struct {
		name     string
		quota    corev1.ResourceQuota
		resource corev1.ResourceName
		want     bool
	}{
		{name: "unscoped object", resource: corev1.ResourceName("count/jobs.batch"), want: true},
		{
			name:     "extended resource",
			resource: corev1.ResourceName("requests.example.com/device"),
			want:     true,
		},
		{
			name:     "DRA resource",
			resource: corev1.ResourceName("requests.deviceclass.resource.kubernetes.io/gpu"),
			want:     true,
		},
		{
			name:     "unknown native resource",
			resource: corev1.ResourceName("requests.kubernetes.io/unknown"),
		},
		{name: "unscoped PVC storage", resource: corev1.ResourceRequestsStorage, want: true},
		{
			name: "VAC exists excludes tool PVC",
			quota: corev1.ResourceQuota{Spec: corev1.ResourceQuotaSpec{
				Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeVolumeAttributesClass},
			}},
			resource: corev1.ResourceRequestsStorage,
		},
		{
			name: "VAC does not exist includes tool PVC",
			quota: corev1.ResourceQuota{Spec: corev1.ResourceQuotaSpec{
				ScopeSelector: &corev1.ScopeSelector{
					MatchExpressions: []corev1.ScopedResourceSelectorRequirement{{
						ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
						Operator:  corev1.ScopeSelectorOpDoesNotExist,
					}},
				},
			}},
			resource: corev1.ResourceRequestsStorage,
			want:     true,
		},
		{
			name: "best effort pod",
			quota: corev1.ResourceQuota{Spec: corev1.ResourceQuotaSpec{
				Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeBestEffort},
			}},
			resource: corev1.ResourcePods,
			want:     true,
		},
		{
			name: "not best effort pod",
			quota: corev1.ResourceQuota{Spec: corev1.ResourceQuotaSpec{
				Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeNotBestEffort},
			}},
			resource: corev1.ResourcePods,
		},
		{
			name: "scoped job object",
			quota: corev1.ResourceQuota{Spec: corev1.ResourceQuotaSpec{
				Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeBestEffort},
			}},
			resource: corev1.ResourceName("count/jobs.batch"),
		},
		{
			name: "priority absent satisfies not in",
			quota: corev1.ResourceQuota{Spec: corev1.ResourceQuotaSpec{
				ScopeSelector: &corev1.ScopeSelector{
					MatchExpressions: []corev1.ScopedResourceSelectorRequirement{{
						ScopeName: corev1.ResourceQuotaScopePriorityClass,
						Operator:  corev1.ScopeSelectorOpNotIn,
						Values:    []string{"high"},
					}},
				},
			}},
			resource: corev1.ResourcePods,
			want:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ToolQuotaResourceMatches(test.quota, test.resource); got != test.want {
				t.Fatalf("match=%t, want %t", got, test.want)
			}
		})
	}
}
