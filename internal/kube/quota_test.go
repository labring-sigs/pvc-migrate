package kube

import (
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestResourceQuotaDemandIncludesObjectAndStorageClassResources(t *testing.T) {
	demand, err := resourceQuotaDemand(domain.ResourceEstimate{
		StorageRequests:      "3Gi",
		PVCs:                 2,
		Pods:                 4,
		Jobs:                 2,
		Deployments:          2,
		ReplicaSets:          2,
		Services:             2,
		ServiceNodePorts:     1,
		ServiceLoadBalancers: 1,
		Endpoints:            2,
		EndpointSlices:       2,
		Secrets:              1,
		ConfigMaps:           1,
		ServiceAccounts:      2,
		Leases:               1,
		ByStorageClass:       map[string]string{"fast": "3Gi", "": "100Gi"},
		PVCsByStorageClass:   map[string]int{"fast": 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	want := map[corev1.ResourceName]string{
		corev1.ResourceRequestsStorage:                                                 "3Gi",
		corev1.ResourcePersistentVolumeClaims:                                          "2",
		corev1.ResourcePods:                                                            "4",
		corev1.ResourceName("count/pods"):                                              "4",
		corev1.ResourceServices:                                                        "2",
		corev1.ResourceServicesNodePorts:                                               "1",
		corev1.ResourceServicesLoadBalancers:                                           "1",
		corev1.ResourceSecrets:                                                         "1",
		corev1.ResourceConfigMaps:                                                      "1",
		corev1.ResourceName("count/jobs.batch"):                                        "2",
		corev1.ResourceName("count/deployments.apps"):                                  "2",
		corev1.ResourceName("count/replicasets.apps"):                                  "2",
		corev1.ResourceName("count/services"):                                          "2",
		corev1.ResourceName("count/endpoints"):                                         "2",
		corev1.ResourceName("count/endpointslices.discovery.k8s.io"):                   "2",
		corev1.ResourceName("count/secrets"):                                           "1",
		corev1.ResourceName("count/configmaps"):                                        "1",
		corev1.ResourceName("count/serviceaccounts"):                                   "2",
		corev1.ResourceName("count/persistentvolumeclaims"):                            "2",
		corev1.ResourceName("count/leases.coordination.k8s.io"):                        "1",
		corev1.ResourceName("fast.storageclass.storage.k8s.io/requests.storage"):       "3Gi",
		corev1.ResourceName("fast.storageclass.storage.k8s.io/persistentvolumeclaims"): "2",
	}
	for name, expected := range want {
		quantity, ok := demand[name]
		if !ok || quantity.Cmp(resource.MustParse(expected)) != 0 {
			t.Fatalf("demand[%s]=%s want=%s", name, quantity.String(), expected)
		}
	}

	if _, exists := demand[corev1.ResourceName(".storageclass.storage.k8s.io/requests.storage")]; exists {
		t.Fatal("empty StorageClass should be omitted")
	}
}

func TestResourceQuotaDemandCountsPVCsPerStorageClass(t *testing.T) {
	demand, err := resourceQuotaDemand(domain.ResourceEstimate{
		StorageRequests:    "3Gi",
		PVCs:               2,
		ByStorageClass:     map[string]string{"fast": "1Gi", "slow": "2Gi"},
		PVCsByStorageClass: map[string]int{"fast": 1, "slow": 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, class := range []string{"fast", "slow"} {
		name := corev1.ResourceName(class + ".storageclass.storage.k8s.io/persistentvolumeclaims")
		if quantity := demand[name]; quantity.Cmp(resource.MustParse("1")) != 0 {
			t.Fatalf("demand[%s]=%s want=1", name, quantity.String())
		}
	}
}

func TestResourceQuotaDemandRejectsInvalidQuantities(t *testing.T) {
	tests := []domain.ResourceEstimate{
		{StorageRequests: "bad", ByStorageClass: map[string]string{}},
		{StorageRequests: "1Gi", ByStorageClass: map[string]string{"fast": "bad"}},
		{
			StorageRequests:    "1Gi",
			ByStorageClass:     map[string]string{"fast": "1Gi"},
			PVCsByStorageClass: map[string]int{},
		},
	}
	for _, estimate := range tests {
		_, err := resourceQuotaDemand(estimate)
		if domain.CategoryOf(err) != domain.ErrorInternal {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	}
}

func TestResourceQuotaDemandOmitsZeroResources(t *testing.T) {
	demand, err := resourceQuotaDemand(domain.ResourceEstimate{
		StorageRequests:    "0",
		ByStorageClass:     map[string]string{"fast": "0"},
		PVCsByStorageClass: map[string]int{"fast": 0},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(demand) != 0 {
		t.Fatalf("zero estimate produced quota demand: %v", demand)
	}
}

func TestEvaluateResourceQuotaCapacityCountsLimitRangeDefault(t *testing.T) {
	quota := corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "ephemeral"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceLimitsEphemeralStorage: resource.MustParse("1Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceLimitsEphemeralStorage: resource.MustParse("0"),
			},
		},
	}
	limitRange := corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Name: "defaults"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Default: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
			},
		}}},
	}

	report, err := EvaluateResourceQuotaCapacity(
		"application",
		[]corev1.ResourceQuota{quota},
		[]corev1.LimitRange{limitRange},
		domain.ResourceEstimate{Pods: 1},
	)
	if err != nil {
		t.Fatal(err)
	}

	if report.Checked != 1 || len(report.Violations) != 1 {
		t.Fatalf("report=%#v, want one defaulted limit overflow", report)
	}
}

func TestEvaluateResourceQuotaCapacityIgnoresDefaultRequestForExplicitZero(t *testing.T) {
	quota := corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "ephemeral"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceLimitsEphemeralStorage: resource.MustParse("0"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceLimitsEphemeralStorage: resource.MustParse("0"),
			},
		},
	}
	limitRange := corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Name: "requests"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			DefaultRequest: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			},
		}}},
	}

	report, err := EvaluateResourceQuotaCapacity(
		"application",
		[]corev1.ResourceQuota{quota},
		[]corev1.LimitRange{limitRange},
		domain.ResourceEstimate{Pods: 1},
	)
	if err != nil {
		t.Fatal(err)
	}

	if report.Checked != 0 || len(report.Violations) != 0 {
		t.Fatalf("defaultRequest produced a limit demand: %#v", report)
	}
}

func TestEvaluateResourceQuotaCapacityUsesScopedPodPhasePeaks(t *testing.T) {
	quota := func(name string, scope corev1.ResourceQuotaScope, hard string) corev1.ResourceQuota {
		result := corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status: corev1.ResourceQuotaStatus{
				Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse(hard)},
				Used: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("0")},
			},
		}
		if scope != "" {
			result.Spec.Scopes = []corev1.ResourceQuotaScope{scope}
		}

		return result
	}

	report, err := EvaluateResourceQuotaCapacity(
		"application",
		[]corev1.ResourceQuota{
			quota("all", "", "3"),
			quota("probes", corev1.ResourceQuotaScopeTerminating, "2"),
			quota("transfers", corev1.ResourceQuotaScopeNotTerminating, "2"),
		},
		nil,
		domain.ResourceEstimate{
			Pods:               4,
			TerminatingPods:    3,
			NotTerminatingPods: 2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	joined := strings.Join(report.Violations, "; ")
	for _, expected := range []string{
		"application/all pods: used 0 + requested 4 exceeds hard 3",
		"application/probes pods: used 0 + requested 3 exceeds hard 2",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("violations %q omit %q", joined, expected)
		}
	}

	if strings.Contains(joined, "transfers") {
		t.Fatalf("NotTerminating peak should fit exactly: %q", joined)
	}
}
