package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestQuotaDemandIncludesObjectAndStorageClassResources(t *testing.T) {
	demand, err := quotaDemand(domain.ResourceEstimate{
		StorageRequests:    "3Gi",
		PVCs:               2,
		Pods:               4,
		Jobs:               2,
		Services:           2,
		Secrets:            1,
		ConfigMaps:         1,
		ServiceAccounts:    2,
		Leases:             1,
		ByStorageClass:     map[string]string{"fast": "3Gi", "": "100Gi"},
		PVCsByStorageClass: map[string]int{"fast": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[corev1.ResourceName]string{
		corev1.ResourceRequestsStorage:                                                 "3Gi",
		corev1.ResourcePersistentVolumeClaims:                                          "2",
		corev1.ResourcePods:                                                            "4",
		corev1.ResourceServices:                                                        "2",
		corev1.ResourceSecrets:                                                         "1",
		corev1.ResourceConfigMaps:                                                      "1",
		corev1.ResourceName("count/jobs.batch"):                                        "2",
		corev1.ResourceName("count/services"):                                          "2",
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

func TestQuotaDemandCountsPVCsPerStorageClass(t *testing.T) {
	demand, err := quotaDemand(domain.ResourceEstimate{
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

func TestQuotaDemandRejectsInvalidQuantities(t *testing.T) {
	tests := []domain.ResourceEstimate{
		{StorageRequests: "bad", ByStorageClass: map[string]string{}},
		{StorageRequests: "1Gi", ByStorageClass: map[string]string{"fast": "bad"}},
		{StorageRequests: "1Gi", ByStorageClass: map[string]string{"fast": "1Gi"}, PVCsByStorageClass: map[string]int{}},
	}
	for _, estimate := range tests {
		if _, err := quotaDemand(estimate); domain.CategoryOf(err) != domain.ErrorInternal {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	}
}

func TestCheckQuotasAllowsExactCapacityAndReportsAllExcess(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "bounded"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceRequestsStorage: resource.MustParse("3Gi"),
			corev1.ResourcePods:            resource.MustParse("5"),
		}},
		Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
			corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
			corev1.ResourcePods:            resource.MustParse("4"),
		}},
	}
	planner := New(kubernetesfake.NewClientset(quota), nil)
	plan := &domain.MigrationPlan{Ready: true}
	planner.checkQuotas(context.Background(), plan, "stage", domain.ResourceEstimate{StorageRequests: "2Gi", Pods: 1, ByStorageClass: map[string]string{}})
	if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
		t.Fatalf("exact capacity check: %#v", plan.Checks)
	}

	plan = &domain.MigrationPlan{Ready: true}
	planner.checkQuotas(context.Background(), plan, "stage", domain.ResourceEstimate{StorageRequests: "3Gi", Pods: 2, ByStorageClass: map[string]string{}})
	if plan.Ready || len(plan.Checks) != 1 {
		t.Fatalf("excess capacity check: %#v", plan.Checks)
	}
	for _, resourceName := range []string{"requests.storage", "pods"} {
		if !strings.Contains(plan.Checks[0].Message, resourceName) {
			t.Fatalf("quota message omits %s: %s", resourceName, plan.Checks[0].Message)
		}
	}
}

func TestCheckLimitRangesValidatesMinimumMaximumAndMalformedCapacity(t *testing.T) {
	limitRange := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "pvc-bounds"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypePersistentVolumeClaim,
			Min:  corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			Max:  corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
		}}},
	}
	planner := New(kubernetesfake.NewClientset(limitRange), nil)
	plan := &domain.MigrationPlan{Ready: true}
	planner.checkLimitRanges(context.Background(), plan, "stage", []domain.PlannedVolume{
		{SourcePVC: domain.ObjectReference{Name: "small"}, DestinationPVC: domain.ObjectReference{Name: "small-target"}, Capacity: "512Mi"},
		{SourcePVC: domain.ObjectReference{Name: "large"}, DestinationPVC: domain.ObjectReference{Name: "large-target"}, Capacity: "3Gi"},
		{SourcePVC: domain.ObjectReference{Name: "broken"}, DestinationPVC: domain.ObjectReference{Name: "broken-target"}, Capacity: "invalid"},
	})
	if plan.Ready || len(plan.Checks) != 1 {
		t.Fatalf("limit checks: %#v", plan.Checks)
	}
	for _, expected := range []string{"below", "above", "invalid capacity"} {
		if !strings.Contains(plan.Checks[0].Message, expected) {
			t.Fatalf("limit message omits %q: %s", expected, plan.Checks[0].Message)
		}
	}
}

func TestCheckLimitRangesRejectsPositiveToolPodMinimums(t *testing.T) {
	limitRange := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "pod-bounds"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypePod,
			Min:  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m")},
		}}},
	}
	planner := New(kubernetesfake.NewClientset(limitRange), nil)
	plan := &domain.MigrationPlan{Ready: true}
	planner.checkLimitRanges(context.Background(), plan, "stage", []domain.PlannedVolume{{Capacity: "1Gi"}})
	if plan.Ready || len(plan.Checks) != 1 || !strings.Contains(plan.Checks[0].Message, "tool Pod resource cpu=0") {
		t.Fatalf("pod minimum check: %#v", plan.Checks)
	}
}

func TestCheckLimitRangesRejectsPositiveToolContainerMinimums(t *testing.T) {
	limitRange := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "container-bounds"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Min:  corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("16Mi")},
		}}},
	}
	planner := New(kubernetesfake.NewClientset(limitRange), nil)
	plan := &domain.MigrationPlan{Ready: true}
	planner.checkLimitRanges(context.Background(), plan, "stage", []domain.PlannedVolume{{Capacity: "1Gi"}})
	if plan.Ready || len(plan.Checks) != 1 || !strings.Contains(plan.Checks[0].Message, "tool container") {
		t.Fatalf("container minimum check: %#v", plan.Checks)
	}
}

func TestCheckLimitRangesModelsToolDefaultsAndMaxRequestRatio(t *testing.T) {
	newLimitRange := func(ratio corev1.ResourceList) *corev1.LimitRange {
		return &corev1.LimitRange{
			ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "container-policy"},
			Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
				Type:                 corev1.LimitTypeContainer,
				Min:                  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")},
				Max:                  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
				Default:              corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")},
				DefaultRequest:       corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				MaxLimitRequestRatio: ratio,
			}}},
		}
	}

	t.Run("defaults and max permit explicit zero", func(t *testing.T) {
		plan := &domain.MigrationPlan{Ready: true}
		New(kubernetesfake.NewClientset(newLimitRange(nil)), nil).checkLimitRanges(context.Background(), plan, "stage", []domain.PlannedVolume{{Capacity: "1Gi"}})
		if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
			t.Fatalf("limit check: %#v", plan.Checks)
		}
	})

	t.Run("max request ratio rejects zero pair", func(t *testing.T) {
		plan := &domain.MigrationPlan{Ready: true}
		New(kubernetesfake.NewClientset(newLimitRange(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")})), nil).checkLimitRanges(context.Background(), plan, "stage", []domain.PlannedVolume{{Capacity: "1Gi"}})
		if plan.Ready || len(plan.Checks) != 1 || !strings.Contains(plan.Checks[0].Message, "maxLimitRequestRatio") {
			t.Fatalf("ratio check: %#v", plan.Checks)
		}
	})
}
