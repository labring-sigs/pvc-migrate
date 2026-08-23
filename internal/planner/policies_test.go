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
		}, Hard: corev1.ResourceList{
			corev1.ResourceRequestsStorage: resource.MustParse("3Gi"),
			corev1.ResourcePods:            resource.MustParse("5"),
		}},
	}
	planner := New(kubernetesfake.NewClientset(quota), nil)
	plan := &domain.MigrationPlan{Ready: true}
	planner.checkQuotas(
		context.Background(),
		plan,
		"stage",
		domain.ResourceEstimate{
			StorageRequests: "2Gi",
			Pods:            1,
			ByStorageClass:  map[string]string{},
		},
	)

	if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
		t.Fatalf("exact capacity check: %#v", plan.Checks)
	}

	plan = &domain.MigrationPlan{Ready: true}
	planner.checkQuotas(
		context.Background(),
		plan,
		"stage",
		domain.ResourceEstimate{
			StorageRequests: "3Gi",
			Pods:            2,
			ByStorageClass:  map[string]string{},
		},
	)

	if plan.Ready || len(plan.Checks) != 1 {
		t.Fatalf("excess capacity check: %#v", plan.Checks)
	}

	for _, resourceName := range []string{"requests.storage", "pods"} {
		if !strings.Contains(plan.Checks[0].Message, resourceName) {
			t.Fatalf("quota message omits %s: %s", resourceName, plan.Checks[0].Message)
		}
	}
}

func TestCheckQuotasAccountsForDefaultedToolResources(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "ephemeral"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceLimitsEphemeralStorage: resource.MustParse("3Gi"),
		}},
	}
	limitRange := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "defaults"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Default: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
			},
		}}},
	}
	planner := New(plannerClient(quota, limitRange), nil)
	plan := &domain.MigrationPlan{Ready: true}
	planner.checkQuotas(context.Background(), plan, "stage", domain.ResourceEstimate{
		StorageRequests:    "0",
		Pods:               2,
		ByStorageClass:     map[string]string{},
		PVCsByStorageClass: map[string]int{},
	})

	if plan.Ready || len(plan.Checks) != 1 ||
		!strings.Contains(plan.Checks[0].Message, "limits.ephemeral-storage") ||
		!strings.Contains(plan.Checks[0].Message, "4Gi") {
		t.Fatalf("defaulted resource quota check: %#v", plan.Checks)
	}
}

func TestCheckQuotasIgnoresNonmatchingToolScope(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "non-best-effort"},
		Spec: corev1.ResourceQuotaSpec{
			Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeNotBestEffort},
			Hard: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("0"),
			},
		},
	}
	planner := New(plannerClient(quota), nil)
	plan := &domain.MigrationPlan{Ready: true}
	planner.checkQuotas(context.Background(), plan, "stage", domain.ResourceEstimate{
		StorageRequests:    "0",
		Pods:               1,
		ByStorageClass:     map[string]string{},
		PVCsByStorageClass: map[string]int{},
	})

	if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
		t.Fatalf("nonmatching scoped quota rejected tool: %#v", plan.Checks)
	}
}

func TestCheckQuotasEnforcesToolObjectCounts(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "objects"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName("count/pods"):             resource.MustParse("4"),
			corev1.ResourceName("count/deployments.apps"): resource.MustParse("1"),
			corev1.ResourceName("count/replicasets.apps"): resource.MustParse("2"),
		}},
		Status: corev1.ResourceQuotaStatus{
			Used: corev1.ResourceList{
				corev1.ResourceName("count/pods"):             resource.MustParse("3"),
				corev1.ResourceName("count/deployments.apps"): resource.MustParse("0"),
				corev1.ResourceName("count/replicasets.apps"): resource.MustParse("2"),
			},
		},
	}
	planner := New(plannerClient(quota), nil)
	plan := &domain.MigrationPlan{Ready: true}
	planner.checkQuotas(context.Background(), plan, "stage", domain.ResourceEstimate{
		StorageRequests:    "0",
		Pods:               2,
		Deployments:        2,
		ReplicaSets:        1,
		ByStorageClass:     map[string]string{},
		PVCsByStorageClass: map[string]int{},
	})

	if plan.Ready || len(plan.Checks) != 1 {
		t.Fatalf("tool object-count quota check: %#v", plan.Checks)
	}

	for _, resourceName := range []string{"count/pods", "count/deployments.apps", "count/replicasets.apps"} {
		if !strings.Contains(plan.Checks[0].Message, resourceName) {
			t.Fatalf(
				"object-count quota message omits %s: %s",
				resourceName,
				plan.Checks[0].Message,
			)
		}
	}
}

func TestCheckQuotasSkipsLimitRangesWithoutPods(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "session", Name: "objects"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceConfigMaps: resource.MustParse("1"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceConfigMaps: resource.MustParse("0"),
			},
		},
	}
	client := kubernetesfake.NewClientset(quota)
	planner := New(client, nil)
	plan := &domain.MigrationPlan{Ready: true}
	planner.checkQuotas(context.Background(), plan, "session", domain.ResourceEstimate{
		StorageRequests:    "0",
		ConfigMaps:         1,
		ByStorageClass:     map[string]string{},
		PVCsByStorageClass: map[string]int{},
	})

	if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
		t.Fatalf("object-only quota check: %#v", plan.Checks)
	}

	for _, action := range client.Actions() {
		if action.GetResource().Resource == "limitranges" {
			t.Fatalf("object-only quota check listed LimitRanges: %#v", client.Actions())
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
		{
			SourcePVC:      domain.ObjectReference{Name: "small"},
			DestinationPVC: domain.ObjectReference{Name: "small-target"},
			Capacity:       "512Mi",
		},
		{
			SourcePVC:      domain.ObjectReference{Name: "large"},
			DestinationPVC: domain.ObjectReference{Name: "large-target"},
			Capacity:       "3Gi",
		},
		{
			SourcePVC:      domain.ObjectReference{Name: "broken"},
			DestinationPVC: domain.ObjectReference{Name: "broken-target"},
			Capacity:       "invalid",
		},
	}, 0)

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
	planner.checkLimitRanges(
		context.Background(),
		plan,
		"stage",
		[]domain.PlannedVolume{{Capacity: "1Gi"}},
		1,
	)

	if plan.Ready || len(plan.Checks) != 1 ||
		!strings.Contains(plan.Checks[0].Message, "tool Pod resource cpu request 0") {
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
	planner.checkLimitRanges(
		context.Background(),
		plan,
		"stage",
		[]domain.PlannedVolume{{Capacity: "1Gi"}},
		1,
	)

	if plan.Ready || len(plan.Checks) != 1 ||
		!strings.Contains(plan.Checks[0].Message, "tool container") {
		t.Fatalf("container minimum check: %#v", plan.Checks)
	}
}

func TestCheckLimitRangesModelsToolDefaultsAndMaxRequestRatio(t *testing.T) {
	newLimitRange := func(ratio corev1.ResourceList) *corev1.LimitRange {
		return &corev1.LimitRange{
			ObjectMeta: metav1.ObjectMeta{Namespace: "stage", Name: "container-policy"},
			Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{
				{
					Type: corev1.LimitTypeContainer,
					Min: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("0"),
					},
					Max: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("1"),
					},
					Default: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("250m"),
					},
					DefaultRequest: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"),
					},
					MaxLimitRequestRatio: ratio,
				},
			}},
		}
	}

	t.Run("defaults and max permit explicit zero", func(t *testing.T) {
		plan := &domain.MigrationPlan{Ready: true}
		New(
			kubernetesfake.NewClientset(newLimitRange(nil)),
			nil,
		).checkLimitRanges(
			context.Background(),
			plan,
			"stage",
			[]domain.PlannedVolume{{Capacity: "1Gi"}},
			1,
		)

		if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
			t.Fatalf("limit check: %#v", plan.Checks)
		}
	})

	t.Run("max request ratio rejects zero pair", func(t *testing.T) {
		plan := &domain.MigrationPlan{Ready: true}
		New(
			kubernetesfake.NewClientset(
				newLimitRange(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}),
			),
			nil,
		).checkLimitRanges(
			context.Background(),
			plan,
			"stage",
			[]domain.PlannedVolume{{Capacity: "1Gi"}},
			1,
		)

		if plan.Ready || len(plan.Checks) != 1 ||
			!strings.Contains(plan.Checks[0].Message, "maxLimitRequestRatio") {
			t.Fatalf("ratio check: %#v", plan.Checks)
		}
	})
}
