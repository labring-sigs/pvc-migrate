package kube_test

import (
	"context"
	"strings"
	"testing"

	. "github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func admittedQuota(quota *corev1.ResourceQuota) *corev1.ResourceQuota {
	quota.Status.Hard = quota.Spec.Hard.DeepCopy()
	if quota.Status.Used == nil {
		quota.Status.Used = corev1.ResourceList{}
	}

	for name := range quota.Status.Hard {
		if _, exists := quota.Status.Used[name]; !exists {
			quota.Status.Used[name] = resource.MustParse("0")
		}
	}

	return quota
}

func TestCheckPVCAdmissionPoliciesProjectsReplacementNetUsage(t *testing.T) {
	client := fake.NewClientset(
		admittedQuota(&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "storage"},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceRequestsStorage: resource.MustParse(
					"2Gi",
				),
				corev1.ResourcePersistentVolumeClaims: resource.MustParse(
					"1",
				),
				corev1.ResourceName("slow.storageclass.storage.k8s.io/requests.storage"): resource.MustParse(
					"1Gi",
				),
				corev1.ResourceName("fast.storageclass.storage.k8s.io/requests.storage"): resource.MustParse(
					"2Gi",
				),
			}},
			Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
				corev1.ResourceRequestsStorage: resource.MustParse(
					"1Gi",
				),
				corev1.ResourcePersistentVolumeClaims: resource.MustParse(
					"1",
				),
				corev1.ResourceName("slow.storageclass.storage.k8s.io/requests.storage"): resource.MustParse(
					"1Gi",
				),
			}},
		}),
	)

	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{
		{
			Namespace:             "app",
			Name:                  "data",
			RequestedStorage:      resource.MustParse("2Gi"),
			RequestedStorageClass: "fast",
			Existing:              true,
			ExistingStorage:       resource.MustParse("1Gi"),
			ExistingStorageClass:  "slow",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 0 {
		t.Fatalf(
			"replacement was rejected despite fitting projected quota: %v",
			report.QuotaViolations,
		)
	}
}

func TestCheckPVCAdmissionPoliciesUsesAdmittedQuotaStatus(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "storage"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
		}},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceRequestsStorage: resource.MustParse("2Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
			},
		},
	}

	report, err := CheckPVCAdmissionPolicies(
		context.Background(),
		fake.NewClientset(quota),
		[]PVCAdmissionChange{{
			Namespace:        "app",
			Name:             "data",
			RequestedStorage: resource.MustParse("1Gi"),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 0 {
		t.Fatalf("stale quota spec overrode admitted status: %v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesIgnoresUnrelatedUnknownQuotaUsage(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "pods"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourcePods: resource.MustParse("10"),
			},
		},
	}

	report, err := CheckPVCAdmissionPolicies(
		context.Background(),
		fake.NewClientset(quota),
		[]PVCAdmissionChange{{
			Namespace:        "app",
			Name:             "data",
			RequestedStorage: resource.MustParse("1Gi"),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 0 {
		t.Fatalf("unrelated quota status blocked PVC admission: %v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesIgnoresPodScopedObjectQuota(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "best-effort"},
		Spec: corev1.ResourceQuotaSpec{
			Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeBestEffort},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceName("count/persistentvolumeclaims"): resource.MustParse("0"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceName("count/persistentvolumeclaims"): resource.MustParse("0"),
			},
		},
	}

	report, err := CheckPVCAdmissionPolicies(
		context.Background(),
		fake.NewClientset(quota),
		[]PVCAdmissionChange{{
			Namespace:        "app",
			Name:             "data",
			RequestedStorage: resource.MustParse("1Gi"),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 0 {
		t.Fatalf("Pod-scoped quota blocked PVC admission: %v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesMatchesVolumeAttributesClassScopes(t *testing.T) {
	tests := []struct {
		name       string
		selector   corev1.ScopedResourceSelectorRequirement
		classes    []string
		violations int
	}{
		{
			name: "exists ignores unclassified PVC",
			selector: corev1.ScopedResourceSelectorRequirement{
				ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
				Operator:  corev1.ScopeSelectorOpExists,
			},
		},
		{
			name: "does not exist matches unclassified PVC",
			selector: corev1.ScopedResourceSelectorRequirement{
				ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
				Operator:  corev1.ScopeSelectorOpDoesNotExist,
			},
			violations: 1,
		},
		{
			name: "in matches selected class",
			selector: corev1.ScopedResourceSelectorRequirement{
				ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
				Operator:  corev1.ScopeSelectorOpIn,
				Values:    []string{"gold"},
			},
			classes:    []string{"gold"},
			violations: 1,
		},
		{
			name: "not in includes missing class",
			selector: corev1.ScopedResourceSelectorRequirement{
				ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
				Operator:  corev1.ScopeSelectorOpNotIn,
				Values:    []string{"gold"},
			},
			violations: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			quota := &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "vac"},
				Spec: corev1.ResourceQuotaSpec{ScopeSelector: &corev1.ScopeSelector{
					MatchExpressions: []corev1.ScopedResourceSelectorRequirement{test.selector},
				}},
				Status: corev1.ResourceQuotaStatus{
					Hard: corev1.ResourceList{
						corev1.ResourceRequestsStorage: resource.MustParse("0"),
					},
					Used: corev1.ResourceList{
						corev1.ResourceRequestsStorage: resource.MustParse("0"),
					},
				},
			}

			report, err := CheckPVCAdmissionPolicies(
				context.Background(),
				fake.NewClientset(quota),
				[]PVCAdmissionChange{{
					Namespace:                           "app",
					Name:                                "data",
					RequestedStorage:                    resource.MustParse("1Gi"),
					RequestedVolumeAttributesClassNames: test.classes,
				}},
			)
			if err != nil {
				t.Fatal(err)
			}

			if len(report.QuotaViolations) != test.violations {
				t.Fatalf("quota violations=%v, want %d", report.QuotaViolations, test.violations)
			}
		})
	}
}

func TestCheckPVCAdmissionPoliciesProjectsExistingVolumeAttributesClasses(t *testing.T) {
	currentClass := "gold"
	existing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "data",
			UID:       types.UID("source-uid"),
		},
		Status: corev1.PersistentVolumeClaimStatus{
			CurrentVolumeAttributesClassName: &currentClass,
			ModifyVolumeStatus: &corev1.ModifyVolumeStatus{
				TargetVolumeAttributesClassName: "silver",
			},
		},
	}
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "gold"},
		Spec: corev1.ResourceQuotaSpec{ScopeSelector: &corev1.ScopeSelector{
			MatchExpressions: []corev1.ScopedResourceSelectorRequirement{{
				ScopeName: corev1.ResourceQuotaScopeVolumeAttributesClass,
				Operator:  corev1.ScopeSelectorOpIn,
				Values:    []string{"gold"},
			}},
		}},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
			},
		},
	}

	report, err := CheckPVCAdmissionPolicies(
		context.Background(),
		fake.NewClientset(existing, quota),
		[]PVCAdmissionChange{{
			Namespace:                           "app",
			Name:                                "data",
			RequestedStorage:                    resource.MustParse("1Gi"),
			Existing:                            true,
			ExistingUID:                         existing.UID,
			ExistingStorage:                     resource.MustParse("1Gi"),
			RequestedVolumeAttributesClassNames: []string{"gold"},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 0 {
		t.Fatalf("replacement failed to subtract current VAC usage: %v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesUsesAllocatedStorageForExistingPVC(t *testing.T) {
	existing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "data",
			UID:       types.UID("pvc-uid"),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{AllocatedResources: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("3Gi"),
		}},
	}
	quota := admittedQuota(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "storage"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceRequestsStorage: resource.MustParse("3Gi"),
		}},
		Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
			corev1.ResourceRequestsStorage: resource.MustParse("3Gi"),
		}},
	})

	report, err := CheckPVCAdmissionPolicies(
		context.Background(),
		fake.NewClientset(existing, quota),
		[]PVCAdmissionChange{{
			Namespace:        "app",
			Name:             "data",
			RequestedStorage: resource.MustParse("3Gi"),
			Existing:         true,
			ExistingUID:      existing.UID,
			ExistingStorage:  resource.MustParse("1Gi"),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 0 {
		t.Fatalf("allocated storage caused a false quota violation: %v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesRejectsMissingIdentifiedPVC(t *testing.T) {
	quota := admittedQuota(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "storage"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceRequestsStorage: resource.MustParse("3Gi"),
		}},
	})

	_, err := CheckPVCAdmissionPolicies(
		context.Background(),
		fake.NewClientset(quota),
		[]PVCAdmissionChange{{
			Namespace:        "app",
			Name:             "data",
			RequestedStorage: resource.MustParse("1Gi"),
			Existing:         true,
			ExistingUID:      types.UID("pvc-uid"),
			ExistingStorage:  resource.MustParse("1Gi"),
		}},
	)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("error=%v, want NotFound", err)
	}
}

func TestCheckPVCAdmissionPoliciesReportsQuotaAndLimitRangeOverflow(t *testing.T) {
	client := fake.NewClientset(
		admittedQuota(&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "storage"},
			Spec: corev1.ResourceQuotaSpec{
				Hard: corev1.ResourceList{
					corev1.ResourceRequestsStorage: resource.MustParse("2Gi"),
				},
			},
			Status: corev1.ResourceQuotaStatus{
				Used: corev1.ResourceList{
					corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
				},
			},
		}),
		&corev1.LimitRange{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "pvc-size"},
			Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
				Type: corev1.LimitTypePersistentVolumeClaim,
				Max:  corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
			}}},
		},
	)

	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{
		{
			Namespace:             "app",
			Name:                  "data",
			RequestedStorage:      resource.MustParse("3Gi"),
			RequestedStorageClass: "fast",
			Existing:              true,
			ExistingStorage:       resource.MustParse("1Gi"),
			ExistingStorageClass:  "fast",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 1 ||
		!strings.Contains(report.QuotaViolations[0], "requests.storage") {
		t.Fatalf("quota violations=%v", report.QuotaViolations)
	}

	if len(report.LimitRangeViolations) != 1 ||
		!strings.Contains(report.LimitRangeViolations[0], "above") {
		t.Fatalf("limit range violations=%v", report.LimitRangeViolations)
	}
}

func TestCheckPVCAdmissionPoliciesCountsNewClaimWhenOldClaimIsGone(t *testing.T) {
	client := fake.NewClientset(admittedQuota(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "claims"},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Used: corev1.ResourceList{
				corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
			},
		},
	}))

	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{
		{
			Namespace:        "app",
			Name:             "data",
			RequestedStorage: resource.MustParse("1Gi"),
			Existing:         false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 1 ||
		!strings.Contains(report.QuotaViolations[0], "persistentvolumeclaims") {
		t.Fatalf("quota violations=%v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesProjectsWildcardStorageClassQuota(t *testing.T) {
	client := fake.NewClientset(admittedQuota(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "all-storage"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/requests.storage"): resource.MustParse(
				"2Gi",
			),
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse(
				"1",
			),
		}},
		Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/requests.storage"): resource.MustParse(
				"1Gi",
			),
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse(
				"1",
			),
		}},
	}))

	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{
		{
			Namespace:             "app",
			Name:                  "data",
			RequestedStorage:      resource.MustParse("2Gi"),
			RequestedStorageClass: "fast",
			Existing:              true,
			ExistingStorage:       resource.MustParse("1Gi"),
			ExistingStorageClass:  "slow",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 0 {
		t.Fatalf("wildcard quota rejected an in-place replacement: %v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesAggregatesMultipleReplacementsForWildcardQuota(t *testing.T) {
	client := fake.NewClientset(admittedQuota(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "all-storage"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/requests.storage"): resource.MustParse(
				"3Gi",
			),
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse(
				"1",
			),
		}},
		Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/requests.storage"): resource.MustParse(
				"1Gi",
			),
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse(
				"1",
			),
		}},
	}))

	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{
		{
			Namespace:             "app",
			Name:                  "data",
			RequestedStorage:      resource.MustParse("1Gi"),
			RequestedStorageClass: "fast",
			Existing:              true,
			ExistingStorage:       resource.MustParse("1Gi"),
			ExistingStorageClass:  "slow",
		},
		{
			Namespace:             "app",
			Name:                  "logs",
			RequestedStorage:      resource.MustParse("3Gi"),
			RequestedStorageClass: "fast",
			Existing:              false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 2 {
		t.Fatalf("wildcard quota violations=%v", report.QuotaViolations)
	}

	if !strings.Contains(strings.Join(report.QuotaViolations, "; "), "requests.storage") ||
		!strings.Contains(strings.Join(report.QuotaViolations, "; "), "persistentvolumeclaims") {
		t.Fatalf("wildcard quota violations missing resources=%v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesKeepsWildcardCountForUnclassifiedReplacement(t *testing.T) {
	client := fake.NewClientset(admittedQuota(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "all-claims"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse(
				"1",
			),
		}},
		Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse(
				"1",
			),
		}},
	}))

	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{
		{
			Namespace:             "app",
			Name:                  "data",
			RequestedStorage:      resource.MustParse("1Gi"),
			RequestedStorageClass: "fast",
			Existing:              true,
			ExistingStorage:       resource.MustParse("1Gi"),
			ExistingStorageClass:  "",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(report.QuotaViolations) != 0 {
		t.Fatalf(
			"wildcard count quota rejected an in-place unclassified replacement: %v",
			report.QuotaViolations,
		)
	}
}
