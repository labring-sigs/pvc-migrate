package kube

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCheckPVCAdmissionPoliciesProjectsReplacementNetUsage(t *testing.T) {
	client := fake.NewClientset(
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "storage"},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceRequestsStorage:                                           resource.MustParse("2Gi"),
				corev1.ResourcePersistentVolumeClaims:                                    resource.MustParse("1"),
				corev1.ResourceName("slow.storageclass.storage.k8s.io/requests.storage"): resource.MustParse("1Gi"),
				corev1.ResourceName("fast.storageclass.storage.k8s.io/requests.storage"): resource.MustParse("2Gi"),
			}},
			Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
				corev1.ResourceRequestsStorage:                                           resource.MustParse("1Gi"),
				corev1.ResourcePersistentVolumeClaims:                                    resource.MustParse("1"),
				corev1.ResourceName("slow.storageclass.storage.k8s.io/requests.storage"): resource.MustParse("1Gi"),
			}},
		},
	)
	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{{
		Namespace: "app", Name: "data", RequestedStorage: resource.MustParse("2Gi"), RequestedStorageClass: "fast",
		Existing: true, ExistingStorage: resource.MustParse("1Gi"), ExistingStorageClass: "slow",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.QuotaViolations) != 0 {
		t.Fatalf("replacement was rejected despite fitting projected quota: %v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesReportsQuotaAndLimitRangeOverflow(t *testing.T) {
	client := fake.NewClientset(
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "storage"},
			Spec:       corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourceRequestsStorage: resource.MustParse("2Gi")}},
			Status:     corev1.ResourceQuotaStatus{Used: corev1.ResourceList{corev1.ResourceRequestsStorage: resource.MustParse("1Gi")}},
		},
		&corev1.LimitRange{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "pvc-size"},
			Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
				Type: corev1.LimitTypePersistentVolumeClaim,
				Max:  corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
			}}},
		},
	)
	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{{
		Namespace: "app", Name: "data", RequestedStorage: resource.MustParse("3Gi"), RequestedStorageClass: "fast",
		Existing: true, ExistingStorage: resource.MustParse("1Gi"), ExistingStorageClass: "fast",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.QuotaViolations) != 1 || !strings.Contains(report.QuotaViolations[0], "requests.storage") {
		t.Fatalf("quota violations=%v", report.QuotaViolations)
	}
	if len(report.LimitRangeViolations) != 1 || !strings.Contains(report.LimitRangeViolations[0], "above") {
		t.Fatalf("limit range violations=%v", report.LimitRangeViolations)
	}
}

func TestCheckPVCAdmissionPoliciesCountsNewClaimWhenOldClaimIsGone(t *testing.T) {
	client := fake.NewClientset(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "claims"},
		Spec:       corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourcePersistentVolumeClaims: resource.MustParse("1")}},
		Status:     corev1.ResourceQuotaStatus{Used: corev1.ResourceList{corev1.ResourcePersistentVolumeClaims: resource.MustParse("1")}},
	})
	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{{
		Namespace: "app", Name: "data", RequestedStorage: resource.MustParse("1Gi"), Existing: false,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.QuotaViolations) != 1 || !strings.Contains(report.QuotaViolations[0], "persistentvolumeclaims") {
		t.Fatalf("quota violations=%v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesProjectsWildcardStorageClassQuota(t *testing.T) {
	client := fake.NewClientset(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "all-storage"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/requests.storage"):       resource.MustParse("2Gi"),
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse("1"),
		}},
		Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/requests.storage"):       resource.MustParse("1Gi"),
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse("1"),
		}},
	})
	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{{
		Namespace: "app", Name: "data", RequestedStorage: resource.MustParse("2Gi"), RequestedStorageClass: "fast",
		Existing: true, ExistingStorage: resource.MustParse("1Gi"), ExistingStorageClass: "slow",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.QuotaViolations) != 0 {
		t.Fatalf("wildcard quota rejected an in-place replacement: %v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesAggregatesMultipleReplacementsForWildcardQuota(t *testing.T) {
	client := fake.NewClientset(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "all-storage"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/requests.storage"):       resource.MustParse("3Gi"),
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse("1"),
		}},
		Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/requests.storage"):       resource.MustParse("1Gi"),
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse("1"),
		}},
	})
	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{
		{Namespace: "app", Name: "data", RequestedStorage: resource.MustParse("1Gi"), RequestedStorageClass: "fast", Existing: true, ExistingStorage: resource.MustParse("1Gi"), ExistingStorageClass: "slow"},
		{Namespace: "app", Name: "logs", RequestedStorage: resource.MustParse("3Gi"), RequestedStorageClass: "fast", Existing: false},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.QuotaViolations) != 2 {
		t.Fatalf("wildcard quota violations=%v", report.QuotaViolations)
	}
	if !strings.Contains(strings.Join(report.QuotaViolations, "; "), "requests.storage") || !strings.Contains(strings.Join(report.QuotaViolations, "; "), "persistentvolumeclaims") {
		t.Fatalf("wildcard quota violations missing resources=%v", report.QuotaViolations)
	}
}

func TestCheckPVCAdmissionPoliciesKeepsWildcardCountForUnclassifiedReplacement(t *testing.T) {
	client := fake.NewClientset(&corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "all-claims"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse("1"),
		}},
		Status: corev1.ResourceQuotaStatus{Used: corev1.ResourceList{
			corev1.ResourceName(".storageclass.storage.k8s.io/persistentvolumeclaims"): resource.MustParse("1"),
		}},
	})
	report, err := CheckPVCAdmissionPolicies(context.Background(), client, []PVCAdmissionChange{{
		Namespace: "app", Name: "data", RequestedStorage: resource.MustParse("1Gi"), RequestedStorageClass: "fast",
		Existing: true, ExistingStorage: resource.MustParse("1Gi"), ExistingStorageClass: "",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.QuotaViolations) != 0 {
		t.Fatalf("wildcard count quota rejected an in-place unclassified replacement: %v", report.QuotaViolations)
	}
}
