package kube

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	countResourcePrefix            = "count/"
	storageClassQuotaResourceInfix = ".storageclass.storage.k8s.io/"

	countPodsResource                   corev1.ResourceName = "count/pods"
	countPersistentVolumeClaimsResource corev1.ResourceName = "count/persistentvolumeclaims"
)

// NamespaceResourcePolicies contains ResourceQuota and LimitRange objects
// collected in one pass. Errors stay separate so callers can report both
// policy checks independently.
type NamespaceResourcePolicies struct {
	ResourceQuotas   *corev1.ResourceQuotaList
	LimitRanges      *corev1.LimitRangeList
	ResourceQuotaErr error
	LimitRangeErr    error
}

// ReadNamespaceResourcePolicies reads quota and, when requested, LimitRange
// state concurrently. The collected result can feed both LimitRange validation
// and quota projection without listing LimitRanges twice.
func ReadNamespaceResourcePolicies(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	includeLimitRanges bool,
) NamespaceResourcePolicies {
	var result NamespaceResourcePolicies

	var wg sync.WaitGroup
	wg.Go(func() {
		result.ResourceQuotas, result.ResourceQuotaErr = client.CoreV1().
			ResourceQuotas(namespace).
			List(ctx, metav1.ListOptions{})
	})

	if includeLimitRanges {
		wg.Go(func() {
			result.LimitRanges, result.LimitRangeErr = client.CoreV1().
				LimitRanges(namespace).
				List(ctx, metav1.ListOptions{})
		})
	}

	wg.Wait()

	return result
}

// ResourceQuotaReport describes the bounded resources evaluated for one
// namespace and every projected overflow found in admitted quota status.
type ResourceQuotaReport struct {
	Checked    int
	Violations []string
}

func resourceQuotaDemand(estimate domain.ResourceEstimate) (corev1.ResourceList, error) {
	storageValue := strings.TrimSpace(estimate.StorageRequests)
	if storageValue == "" {
		storageValue = "0"
	}

	storage, err := resource.ParseQuantity(storageValue)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorInternal,
			"quota estimate",
			"parse storage demand",
			err,
		)
	}

	result := corev1.ResourceList{}
	if storage.Sign() > 0 {
		result[corev1.ResourceRequestsStorage] = storage
	}

	addCount := func(name corev1.ResourceName, count int) {
		if count > 0 {
			result[name] = *resource.NewQuantity(int64(count), resource.DecimalSI)
		}
	}
	addCount(corev1.ResourcePersistentVolumeClaims, estimate.PVCs)
	addCount(corev1.ResourcePods, estimate.Pods)
	addCount(countPodsResource, estimate.Pods)
	addCount(corev1.ResourceName("count/deployments.apps"), estimate.Deployments)
	addCount(corev1.ResourceName("count/replicasets.apps"), estimate.ReplicaSets)
	addCount(corev1.ResourceServices, estimate.Services)
	addCount(corev1.ResourceServicesNodePorts, estimate.ServiceNodePorts)
	addCount(corev1.ResourceServicesLoadBalancers, estimate.ServiceLoadBalancers)
	addCount(corev1.ResourceSecrets, estimate.Secrets)
	addCount(corev1.ResourceConfigMaps, estimate.ConfigMaps)
	addCount(corev1.ResourceName("count/jobs.batch"), estimate.Jobs)
	addCount(corev1.ResourceName("count/services"), estimate.Services)
	addCount(corev1.ResourceName("count/endpoints"), estimate.Endpoints)
	addCount(
		corev1.ResourceName("count/endpointslices.discovery.k8s.io"),
		estimate.EndpointSlices,
	)
	addCount(corev1.ResourceName("count/secrets"), estimate.Secrets)
	addCount(corev1.ResourceName("count/configmaps"), estimate.ConfigMaps)
	addCount(corev1.ResourceName("count/serviceaccounts"), estimate.ServiceAccounts)
	addCount(countPersistentVolumeClaimsResource, estimate.PVCs)
	addCount(corev1.ResourceName("count/leases.coordination.k8s.io"), estimate.Leases)

	for class, value := range estimate.ByStorageClass {
		if class == "" {
			continue
		}

		quantity, parseErr := resource.ParseQuantity(value)
		if parseErr != nil {
			return nil, domain.WrapError(
				domain.ErrorInternal,
				"quota estimate",
				fmt.Sprintf("parse StorageClass %s demand", class),
				parseErr,
			)
		}

		classPVCs, known := estimate.PVCsByStorageClass[class]
		if !known {
			return nil, domain.NewError(
				domain.ErrorInternal,
				"quota estimate",
				fmt.Sprintf("StorageClass %s PVC demand is missing", class),
			)
		}

		if quantity.Sign() > 0 {
			result[storageClassQuotaResourceName(class, corev1.ResourceRequestsStorage)] = quantity
		}

		addCount(storageClassQuotaResourceName(
			class,
			corev1.ResourcePersistentVolumeClaims,
		), classPVCs)
	}

	return result, nil
}

func storageClassQuotaResourceName(
	class string,
	resourceName corev1.ResourceName,
) corev1.ResourceName {
	return corev1.ResourceName(class + storageClassQuotaResourceInfix + string(resourceName))
}

func splitStorageClassQuotaResourceName(
	name corev1.ResourceName,
) (string, corev1.ResourceName, bool) {
	class, resourceName, found := strings.Cut(string(name), storageClassQuotaResourceInfix)

	return class, corev1.ResourceName(resourceName), found
}

// EvaluateResourceQuotaCapacity projects one workflow's peak demand against
// the admitted hard and used values returned by the ResourceQuota controller.
func EvaluateResourceQuotaCapacity(
	namespace string,
	quotas []corev1.ResourceQuota,
	limitRanges []corev1.LimitRange,
	estimate domain.ResourceEstimate,
) (ResourceQuotaReport, error) {
	report := ResourceQuotaReport{}
	for _, quota := range quotas {
		quotaEstimate := estimate
		quotaEstimate.Pods = toolQuotaPodCount(quota, estimate)

		demand, err := resourceQuotaDemand(quotaEstimate)
		if err != nil {
			return ResourceQuotaReport{}, err
		}

		maps.Copy(demand, ToolComputeQuotaDemand(limitRanges, quotaEstimate.Pods))

		for name, requested := range demand {
			if !ToolQuotaResourceMatches(quota, name) {
				continue
			}

			hard, bounded := quota.Status.Hard[name]
			if !bounded {
				continue
			}

			report.Checked++

			used, known := quota.Status.Used[name]
			if !known {
				report.Violations = append(
					report.Violations,
					fmt.Sprintf(
						"%s/%s %s: quota status has no current usage",
						namespace,
						quota.Name,
						name,
					),
				)

				continue
			}

			projected := used.DeepCopy()
			projected.Add(requested)

			if projected.Cmp(hard) > 0 {
				report.Violations = append(
					report.Violations,
					fmt.Sprintf(
						"%s/%s %s: used %s + requested %s exceeds hard %s",
						namespace,
						quota.Name,
						name,
						used.String(),
						requested.String(),
						hard.String(),
					),
				)
			}
		}
	}

	sort.Strings(report.Violations)

	return report, nil
}
