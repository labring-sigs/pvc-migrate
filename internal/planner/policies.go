package planner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (p *Planner) checkLimitRanges(
	ctx context.Context,
	plan *domain.MigrationPlan,
	namespace string,
	volumes []domain.PlannedVolume,
) {
	p.logInfo("checking LimitRanges", "namespace", namespace, "volumes", len(volumes))

	items, err := p.client.CoreV1().LimitRanges(namespace).List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		plan.AddCheck(
			warned(
				"limit-range",
				fmt.Sprintf(
					"namespace %s does not exist yet; reservation will create it",
					namespace,
				),
			),
		)

		return
	}

	if err != nil {
		plan.AddCheck(
			failed("limit-range", fmt.Sprintf("list LimitRanges in %s: %v", namespace, err)),
		)
		return
	}

	if items == nil {
		plan.AddCheck(
			failed(
				"limit-range",
				fmt.Sprintf("list LimitRanges in %s returned an empty object", namespace),
			),
		)

		return
	}

	violations := make([]string, 0)

	zero := resource.MustParse("0")
	for _, limitRange := range items.Items {
		for _, item := range limitRange.Spec.Limits {
			if item.Type == corev1.LimitTypeContainer || item.Type == corev1.LimitTypePod {
				for _, resourceName := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
					if minimum, ok := item.Min[resourceName]; ok && minimum.Cmp(zero) > 0 {
						scope := "tool Pod"
						if item.Type == corev1.LimitTypeContainer {
							scope = "tool container"
						}

						violations = append(
							violations,
							fmt.Sprintf(
								"%s resource %s=0 is below %s minimum %s",
								scope,
								resourceName,
								limitRange.Name,
								minimum.String(),
							),
						)
					}

					if ratio, ok := item.MaxLimitRequestRatio[resourceName]; ok &&
						ratio.Cmp(zero) > 0 {
						scope := "tool Pod"
						if item.Type == corev1.LimitTypeContainer {
							scope = "tool container"
						}
						// Tool containers explicitly set both request and limit to zero. Kubernetes
						// rejects that pair when a LimitRange requires a non-zero ratio.
						violations = append(
							violations,
							fmt.Sprintf(
								"%s resource %s=0/0 violates %s maxLimitRequestRatio %s",
								scope,
								resourceName,
								limitRange.Name,
								ratio.String(),
							),
						)
					}
				}

				continue
			}

			if item.Type != corev1.LimitTypePersistentVolumeClaim {
				continue
			}

			for _, volume := range volumes {
				capacity, parseErr := resource.ParseQuantity(volume.Capacity)
				if parseErr != nil {
					violations = append(
						violations,
						fmt.Sprintf(
							"%s has invalid capacity %q",
							volume.SourcePVC.Name,
							volume.Capacity,
						),
					)

					continue
				}

				if minimum, ok := item.Min[corev1.ResourceStorage]; ok &&
					capacity.Cmp(minimum) < 0 {
					violations = append(
						violations,
						fmt.Sprintf(
							"%s requires %s below %s minimum %s",
							volume.DestinationPVC.Name,
							capacity.String(),
							limitRange.Name,
							minimum.String(),
						),
					)
				}

				if maximum, ok := item.Max[corev1.ResourceStorage]; ok &&
					capacity.Cmp(maximum) > 0 {
					violations = append(
						violations,
						fmt.Sprintf(
							"%s requires %s above %s maximum %s",
							volume.DestinationPVC.Name,
							capacity.String(),
							limitRange.Name,
							maximum.String(),
						),
					)
				}
			}
		}
	}

	if len(violations) > 0 {
		plan.AddCheck(failed("limit-range", strings.Join(violations, "; ")))
		return
	}

	plan.AddCheck(
		passed(
			"limit-range",
			fmt.Sprintf("%d LimitRange object(s) permit all target PVC requests", len(items.Items)),
		),
	)
}

func (p *Planner) checkQuotas(
	ctx context.Context,
	plan *domain.MigrationPlan,
	namespace string,
	estimate domain.ResourceEstimate,
) {
	p.logInfo("checking ResourceQuotas", "namespace", namespace)

	items, err := p.client.CoreV1().ResourceQuotas(namespace).List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		plan.AddCheck(
			warned(
				"resource-quota",
				fmt.Sprintf(
					"namespace %s does not exist yet; no quota is currently applied",
					namespace,
				),
			),
		)

		return
	}

	if err != nil {
		plan.AddCheck(
			failed("resource-quota", fmt.Sprintf("list ResourceQuotas in %s: %v", namespace, err)),
		)
		return
	}

	if items == nil {
		plan.AddCheck(
			failed(
				"resource-quota",
				fmt.Sprintf("list ResourceQuotas in %s returned an empty object", namespace),
			),
		)

		return
	}

	demand, err := quotaDemand(estimate)
	if err != nil {
		plan.AddCheck(failed("resource-quota", err.Error()))
		return
	}

	violations := make([]string, 0)

	checked := 0
	for _, quota := range items.Items {
		hard := quota.Spec.Hard
		for name, requested := range demand {
			limit, bounded := hard[name]
			if !bounded {
				continue
			}

			checked++
			used := quota.Status.Used[name]
			total := used.DeepCopy()
			total.Add(requested)

			if total.Cmp(limit) > 0 {
				violations = append(
					violations,
					fmt.Sprintf(
						"%s/%s %s: used %s + requested %s exceeds hard %s",
						namespace,
						quota.Name,
						name,
						used.String(),
						requested.String(),
						limit.String(),
					),
				)
			}
		}
	}

	sort.Strings(violations)

	if len(violations) > 0 {
		plan.AddCheck(failed("resource-quota", strings.Join(violations, "; ")))
		return
	}

	plan.AddCheck(
		passed(
			"resource-quota",
			fmt.Sprintf(
				"%d ResourceQuota object(s), %d bounded resource(s), all have capacity",
				len(items.Items),
				checked,
			),
		),
	)
}

func quotaDemand(estimate domain.ResourceEstimate) (corev1.ResourceList, error) {
	storage, err := resource.ParseQuantity(estimate.StorageRequests)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorInternal,
			"quota estimate",
			"parse storage demand",
			err,
		)
	}

	result := corev1.ResourceList{
		corev1.ResourceRequestsStorage:                          storage,
		corev1.ResourcePersistentVolumeClaims:                   *resource.NewQuantity(int64(estimate.PVCs), resource.DecimalSI),
		corev1.ResourcePods:                                     *resource.NewQuantity(int64(estimate.Pods), resource.DecimalSI),
		corev1.ResourceServices:                                 *resource.NewQuantity(int64(estimate.Services), resource.DecimalSI),
		corev1.ResourceSecrets:                                  *resource.NewQuantity(int64(estimate.Secrets), resource.DecimalSI),
		corev1.ResourceConfigMaps:                               *resource.NewQuantity(int64(estimate.ConfigMaps), resource.DecimalSI),
		corev1.ResourceName("count/jobs.batch"):                 *resource.NewQuantity(int64(estimate.Jobs), resource.DecimalSI),
		corev1.ResourceName("count/services"):                   *resource.NewQuantity(int64(estimate.Services), resource.DecimalSI),
		corev1.ResourceName("count/secrets"):                    *resource.NewQuantity(int64(estimate.Secrets), resource.DecimalSI),
		corev1.ResourceName("count/configmaps"):                 *resource.NewQuantity(int64(estimate.ConfigMaps), resource.DecimalSI),
		corev1.ResourceName("count/serviceaccounts"):            *resource.NewQuantity(int64(estimate.ServiceAccounts), resource.DecimalSI),
		corev1.ResourceName("count/persistentvolumeclaims"):     *resource.NewQuantity(int64(estimate.PVCs), resource.DecimalSI),
		corev1.ResourceName("count/leases.coordination.k8s.io"): *resource.NewQuantity(int64(estimate.Leases), resource.DecimalSI),
	}
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

		result[corev1.ResourceName(class+".storageclass.storage.k8s.io/requests.storage")] = quantity

		classPVCs, known := estimate.PVCsByStorageClass[class]
		if !known {
			return nil, domain.NewError(
				domain.ErrorInternal,
				"quota estimate",
				fmt.Sprintf("StorageClass %s PVC demand is missing", class),
			)
		}

		result[corev1.ResourceName(class+".storageclass.storage.k8s.io/persistentvolumeclaims")] = *resource.NewQuantity(int64(classPVCs), resource.DecimalSI)
	}

	return result, nil
}

func (p *Planner) checkNetworkPolicies(
	ctx context.Context,
	plan *domain.MigrationPlan,
	namespaces ...string,
) {
	p.logInfo("checking NetworkPolicies", "namespaces", namespaces)
	unique := make([]string, 0, len(namespaces))

	seen := map[string]struct{}{}
	for _, namespace := range namespaces {
		if _, ok := seen[namespace]; ok {
			continue
		}

		seen[namespace] = struct{}{}
		unique = append(unique, namespace)
	}

	type result struct {
		namespace string
		count     int
		empty     bool
		err       error
	}

	results := make([]result, len(unique))
	parallel.For(len(unique), func(index int) {
		namespace := unique[index]
		policies, err := p.client.NetworkingV1().
			NetworkPolicies(namespace).
			List(ctx, metav1.ListOptions{})

		count := 0
		if policies != nil {
			count = len(policies.Items)
		}

		results[index] = result{
			namespace: namespace,
			count:     count,
			empty:     policies == nil,
			err:       err,
		}
	})

	for _, result := range results {
		namespace, err := result.namespace, result.err
		if apierrors.IsNotFound(err) {
			continue
		}

		if err != nil {
			plan.AddCheck(
				failed(
					"network-policy",
					fmt.Sprintf("list NetworkPolicies in %s: %v", namespace, err),
				),
			)

			continue
		}

		if result.empty {
			plan.AddCheck(
				failed(
					"network-policy",
					fmt.Sprintf("list NetworkPolicies in %s returned an empty object", namespace),
				),
			)

			continue
		}

		if result.count > 0 {
			plan.AddCheck(
				warned(
					"network-policy",
					fmt.Sprintf(
						"namespace %s has %d NetworkPolicy object(s); clusterip copy connectivity will be validated by the real copy job",
						namespace,
						result.count,
					),
				),
			)
		} else {
			plan.AddCheck(
				passed(
					"network-policy",
					fmt.Sprintf("namespace %s has no NetworkPolicy objects", namespace),
				),
			)
		}
	}
}
