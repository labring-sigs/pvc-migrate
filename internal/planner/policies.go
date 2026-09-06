package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	limitRangeCheckName    = "limit-range"
	resourceQuotaCheckName = "resource-quota"
)

func (p *Planner) checkLimitRanges(
	plan *domain.MigrationPlan,
	namespace string,
	volumes []domain.PlannedVolume,
	toolPods int,
	policies kube.NamespaceResourcePolicies,
) {
	p.logInfo("checking LimitRanges", "namespace", namespace, "volumes", len(volumes))

	items, err := policies.LimitRanges, policies.LimitRangeErr
	if err != nil {
		plan.AddCheck(
			failed(limitRangeCheckName, fmt.Sprintf("list LimitRanges in %s: %v", namespace, err)),
		)
		return
	}

	if items == nil {
		plan.AddCheck(
			failed(
				limitRangeCheckName,
				fmt.Sprintf("list LimitRanges in %s returned an empty object", namespace),
			),
		)

		return
	}

	violations := make([]string, 0)

	if toolPods > 0 {
		violations = append(violations, kube.ToolLimitRangeViolations(items.Items)...)
	}

	for _, limitRange := range items.Items {
		for _, item := range limitRange.Spec.Limits {
			if item.Type == corev1.LimitTypeContainer || item.Type == corev1.LimitTypePod {
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
		plan.AddCheck(
			failed(
				limitRangeCheckName,
				fmt.Sprintf("namespace %s: %s", namespace, strings.Join(violations, "; ")),
			),
		)

		return
	}

	plan.AddCheck(
		passed(
			limitRangeCheckName,
			fmt.Sprintf(
				"namespace %s: %d LimitRange object(s) permit all target PVC requests",
				namespace,
				len(items.Items),
			),
		),
	)
}

func (p *Planner) checkQuotas(
	plan *domain.MigrationPlan,
	namespace string,
	estimate domain.ResourceEstimate,
	policies kube.NamespaceResourcePolicies,
) {
	p.logInfo("checking ResourceQuotas", "namespace", namespace)

	items, err := policies.ResourceQuotas, policies.ResourceQuotaErr
	if err != nil {
		plan.AddCheck(
			failed(
				resourceQuotaCheckName,
				fmt.Sprintf("list ResourceQuotas in %s: %v", namespace, err),
			),
		)

		return
	}

	if items == nil {
		plan.AddCheck(
			failed(
				resourceQuotaCheckName,
				fmt.Sprintf("list ResourceQuotas in %s returned an empty object", namespace),
			),
		)

		return
	}

	var limitRanges []corev1.LimitRange
	if estimate.Pods > 0 {
		if policies.LimitRangeErr != nil {
			plan.AddCheck(
				failed(
					resourceQuotaCheckName,
					fmt.Sprintf(
						"list LimitRanges for ResourceQuota evaluation in %s: %v",
						namespace,
						policies.LimitRangeErr,
					),
				),
			)

			return
		}

		if policies.LimitRanges == nil {
			plan.AddCheck(
				failed(
					resourceQuotaCheckName,
					fmt.Sprintf(
						"list LimitRanges for ResourceQuota evaluation in %s returned an empty object",
						namespace,
					),
				),
			)

			return
		}

		limitRanges = policies.LimitRanges.Items
	}

	report, err := kube.EvaluateResourceQuotaCapacity(
		namespace,
		items.Items,
		limitRanges,
		estimate,
	)
	if err != nil {
		plan.AddCheck(failed(resourceQuotaCheckName, err.Error()))
		return
	}

	if len(report.Violations) > 0 {
		plan.AddCheck(failed(resourceQuotaCheckName, strings.Join(report.Violations, "; ")))
		return
	}

	plan.AddCheck(
		passed(
			resourceQuotaCheckName,
			fmt.Sprintf(
				"namespace %s: %d ResourceQuota object(s), %d bounded resource(s), all have capacity",
				namespace,
				len(items.Items),
				report.Checked,
			),
		),
	)
}

func (p *Planner) checkNamespaceResourcePolicies(
	ctx context.Context,
	plan *domain.MigrationPlan,
	namespace string,
	volumes []domain.PlannedVolume,
	estimate domain.ResourceEstimate,
) {
	includeLimitRanges := len(volumes) > 0 || estimate.Pods > 0
	policies := kube.ReadNamespaceResourcePolicies(
		ctx,
		p.client,
		namespace,
		includeLimitRanges,
	)

	if includeLimitRanges {
		p.checkLimitRanges(plan, namespace, volumes, estimate.Pods, policies)
	}

	p.checkQuotas(plan, namespace, estimate, policies)
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
		if err != nil {
			plan.AddCheck(
				failed(
					domain.CheckNameNetworkPolicy,
					fmt.Sprintf("list NetworkPolicies in %s: %v", namespace, err),
				),
			)

			continue
		}

		if result.empty {
			plan.AddCheck(
				failed(
					domain.CheckNameNetworkPolicy,
					fmt.Sprintf("list NetworkPolicies in %s returned an empty object", namespace),
				),
			)

			continue
		}

		if result.count > 0 {
			plan.AddCheck(
				warned(
					domain.CheckNameNetworkPolicy,
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
					domain.CheckNameNetworkPolicy,
					fmt.Sprintf("namespace %s has no NetworkPolicy objects", namespace),
				),
			)
		}
	}
}
