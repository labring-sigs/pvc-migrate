package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
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
	toolPods int,
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

	var limitRanges []corev1.LimitRange
	if estimate.Pods > 0 {
		items, listErr := p.client.CoreV1().LimitRanges(namespace).List(ctx, metav1.ListOptions{})
		if listErr != nil {
			plan.AddCheck(
				failed(
					"resource-quota",
					fmt.Sprintf(
						"list LimitRanges for ResourceQuota evaluation in %s: %v",
						namespace,
						listErr,
					),
				),
			)

			return
		}

		if items == nil {
			plan.AddCheck(
				failed(
					"resource-quota",
					fmt.Sprintf(
						"list LimitRanges for ResourceQuota evaluation in %s returned an empty object",
						namespace,
					),
				),
			)

			return
		}

		limitRanges = items.Items
	}

	report, err := kube.EvaluateResourceQuotaCapacity(
		namespace,
		items.Items,
		limitRanges,
		estimate,
	)
	if err != nil {
		plan.AddCheck(failed("resource-quota", err.Error()))
		return
	}

	if len(report.Violations) > 0 {
		plan.AddCheck(failed("resource-quota", strings.Join(report.Violations, "; ")))
		return
	}

	plan.AddCheck(
		passed(
			"resource-quota",
			fmt.Sprintf(
				"%d ResourceQuota object(s), %d bounded resource(s), all have capacity",
				len(items.Items),
				report.Checked,
			),
		),
	)
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
