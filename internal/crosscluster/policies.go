package crosscluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func (s *Service) planCrossClusterPolicies(
	ctx context.Context,
	plan *Plan,
	options Options,
	destinationClass *storagev1.StorageClass,
) {
	if len(plan.Volumes) == 0 {
		return
	}

	// Transfers are serialized by execution.go, so one chart instance is the
	// peak even when the session contains several PVCs.
	sourceResources := kube.PVMigrateResourceEstimate(plan.Strategies, false, false)

	sessionResources := domain.ResourceEstimate{ConfigMaps: 1, Leases: 1}
	if options.SessionNamespace == options.SourceNamespace {
		sourceResources.ConfigMaps++
		sourceResources.Leases++
	} else {
		addCrossClusterQuotaCheck(
			ctx,
			plan,
			s.source.Kubernetes,
			options.SessionNamespace,
			"source-session",
			sessionResources,
		)
	}

	addCrossClusterToolPolicyChecks(
		ctx,
		plan,
		s.source.Kubernetes,
		options.SourceNamespace,
		"source-tool",
		sourceResources,
	)

	destinationResources := kube.PVMigrateResourceEstimate(
		plan.Strategies,
		false,
		true,
	)
	if destinationClass.VolumeBindingMode != nil &&
		*destinationClass.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer &&
		destinationResources.Pods < 1 {
		destinationResources.Pods = 1
		destinationResources.NotTerminatingPods = 1
	}

	addCrossClusterToolPolicyChecks(
		ctx,
		plan,
		s.destination.Kubernetes,
		options.DestinationNamespace,
		"destination-tool",
		destinationResources,
	)

	changes := make([]kube.PVCAdmissionChange, 0, len(plan.Volumes))
	for _, volume := range plan.Volumes {
		capacity, err := resource.ParseQuantity(volume.Capacity)
		if err != nil {
			plan.AddCheck(
				"destination-pvc-policy",
				false,
				fmt.Sprintf(
					"destination PVC %s/%s has invalid planned capacity %q",
					volume.DestinationNamespace,
					volume.DestinationPVC,
					volume.Capacity,
				),
			)

			return
		}

		changes = append(changes, kube.PVCAdmissionChange{
			Namespace:             volume.DestinationNamespace,
			Name:                  volume.DestinationPVC,
			RequestedStorage:      capacity,
			RequestedStorageClass: volume.StorageClass,
		})
	}

	report, err := kube.CheckPVCAdmissionPolicies(
		ctx,
		s.destination.Kubernetes,
		changes,
	)
	if err != nil {
		plan.AddCheck(
			"destination-pvc-policy",
			false,
			"evaluate destination PVC admission policies: "+err.Error(),
		)

		return
	}

	violations := append(
		append([]string(nil), report.QuotaViolations...),
		report.LimitRangeViolations...,
	)
	if len(violations) > 0 {
		plan.AddCheck(
			"destination-pvc-policy",
			false,
			strings.Join(violations, "; "),
		)

		return
	}

	plan.AddCheck(
		"destination-pvc-policy",
		true,
		fmt.Sprintf(
			"destination namespace policies permit %d PVC request(s)",
			len(changes),
		),
	)
}

func addCrossClusterToolPolicyChecks(
	ctx context.Context,
	plan *Plan,
	client kubernetes.Interface,
	namespace string,
	name string,
	estimate domain.ResourceEstimate,
) {
	limitRanges, err := client.CoreV1().LimitRanges(namespace).List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		plan.AddCheck(
			name+"-limit-range",
			true,
			fmt.Sprintf(
				"namespace %s does not exist yet; no LimitRange is currently applied",
				namespace,
			),
		)
	} else if err != nil {
		plan.AddCheck(
			name+"-limit-range",
			false,
			fmt.Sprintf("list LimitRanges in %s: %v", namespace, err),
		)
	} else if limitRanges == nil {
		plan.AddCheck(
			name+"-limit-range",
			false,
			fmt.Sprintf("list LimitRanges in %s returned an empty object", namespace),
		)
	} else if violations := kube.ToolLimitRangeViolations(limitRanges.Items); len(violations) > 0 {
		plan.AddCheck(name+"-limit-range", false, strings.Join(violations, "; "))
	} else {
		plan.AddCheck(
			name+"-limit-range",
			true,
			fmt.Sprintf("%d LimitRange object(s) permit tool resources", len(limitRanges.Items)),
		)
	}

	addCrossClusterQuotaCheck(ctx, plan, client, namespace, name, estimate)
}

func addCrossClusterQuotaCheck(
	ctx context.Context,
	plan *Plan,
	client kubernetes.Interface,
	namespace string,
	name string,
	estimate domain.ResourceEstimate,
) {
	quotas, err := client.CoreV1().ResourceQuotas(namespace).List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		plan.AddCheck(
			name+"-resource-quota",
			true,
			fmt.Sprintf(
				"namespace %s does not exist yet; no ResourceQuota is currently applied",
				namespace,
			),
		)

		return
	}

	if err != nil {
		plan.AddCheck(
			name+"-resource-quota",
			false,
			fmt.Sprintf("list ResourceQuotas in %s: %v", namespace, err),
		)

		return
	}

	if quotas == nil {
		plan.AddCheck(
			name+"-resource-quota",
			false,
			fmt.Sprintf("list ResourceQuotas in %s returned an empty object", namespace),
		)

		return
	}

	var limitRangeItems []corev1.LimitRange
	if estimate.Pods > 0 {
		limitRanges, listErr := client.CoreV1().LimitRanges(namespace).List(
			ctx,
			metav1.ListOptions{},
		)
		if listErr != nil && !apierrors.IsNotFound(listErr) {
			plan.AddCheck(
				name+"-resource-quota",
				false,
				fmt.Sprintf("list LimitRanges in %s for quota evaluation: %v", namespace, listErr),
			)

			return
		}

		if limitRanges != nil {
			limitRangeItems = limitRanges.Items
		}
	}

	report, err := kube.EvaluateResourceQuotaCapacity(
		namespace,
		quotas.Items,
		limitRangeItems,
		estimate,
	)
	if err != nil {
		plan.AddCheck(name+"-resource-quota", false, err.Error())

		return
	}

	if len(report.Violations) > 0 {
		plan.AddCheck(
			name+"-resource-quota",
			false,
			strings.Join(report.Violations, "; "),
		)

		return
	}

	plan.AddCheck(
		name+"-resource-quota",
		true,
		fmt.Sprintf(
			"%d ResourceQuota object(s), %d bounded resource(s), all have capacity",
			len(quotas.Items),
			report.Checked,
		),
	)
}
