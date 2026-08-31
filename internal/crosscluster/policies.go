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
	"k8s.io/client-go/kubernetes"
)

func (s *Service) planCrossClusterPolicies(
	ctx context.Context,
	plan *Plan,
	options Options,
	destinationClass *storagev1.StorageClass,
) {
	const pvcPolicyCheckName = domain.CheckNameDestinationPVCPolicy

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
		addCrossClusterPolicyChecks(
			ctx,
			plan,
			s.source.Kubernetes,
			options.SessionNamespace,
			"source-session",
			sessionResources,
			false,
		)
	}

	addCrossClusterPolicyChecks(
		ctx,
		plan,
		s.source.Kubernetes,
		options.SourceNamespace,
		"source-tool",
		sourceResources,
		true,
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

	addCrossClusterPolicyChecks(
		ctx,
		plan,
		s.destination.Kubernetes,
		options.DestinationNamespace,
		"destination-tool",
		destinationResources,
		true,
	)

	changes := make([]kube.PVCAdmissionChange, 0, len(plan.Volumes))
	for _, volume := range plan.Volumes {
		capacity, err := resource.ParseQuantity(volume.Capacity)
		if err != nil {
			plan.AddCheck(
				pvcPolicyCheckName,
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
			pvcPolicyCheckName,
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
			pvcPolicyCheckName,
			false,
			strings.Join(violations, "; "),
		)

		return
	}

	plan.AddCheck(
		pvcPolicyCheckName,
		true,
		fmt.Sprintf(
			"destination namespace policies permit %d PVC request(s)",
			len(changes),
		),
	)
}

func addCrossClusterPolicyChecks(
	ctx context.Context,
	plan *Plan,
	client kubernetes.Interface,
	namespace string,
	name string,
	estimate domain.ResourceEstimate,
	checkToolLimitRanges bool,
) {
	quotaCheckName := domain.CheckName(name + "-resource-quota")

	policies := kube.ReadNamespaceResourcePolicies(
		ctx,
		client,
		namespace,
		checkToolLimitRanges || estimate.Pods > 0,
	)
	if checkToolLimitRanges {
		addCrossClusterLimitRangeCheck(plan, namespace, name, policies)
	}

	if apierrors.IsNotFound(policies.ResourceQuotaErr) {
		plan.AddCheck(
			quotaCheckName,
			true,
			fmt.Sprintf(
				"namespace %s does not exist yet; no ResourceQuota is currently applied",
				namespace,
			),
		)

		return
	}

	if policies.ResourceQuotaErr != nil {
		plan.AddCheck(
			quotaCheckName,
			false,
			fmt.Sprintf(
				"list ResourceQuotas in %s: %v",
				namespace,
				policies.ResourceQuotaErr,
			),
		)

		return
	}

	if policies.ResourceQuotas == nil {
		plan.AddCheck(
			quotaCheckName,
			false,
			fmt.Sprintf("list ResourceQuotas in %s returned an empty object", namespace),
		)

		return
	}

	var limitRangeItems []corev1.LimitRange
	if estimate.Pods > 0 {
		if policies.LimitRangeErr != nil && !apierrors.IsNotFound(policies.LimitRangeErr) {
			plan.AddCheck(
				quotaCheckName,
				false,
				fmt.Sprintf(
					"list LimitRanges in %s for quota evaluation: %v",
					namespace,
					policies.LimitRangeErr,
				),
			)

			return
		}

		if policies.LimitRanges != nil {
			limitRangeItems = policies.LimitRanges.Items
		}
	}

	report, err := kube.EvaluateResourceQuotaCapacity(
		namespace,
		policies.ResourceQuotas.Items,
		limitRangeItems,
		estimate,
	)
	if err != nil {
		plan.AddCheck(quotaCheckName, false, err.Error())

		return
	}

	if len(report.Violations) > 0 {
		plan.AddCheck(
			quotaCheckName,
			false,
			strings.Join(report.Violations, "; "),
		)

		return
	}

	plan.AddCheck(
		quotaCheckName,
		true,
		fmt.Sprintf(
			"%d ResourceQuota object(s), %d bounded resource(s), all have capacity",
			len(policies.ResourceQuotas.Items),
			report.Checked,
		),
	)
}

func addCrossClusterLimitRangeCheck(
	plan *Plan,
	namespace string,
	name string,
	policies kube.NamespaceResourcePolicies,
) {
	checkName := domain.CheckName(name + "-limit-range")
	switch {
	case apierrors.IsNotFound(policies.LimitRangeErr):
		plan.AddCheck(
			checkName,
			true,
			fmt.Sprintf(
				"namespace %s does not exist yet; no LimitRange is currently applied",
				namespace,
			),
		)
	case policies.LimitRangeErr != nil:
		plan.AddCheck(
			checkName,
			false,
			fmt.Sprintf("list LimitRanges in %s: %v", namespace, policies.LimitRangeErr),
		)
	case policies.LimitRanges == nil:
		plan.AddCheck(
			checkName,
			false,
			fmt.Sprintf("list LimitRanges in %s returned an empty object", namespace),
		)
	default:
		violations := kube.ToolLimitRangeViolations(policies.LimitRanges.Items)
		if len(violations) > 0 {
			plan.AddCheck(checkName, false, strings.Join(violations, "; "))
			return
		}

		plan.AddCheck(
			checkName,
			true,
			fmt.Sprintf(
				"%d LimitRange object(s) permit tool resources",
				len(policies.LimitRanges.Items),
			),
		)
	}
}
