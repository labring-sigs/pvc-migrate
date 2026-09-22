package backup

import (
	"context"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

func checkBackupQuota(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	sessionNamespace string,
	sessionResources domain.ResourceEstimate,
) error {
	operation := "backup"

	toolResources := objectTransferToolResourceEstimate()
	if sessionNamespace == "" {
		return checkNamespaceAdmissionPolicies(
			ctx,
			client,
			namespace,
			operation,
			toolResources,
		)
	}

	if sessionNamespace == namespace {
		toolResources.Secrets += sessionResources.Secrets
		toolResources.ConfigMaps += sessionResources.ConfigMaps
		toolResources.Leases += sessionResources.Leases

		return checkNamespaceAdmissionPolicies(
			ctx,
			client,
			namespace,
			operation,
			toolResources,
		)
	}

	checks := [2]struct {
		namespace string
		estimate  domain.ResourceEstimate
		err       error
	}{
		{namespace: namespace, estimate: toolResources},
		{namespace: sessionNamespace, estimate: sessionResources},
	}

	parallel.For(2, func(index int) {
		checks[index].err = checkNamespaceAdmissionPolicies(
			ctx,
			client,
			checks[index].namespace,
			operation,
			checks[index].estimate,
		)
	})

	// Return the source-namespace error first for deterministic diagnostics when
	// both independent policy reads fail in the same preflight.
	if checks[0].err != nil {
		return checks[0].err
	}

	return checks[1].err
}

func checkNamespaceAdmissionPolicies(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	operation string,
	estimate domain.ResourceEstimate,
) error {
	phase := operation + " preflight"

	policies := kube.ReadNamespaceResourcePolicies(ctx, client, namespace, estimate.Pods > 0)

	if policies.ResourceQuotaErr != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			phase,
			"list ResourceQuotas in "+namespace,
			policies.ResourceQuotaErr,
		)
	}

	if policies.ResourceQuotas == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			phase,
			"list ResourceQuotas in "+namespace+" returned an empty object",
		)
	}

	if policies.LimitRangeErr != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			phase,
			"list LimitRanges in "+namespace,
			policies.LimitRangeErr,
		)
	}

	if estimate.Pods > 0 && policies.LimitRanges == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			phase,
			"list LimitRanges in "+namespace+" returned an empty object",
		)
	}

	if estimate.Pods > 0 {
		violations := kube.ToolLimitRangeViolations(policies.LimitRanges.Items)
		if len(violations) > 0 {
			return domain.NewError(
				domain.ErrorPrecondition,
				phase,
				"tool resources violate namespace LimitRange: "+strings.Join(violations, "; "),
			)
		}
	}

	var limitRangeItems []corev1.LimitRange
	if policies.LimitRanges != nil {
		limitRangeItems = policies.LimitRanges.Items
	}

	report, err := kube.EvaluateResourceQuotaCapacity(
		namespace,
		policies.ResourceQuotas.Items,
		limitRangeItems,
		estimate,
	)
	if err != nil {
		return err
	}

	if len(report.Violations) > 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			phase,
			"workflow resources exceed namespace quota: "+strings.Join(report.Violations, "; "),
		)
	}

	return nil
}

func objectTransferToolResourceEstimate() domain.ResourceEstimate {
	estimate := domain.ResourceEstimate{
		Pods:               1,
		TerminatingPods:    1,
		NotTerminatingPods: 1,
		Jobs:               1,
		Secrets:            1,
		ServiceAccounts:    1,
	}
	kube.AddHelmReleaseObjectEstimate(&estimate, 1)

	return estimate
}
