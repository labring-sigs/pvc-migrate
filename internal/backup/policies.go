package backup

import (
	"context"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func checkObjectTransferQuota(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	operation string,
) error {
	toolResources := objectTransferToolResourceEstimate()
	if operation != "backup" || req.SessionStore == nil {
		return checkNamespaceAdmissionPolicies(
			ctx,
			client,
			req.Namespace,
			operation,
			toolResources,
		)
	}

	if strings.TrimSpace(req.SessionNamespace) == "" {
		return domain.NewError(
			domain.ErrorValidation,
			backupPreflightPhase,
			"session namespace is required when backup sessions are enabled",
		)
	}

	sessionResources, err := backupSessionResourceEstimate(ctx, client, req)
	if err != nil {
		return err
	}

	if req.SessionNamespace == req.Namespace {
		toolResources.Secrets += sessionResources.Secrets
		toolResources.ConfigMaps += sessionResources.ConfigMaps
		toolResources.Leases += sessionResources.Leases

		return checkNamespaceAdmissionPolicies(
			ctx,
			client,
			req.Namespace,
			operation,
			toolResources,
		)
	}

	checks := [2]struct {
		namespace string
		estimate  domain.ResourceEstimate
		err       error
	}{
		{namespace: req.Namespace, estimate: toolResources},
		{namespace: req.SessionNamespace, estimate: sessionResources},
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

func backupSessionResourceEstimate(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
) (domain.ResourceEstimate, error) {
	estimate := domain.ResourceEstimate{Secrets: 1}
	if _, ok := req.SessionStore.(*kube.ConfigMapSessionStore); ok {
		estimate.ConfigMaps = 1
	}

	if _, ok := req.SessionStore.(kube.SessionLocker); ok {
		estimate.Leases = 2
	}

	if req.BackupSession == nil {
		return estimate, nil
	}

	sessionID := req.BackupSession.ID
	if estimate.ConfigMaps > 0 {
		exists, err := backupResourceExists(func() error {
			_, getErr := client.CoreV1().ConfigMaps(req.SessionNamespace).Get(
				ctx,
				kube.SessionConfigMapName(sessionID),
				metav1.GetOptions{},
			)

			return getErr
		})
		if err != nil {
			return domain.ResourceEstimate{}, err
		}

		if exists {
			estimate.ConfigMaps--
		}
	}

	exists, err := backupResourceExists(func() error {
		_, getErr := client.CoreV1().Secrets(req.SessionNamespace).Get(
			ctx,
			kube.BackupCredentialsSecretName(sessionID),
			metav1.GetOptions{},
		)

		return getErr
	})
	if err != nil {
		return domain.ResourceEstimate{}, err
	}

	if exists {
		estimate.Secrets--
	}

	if estimate.Leases > 0 {
		leaseIDs := []string{sessionID, backupTargetLockID(req.Store)}
		leaseResults := make([]bool, len(leaseIDs))
		leaseErrors := make([]error, len(leaseIDs))
		parallel.For(len(leaseIDs), func(index int) {
			leaseID := leaseIDs[index]
			exists, err := backupResourceExists(func() error {
				_, getErr := client.CoordinationV1().Leases(req.SessionNamespace).Get(
					ctx,
					kube.SessionLockName(leaseID),
					metav1.GetOptions{},
				)

				return getErr
			})
			leaseResults[index] = exists
			leaseErrors[index] = err
		})

		for index, exists := range leaseResults {
			if leaseErrors[index] != nil {
				return domain.ResourceEstimate{}, leaseErrors[index]
			}

			if exists {
				estimate.Leases--
			}
		}
	}

	return estimate, nil
}

func backupResourceExists(get func() error) (bool, error) {
	err := get()
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			backupPreflightPhase,
			"read session resource for quota evaluation",
			err,
		)
	}

	return true, nil
}
