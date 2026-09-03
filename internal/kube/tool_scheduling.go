package kube

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	// TransferServiceAccountName is shared by short-lived transfer workloads
	// that do not need Kubernetes API credentials. It is deliberately stable
	// so the account can be provisioned before Helm renders a Pod and reused
	// without leaking one account per operation. Rclone uses a caller-supplied
	// account instead when cloud Workload Identity is configured.
	TransferServiceAccountName  = "pvc-migrate-transfer"
	transferServiceAccountLabel = MetadataDomain + "/transfer-service-account"
	transferServiceAccountValue = "no-token"
)

// HelmOverrides keeps typed Helm values separate from values that must remain
// strings. In particular, serviceAccount.create must be a boolean: passing
// "false" through --set-string makes Helm templates treat it as truthy.
type HelmOverrides struct {
	Values       []string
	StringValues []string
}

// ToolComponentNodeHelmValues pins one upstream chart component to a node and
// mirrors the taint tolerations used by node-specific probe Pods.
func ToolComponentNodeHelmValues(component string, node *corev1.Node) ([]string, error) {
	if !validToolProbeComponent(component) || component == ToolComponentShell {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"tool scheduling",
			fmt.Sprintf("unsupported tool component %q", component),
		)
	}

	if node == nil || node.Name == "" {
		return nil, domain.NewError(domain.ErrorKubernetes, "tool scheduling", "node is empty")
	}

	tolerations := ToolComponentTolerationHelmValues(component, node)
	values := make([]string, 1, 1+len(tolerations))
	values[0] = component + ".nodeName=" + node.Name

	return append(values, tolerations...), nil
}

// ToolServiceAccountHelmValues makes rclone use an administrator-provisioned
// or project-managed identity. The name is required so callers cannot
// accidentally fall back to the chart's token-bearing per-release account.
func ToolServiceAccountHelmValues(name string) (HelmOverrides, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return HelmOverrides{}, domain.NewError(
			domain.ErrorValidation,
			"tool identity",
			"service account name is required",
		)
	}

	if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return HelmOverrides{}, domain.NewError(
			domain.ErrorValidation,
			"tool identity",
			fmt.Sprintf(
				"service account name %q is invalid: %s",
				name,
				strings.Join(problems, "; "),
			),
		)
	}

	return HelmOverrides{
		Values:       []string{"rclone.serviceAccount.create=false"},
		StringValues: []string{"rclone.serviceAccount.name=" + name},
	}, nil
}

// TransferServiceAccountHelmValues makes the upstream transfer chart use the
// project-managed no-token identity for components that only need PVC and
// network access. The rclone component intentionally has a separate identity
// contract because cloud Workload Identity may be required for object storage.
func TransferServiceAccountHelmValues() HelmOverrides {
	return HelmOverrides{
		Values: []string{
			"sshd.serviceAccount.create=false",
			"rsync.serviceAccount.create=false",
		},
		StringValues: []string{
			"sshd.serviceAccount.name=" + TransferServiceAccountName,
			"rsync.serviceAccount.name=" + TransferServiceAccountName,
		},
	}
}

// EnsureTransferServiceAccount creates or verifies the namespace-local
// ServiceAccount used by copy transfer Pods. A Pod with this account inherits
// automount=false even though the upstream chart does not expose that PodSpec
// field. Existing accounts are only accepted when they carry this project's
// ownership marker, which prevents accidentally adopting a tenant identity.
func EnsureTransferServiceAccount(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
) error {
	if client == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"transfer identity",
			"Kubernetes client is required",
		)
	}

	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"transfer identity",
			"namespace is required",
		)
	}

	if problems := validation.IsDNS1123Label(namespace); len(problems) > 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"transfer identity",
			fmt.Sprintf(
				"namespace %q is invalid: %s",
				namespace,
				strings.Join(problems, "; "),
			),
		)
	}

	accounts := client.CoreV1().ServiceAccounts(namespace)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		account, getErr := accounts.Get(ctx, TransferServiceAccountName, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			automount := false

			account, getErr = accounts.Create(ctx, &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      TransferServiceAccountName,
					Namespace: namespace,
					Labels: map[string]string{
						ManagedByLabel:              ManagedByValue,
						transferServiceAccountLabel: transferServiceAccountValue,
					},
				},
				AutomountServiceAccountToken: &automount,
			}, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(getErr) {
				account, getErr = accounts.Get(ctx, TransferServiceAccountName, metav1.GetOptions{})
			}
		}

		if getErr != nil {
			return getErr
		}

		if account == nil {
			return errors.New("kubernetes returned an empty ServiceAccount")
		}

		if account.Labels[ManagedByLabel] != ManagedByValue ||
			account.Labels[transferServiceAccountLabel] != transferServiceAccountValue {
			return domain.NewError(
				domain.ErrorConflict,
				"transfer identity",
				fmt.Sprintf(
					"ServiceAccount %s/%s is not managed by pvc-migrate",
					namespace,
					TransferServiceAccountName,
				),
			)
		}

		if account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken {
			fixed := account.DeepCopy()
			automount := false

			fixed.AutomountServiceAccountToken = &automount
			_, getErr = accounts.Update(ctx, fixed, metav1.UpdateOptions{})
		}

		return getErr
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"transfer identity",
			fmt.Sprintf("ensure ServiceAccount %s/%s", namespace, TransferServiceAccountName),
			err,
		)
	}

	return nil
}

// ToolComponentTolerationHelmValues merges hard-taint tolerations across every
// node where a component can run.
func ToolComponentTolerationHelmValues(component string, nodes ...*corev1.Node) []string {
	taintCount := 0
	for _, node := range nodes {
		if node != nil {
			taintCount += len(node.Spec.Taints)
		}
	}

	values := make([]string, 0, taintCount*4)
	seen := map[string]struct{}{}

	index := 0
	for _, node := range nodes {
		if node == nil {
			continue
		}

		for _, taint := range node.Spec.Taints {
			if taint.Effect != corev1.TaintEffectNoSchedule &&
				taint.Effect != corev1.TaintEffectNoExecute {
				continue
			}

			signature := taint.Key + "\x00" + taint.Value + "\x00" + string(taint.Effect)
			if _, exists := seen[signature]; exists {
				continue
			}

			seen[signature] = struct{}{}
			prefix := fmt.Sprintf("%s.tolerations[%d]", component, index)

			values = append(
				values,
				prefix+".key="+taint.Key,
				prefix+".effect="+string(taint.Effect),
			)
			if taint.Value == "" {
				values = append(values, prefix+".operator=Exists")
			} else {
				values = append(values, prefix+".operator=Equal", prefix+".value="+taint.Value)
			}

			index++
		}
	}

	return values
}
