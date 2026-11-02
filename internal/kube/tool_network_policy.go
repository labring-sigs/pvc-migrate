package kube

import (
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// Tool network policy values select whether transfer tool pods receive the
// upstream chart's allow-all NetworkPolicy. Auto keeps the engine default —
// create the policy unless the executing account cannot, so transfers keep
// working in network-isolated namespaces wherever possible; on and off force
// the choice, and an explicit value also overrides the permission fallback.
const (
	ToolNetworkPolicyAuto = "auto"
	ToolNetworkPolicyOn   = "on"
	ToolNetworkPolicyOff  = "off"
)

// NormalizeToolNetworkPolicy validates a configured tool network policy. The
// empty value means auto: callers that never choose keep the engine default.
func NormalizeToolNetworkPolicy(value string) (string, error) {
	switch strings.TrimSpace(value) {
	case "", ToolNetworkPolicyAuto:
		return ToolNetworkPolicyAuto, nil
	case ToolNetworkPolicyOn, ToolNetworkPolicyOff:
		return strings.TrimSpace(value), nil
	default:
		return "", domain.NewError(
			domain.ErrorValidation,
			"tool network policy",
			"unsupported value "+value+"; use auto, on, or off",
		)
	}
}

// ResolveToolNetworkPolicy applies workflow-over-controller precedence: a
// value the workflow spec sets wins over the controller-wide default, and an
// empty spec value keeps whatever the controller configured.
func ResolveToolNetworkPolicy(spec, controller string) string {
	if spec != "" {
		return spec
	}

	return controller
}

// ToolNetworkPolicyHelmValues expands a forced choice into per-component
// chart values. Auto returns nil so the engine leaves the upstream default
// and its permission-based fallback in charge.
func ToolNetworkPolicyHelmValues(value string) []string {
	mode, err := NormalizeToolNetworkPolicy(value)
	if err != nil || mode == ToolNetworkPolicyAuto {
		return nil
	}

	enabled := mode == ToolNetworkPolicyOn

	values := make([]string, 0, 3)
	for _, component := range []string{"rsync", "sshd", "rclone"} {
		values = append(values, component+".networkPolicy.enabled="+boolString(enabled))
	}

	return values
}

func boolString(value bool) string {
	if value {
		return "true"
	}

	return "false"
}
