package kube

import (
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
)

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
