package planner

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func requiredAffinity(topologyKey string, selector map[string]string) *corev1.Affinity {
	return &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey:   topologyKey,
				LabelSelector: &metav1.LabelSelector{MatchLabels: selector},
			}},
		},
	}
}

// 伴生 Pod（如 sidecar/proxy）在源域存活：重建到其它域会违反 required podAffinity。
func TestRecreationSchedulingRejectsUnsatisfiablePodAffinity(t *testing.T) {
	sourcePod := podOnNode("app-0", "node-a", map[string]string{"app": "web"})
	peer := podOnNode("proxy", "node-a", map[string]string{"tier": "edge"})
	sourcePod.Spec.Affinity = requiredAffinity(
		"kubernetes.io/hostname",
		map[string]string{"tier": "edge"},
	)

	client := newSchedulingFake(t,
		sourcePod, peer,
		nodeWithLabels("node-a", map[string]string{"kubernetes.io/hostname": "node-a"}),
		nodeWithLabels("node-b", map[string]string{"kubernetes.io/hostname": "node-b"}),
	)

	recreated := *sourcePod.Spec.DeepCopy()
	recreated.NodeName = "node-b"

	issues := recreationSchedulingIssues(
		context.Background(), client, sourcePod, "node-b", recreated,
	)
	if len(issues) != 1 {
		t.Fatalf("issues=%v", issues)
	}

	if !strings.Contains(issues[0], "podAffinity cannot be satisfied") {
		t.Errorf("issue should explain podAffinity violation: %v", issues)
	}
}

// 伴生 Pod 同时落在目标域：重建后仍同域 → 满足。
func TestRecreationSchedulingAllowsSatisfiedPodAffinity(t *testing.T) {
	sourcePod := podOnNode("app-0", "node-a", map[string]string{"app": "web"})
	peer := podOnNode("proxy", "node-b", map[string]string{"tier": "edge"})
	sourcePod.Spec.Affinity = requiredAffinity(
		"kubernetes.io/hostname",
		map[string]string{"tier": "edge"},
	)

	client := newSchedulingFake(t,
		sourcePod, peer,
		nodeWithLabels("node-a", map[string]string{"kubernetes.io/hostname": "node-a"}),
		nodeWithLabels("node-b", map[string]string{"kubernetes.io/hostname": "node-b"}),
	)

	recreated := *sourcePod.Spec.DeepCopy()
	recreated.NodeName = "node-b"

	if issues := recreationSchedulingIssues(
		context.Background(), client, sourcePod, "node-b", recreated,
	); len(issues) != 0 {
		t.Fatalf("satisfied podAffinity must not fail, got %v", issues)
	}
}

// 无匹配存活 Pod：约束在重建时自然无法满足但也不是迁移引入的问题——不阻断（调度器会按软失败处理这种情况下的
// 必需亲和：没有匹配 pod 就没有域约束）。这与 kube-scheduler 行为一致：podAffinity 无匹配 pod 时不强制。
func TestRecreationSchedulingSkipsPodAffinityWithoutMatches(t *testing.T) {
	sourcePod := podOnNode("app-0", "node-a", map[string]string{"app": "web"})
	sourcePod.Spec.Affinity = requiredAffinity(
		"kubernetes.io/hostname",
		map[string]string{"tier": "nonexistent"},
	)

	client := newSchedulingFake(t,
		sourcePod,
		nodeWithLabels("node-a", map[string]string{"kubernetes.io/hostname": "node-a"}),
		nodeWithLabels("node-b", map[string]string{"kubernetes.io/hostname": "node-b"}),
	)

	recreated := *sourcePod.Spec.DeepCopy()
	recreated.NodeName = "node-b"

	if issues := recreationSchedulingIssues(
		context.Background(), client, sourcePod, "node-b", recreated,
	); len(issues) != 0 {
		t.Fatalf("no matching live pods should skip affinity check, got %v", issues)
	}
}
