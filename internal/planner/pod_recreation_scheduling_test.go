package planner

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func newSchedulingFake(t *testing.T, objects ...runtime.Object) *fake.Clientset {
	t.Helper()
	return fake.NewSimpleClientset(objects...)
}

func podWithLabels(name, node string, podLabels map[string]string) *corev1.Pod {
	return podOnNode(name, node, podLabels)
}

func podOnNode(name, node string, podLabels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Labels: podLabels},
		Spec:       corev1.PodSpec{NodeName: node},
	}
}

func nodeWithLabels(name string, nodeLabels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: nodeLabels},
	}
}

func requiredAntiAffinity(topologyKey string, selector map[string]string) *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey:   topologyKey,
				LabelSelector: &metav1.LabelSelector{MatchLabels: selector},
			}},
		},
	}
}

// Single-replica database: the paused source Pod is the only one matching the
// anti-affinity selector, so recreating it on another node is schedulable and
// must NOT be rejected — this is the false positive the legacy check produced.
func TestRecreationSchedulingAllowsSingleReplicaAntiAffinity(t *testing.T) {
	sourcePod := podWithLabels("db-0", "node-a", map[string]string{
		"app.kubernetes.io/instance": "db",
	})
	sourcePod.Spec.Affinity = requiredAntiAffinity(
		"kubernetes.io/hostname",
		map[string]string{"app.kubernetes.io/instance": "db"},
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
		t.Fatalf("single-replica recreation must be schedulable, got %v", issues)
	}
}

// A genuine second replica on the target topology domain must be flagged.
func TestRecreationSchedulingRejectsRealAntiAffinityConflict(t *testing.T) {
	sourcePod := podWithLabels("db-0", "node-a", map[string]string{
		"app.kubernetes.io/instance": "db",
	})
	peer := podWithLabels("db-1", "node-b", map[string]string{
		"app.kubernetes.io/instance": "db",
	})
	sourcePod.Spec.Affinity = requiredAntiAffinity(
		"kubernetes.io/hostname",
		map[string]string{"app.kubernetes.io/instance": "db"},
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

	if !containsStr(issues[0], "db-1") {
		t.Errorf("conflict should name the blocking Pod: %v", issues)
	}
}

// The same anti-affinity conflict does NOT exist when the recreated Pod lands
// on a topology domain without a matching live Pod.
func TestRecreationSchedulingAllowsOtherTopologyDomain(t *testing.T) {
	sourcePod := podWithLabels("db-0", "node-a", map[string]string{
		"app.kubernetes.io/instance": "db",
	})
	peer := podWithLabels("db-1", "node-a", map[string]string{
		"app.kubernetes.io/instance": "db",
	})
	sourcePod.Spec.Affinity = requiredAntiAffinity(
		"kubernetes.io/hostname",
		map[string]string{"app.kubernetes.io/instance": "db"},
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
		t.Fatalf("other-domain recreation must be schedulable, got %v", issues)
	}
}

// topologySpread DoNotSchedule: placing the recreated Pod on the empty target
// domain keeps maxSkew satisfied, so the legacy blanket rejection was wrong.
func TestRecreationSchedulingAllowsSatisfiableSpread(t *testing.T) {
	sourcePod := podWithLabels("db-0", "node-a", map[string]string{"app": "db"})
	sourcePod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "db"},
		},
	}}

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
		t.Fatalf("satisfiable spread must not fail, got %v", issues)
	}
}

// Two live replicas on node-a plus the recreated pod on node-b: domains 2/1,
// maxSkew 1 holds — still schedulable.
func TestRecreationSchedulingAllowsSpreadWithinMaxSkew(t *testing.T) {
	sourcePod := podWithLabels("db-0", "node-a", map[string]string{"app": "db"})
	liveA := podWithLabels("db-1", "node-a", map[string]string{"app": "db"})
	liveA2 := podWithLabels("db-2", "node-a", map[string]string{"app": "db"})
	sourcePod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "db"},
		},
	}}

	client := newSchedulingFake(t,
		sourcePod, liveA, liveA2,
		nodeWithLabels("node-a", map[string]string{"kubernetes.io/hostname": "node-a"}),
		nodeWithLabels("node-b", map[string]string{"kubernetes.io/hostname": "node-b"}),
	)

	recreated := *sourcePod.Spec.DeepCopy()
	recreated.NodeName = "node-b"

	// domains after recreation: node-a=2, node-b=1 → skew 1 ≤ maxSkew 1.
	if issues := recreationSchedulingIssues(
		context.Background(), client, sourcePod, "node-b", recreated,
	); len(issues) != 0 {
		t.Fatalf("within-maxSkew spread must not fail, got %v", issues)
	}
}

// Three live replicas on node-a: placing the recreated pod on node-b gives
// domains 3/1 → skew 2 > maxSkew 1 → must fail.
func TestRecreationSchedulingRejectsViolatedSpread(t *testing.T) {
	sourcePod := podWithLabels("db-0", "node-a", map[string]string{"app": "db"})
	liveA1 := podOnNode("db-1", "node-a", map[string]string{"app": "db"})
	liveA2 := podOnNode("db-2", "node-a", map[string]string{"app": "db"})
	liveA3 := podOnNode("db-3", "node-a", map[string]string{"app": "db"})
	sourcePod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "db"},
		},
	}}

	client := newSchedulingFake(t,
		sourcePod, liveA1, liveA2, liveA3,
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

	if !containsStr(issues[0], "maxSkew 1") {
		t.Errorf("issue should explain the skew violation: %v", issues)
	}
}

func containsStr(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
