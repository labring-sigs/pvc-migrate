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

// Empty eligible domains count toward the skew the way kube-scheduler counts
// them: recreating next to the only peer on an occupied domain leaves the
// empty domain at zero, and maxSkew 1 is violated even though every domain
// with matching Pods holds Pods. The legacy pod-domains-only count passed
// this placement and the recreated Pod stayed Pending after cutover.
func TestRecreationSchedulingSpreadCountsEmptyEligibleDomains(t *testing.T) {
	sourcePod := podWithLabels("db-0", "node-b", map[string]string{"app": "db"})
	peer := podOnNode("db-1", "node-b", map[string]string{"app": "db"})
	sourcePod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "db"},
		},
	}}

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
		t.Fatalf("empty eligible domain must inflate the skew, got %v", issues)
	}

	if !containsStr(issues[0], "maxSkew 1") {
		t.Errorf("issue should explain the skew violation: %v", issues)
	}
}

// nodeTaintsPolicy defaults to Ignore, so a tainted empty domain inflates the
// skew; opting in with Honor removes the untolerated domain from the count.
func TestRecreationSchedulingSpreadNodeTaintsPolicy(t *testing.T) {
	honor := corev1.NodeInclusionPolicyHonor
	constraint := corev1.TopologySpreadConstraint{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "db"},
		},
	}

	newCase := func() (*corev1.Pod, *corev1.Pod) {
		sourcePod := podWithLabels("db-0", "node-a", map[string]string{"app": "db"})
		peer := podOnNode("db-1", "node-a", map[string]string{"app": "db"})
		sourcePod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{constraint}

		return sourcePod, peer
	}

	tainted := nodeWithLabels(
		"node-tainted", map[string]string{"kubernetes.io/hostname": "node-tainted"},
	)
	tainted.Spec.Taints = []corev1.Taint{{
		Key: "node-role.kubernetes.io/control-plane", Effect: corev1.TaintEffectNoSchedule,
	}}

	for _, testCase := range []struct {
		name       string
		honorTaint bool
		wantIssue  bool
	}{
		{name: "default ignores taints", honorTaint: false, wantIssue: true},
		{name: "honor excludes untolerated domain", honorTaint: true, wantIssue: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sourcePod, peer := newCase()

			spec := *sourcePod.Spec.DeepCopy()
			if testCase.honorTaint {
				honored := constraint
				honored.NodeTaintsPolicy = &honor
				spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{honored}
			}

			client := newSchedulingFake(t,
				sourcePod, peer,
				nodeWithLabels("node-a", map[string]string{"kubernetes.io/hostname": "node-a"}),
				tainted,
			)

			spec.NodeName = "node-a"

			issues := recreationSchedulingIssues(
				context.Background(), client, sourcePod, "node-a", spec,
			)
			if testCase.wantIssue && len(issues) != 1 {
				t.Fatalf("expected a skew violation, got %v", issues)
			}

			if !testCase.wantIssue && len(issues) != 0 {
				t.Fatalf("honored taint policy must drop the empty domain, got %v", issues)
			}
		})
	}
}

// nodeAffinityPolicy defaults to Honor: nodes the recreated Pod cannot use are
// not eligible domains, so they do not inflate the skew.
func TestRecreationSchedulingSpreadHonorsNodeAffinity(t *testing.T) {
	sourcePod := podWithLabels("db-0", "node-a", map[string]string{"app": "db"})
	peer := podOnNode("db-1", "node-a", map[string]string{"app": "db"})
	peer2 := podOnNode("db-2", "node-a", map[string]string{"app": "db"})
	sourcePod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "db"},
		},
	}}

	client := newSchedulingFake(t,
		sourcePod, peer, peer2,
		nodeWithLabels("node-a", map[string]string{"kubernetes.io/hostname": "node-a"}),
		nodeWithLabels("node-b", map[string]string{"kubernetes.io/hostname": "node-b"}),
	)

	recreated := *sourcePod.Spec.DeepCopy()
	recreated.NodeName = "node-a"
	recreated.NodeSelector = map[string]string{"kubernetes.io/hostname": "node-a"}

	// node-b is not an eligible domain under the selector, so the only domain
	// holds every Pod and the skew stays zero.
	if issues := recreationSchedulingIssues(
		context.Background(), client, sourcePod, "node-a", recreated,
	); len(issues) != 0 {
		t.Fatalf("node-affinity-bounded spread must stay satisfiable, got %v", issues)
	}
}

// minDomains floors the global minimum at zero while eligible domains stay
// below the threshold, which turns an otherwise-balanced placement into a
// violation.
func TestRecreationSchedulingSpreadMinDomains(t *testing.T) {
	minDomains := int32(2)
	constraint := corev1.TopologySpreadConstraint{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "db"},
		},
	}

	for _, testCase := range []struct {
		name      string
		setField  bool
		wantIssue bool
	}{
		{name: "below minDomains floors minimum at zero", setField: true, wantIssue: true},
		{name: "default minDomains stays balanced", setField: false, wantIssue: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sourcePod := podWithLabels("db-0", "node-a", map[string]string{"app": "db"})
			peer := podOnNode("db-1", "node-a", map[string]string{"app": "db"})

			constraint := constraint
			if testCase.setField {
				constraint.MinDomains = &minDomains
			}

			sourcePod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{constraint}

			client := newSchedulingFake(t,
				sourcePod, peer,
				nodeWithLabels("node-a", map[string]string{"kubernetes.io/hostname": "node-a"}),
			)

			recreated := *sourcePod.Spec.DeepCopy()
			recreated.NodeName = "node-a"

			issues := recreationSchedulingIssues(
				context.Background(), client, sourcePod, "node-a", recreated,
			)
			if testCase.wantIssue && len(issues) != 1 {
				t.Fatalf("expected a minDomains violation, got %v", issues)
			}

			if !testCase.wantIssue && len(issues) != 0 {
				t.Fatalf("default minDomains must stay satisfiable, got %v", issues)
			}
		})
	}
}

func containsStr(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
