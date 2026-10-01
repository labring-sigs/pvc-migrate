package planner

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Auto-selection must drop candidate nodes the recreated Pod's own placement
// constraints reject, instead of proposing a target the finished plan fails.
// The source Pod runs on node-a, so the unconstrained ordering prefers the
// distinct node-b; every test below pins the only valid domain to node-a.

func requiredPodAffinityFor(selector map[string]string) *corev1.Affinity {
	return &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey:   corev1.LabelHostname,
				LabelSelector: &metav1.LabelSelector{MatchLabels: selector},
			}},
		},
	}
}

func requiredPodAntiAffinityFor(selector map[string]string) *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey:   corev1.LabelHostname,
				LabelSelector: &metav1.LabelSelector{MatchLabels: selector},
			}},
		},
	}
}

func addPeerPod(t *testing.T, p *Planner, name, node string, podLabels map[string]string) {
	t.Helper()

	if _, err := p.client.CoreV1().Pods("app").Create(
		t.Context(),
		podWithLabels(name, node, podLabels),
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}
}

func updateWriter(t *testing.T, p *Planner, mutate func(pod *corev1.Pod)) {
	t.Helper()

	pod, err := p.client.CoreV1().Pods("app").Get(t.Context(), "writer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	mutate(pod)

	if _, err := p.client.CoreV1().Pods("app").Update(
		t.Context(), pod, metav1.UpdateOptions{},
	); err != nil {
		t.Fatal(err)
	}
}

func autoSelectedTargetNode(t *testing.T, p *Planner, object *v1alpha1.PodMigration) string {
	t.Helper()

	report, err := p.PlanNamespacedPodMigration(t.Context(), object, "")
	if err != nil {
		t.Fatal(err)
	}

	if !report.Ready || object.Status.Plan == nil {
		t.Fatalf("plan not ready: checks=%+v", report.Checks)
	}

	return object.Status.Plan.TargetNode
}

func planAutoPodMigration(t *testing.T) (*Planner, *v1alpha1.PodMigration) {
	t.Helper()

	p, object := podMigrationPlanFixture(t)
	object.Spec.TargetNode = "auto"
	// The writer Pod already runs on node-a with the destination class, so the
	// positive selections below exercise an intentional reprovision.
	object.Spec.ForceReprovision = true

	return p, object
}

// required podAffinity: the co-located peer runs on node-a, so auto-selection
// must stay there even though the unconstrained ordering prefers node-b.
func TestAutoSelectionHonorsRequiredPodAffinity(t *testing.T) {
	p, object := planAutoPodMigration(t)
	addPeerPod(t, p, "mysql", "node-a", map[string]string{"app": "mysql"})
	updateWriter(t, p, func(pod *corev1.Pod) {
		pod.Spec.Affinity = requiredPodAffinityFor(map[string]string{"app": "mysql"})
	})

	if node := autoSelectedTargetNode(t, p, object); node != "node-a" {
		t.Fatalf("auto selection must co-locate with the affinity peer on node-a, got %q", node)
	}
}

// required podAntiAffinity: the conflicting peer occupies node-b, so
// auto-selection must fall back to the source node instead.
func TestAutoSelectionAvoidsAntiAffinityConflict(t *testing.T) {
	p, object := planAutoPodMigration(t)
	addPeerPod(t, p, "blocker", "node-b", map[string]string{"app": "blocker"})
	updateWriter(t, p, func(pod *corev1.Pod) {
		pod.Spec.Affinity = requiredPodAntiAffinityFor(map[string]string{"app": "blocker"})
	})

	if node := autoSelectedTargetNode(t, p, object); node != "node-a" {
		t.Fatalf("auto selection must avoid the anti-affinity conflict on node-b, got %q", node)
	}
}

// DoNotSchedule topologySpread: the only matching peer sits on node-b, so
// recreating there would hold 2 Pods against node-a's 0 while maxSkew is 1;
// node-a keeps the domains at 1/1.
func TestAutoSelectionSatisfiesTopologySpread(t *testing.T) {
	p, object := planAutoPodMigration(t)
	updateWriter(t, p, func(pod *corev1.Pod) {
		pod.Labels = map[string]string{"app": "writer"}
		pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew:           1,
			TopologyKey:       corev1.LabelHostname,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "writer"},
			},
		}}
	})
	addPeerPod(t, p, "writer-peer", "node-b", map[string]string{"app": "writer"})

	if node := autoSelectedTargetNode(t, p, object); node != "node-a" {
		t.Fatalf("auto selection must keep the spread balanced on node-a, got %q", node)
	}
}

// When every otherwise-compatible node violates a placement constraint, the
// failure must say so instead of reporting a bare topology miss.
func TestAutoSelectionReportsPlacementExhaustion(t *testing.T) {
	p, object := planAutoPodMigration(t)
	addPeerPod(t, p, "blocker-a", "node-a", map[string]string{"app": "blocker"})
	addPeerPod(t, p, "blocker-b", "node-b", map[string]string{"app": "blocker"})
	updateWriter(t, p, func(pod *corev1.Pod) {
		pod.Spec.Affinity = requiredPodAntiAffinityFor(map[string]string{"app": "blocker"})
	})

	report, err := p.PlanNamespacedPodMigration(t.Context(), object, "")
	if err != nil {
		t.Fatal(err)
	}

	exhausted := hasFailedCheckContaining(
		report.Checks, domain.CheckNameTargetNode, "placement constraints",
	)

	if report.Ready || object.Status.Plan != nil || !exhausted {
		t.Fatalf("exhausted placement must fail explicitly: checks=%+v", report.Checks)
	}
}
