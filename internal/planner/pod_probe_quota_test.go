package planner

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodPrecopyControlsConcurrentProbeQuota(t *testing.T) {
	for _, passes := range []int{0, 1} {
		t.Run(
			map[int]string{0: "final sync only", 1: "warm and final sync"}[passes],
			func(t *testing.T) {
				p, object := podMigrationPlanFixture(t)
				object.Spec.TemporaryNamespace = "app"
				object.Spec.Strategies = []string{domain.StrategyMount}
				object.Spec.ForceReprovision = true
				object.Spec.PrecopyPasses = passes

				pod, err := p.client.CoreV1().
					Pods("app").
					Get(t.Context(), "writer", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				pod.Spec.NodeName = "node-b"
				if _, err := p.client.CoreV1().
					Pods("app").
					Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}

				limit := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("1")}

				quota := &corev1.ResourceQuota{
					ObjectMeta: metav1.ObjectMeta{Name: "probes", Namespace: "app"},
					Spec: corev1.ResourceQuotaSpec{
						Hard:   limit,
						Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeTerminating},
					},
					Status: corev1.ResourceQuotaStatus{
						Hard: limit,
						Used: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("0")},
					},
				}
				if _, err := p.client.CoreV1().
					ResourceQuotas("app").
					Create(t.Context(), quota, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}

				report, err := p.PlanPodMigration(t.Context(), object, "")
				if err != nil {
					t.Fatal(err)
				}

				if passes == 0 {
					if !report.Ready || object.Status.Plan == nil ||
						object.Status.Plan.PrecopyPasses != 0 {
						t.Fatalf("zero precopy ready=%t checks=%+v", report.Ready, report.Checks)
					}
				} else if report.Ready || object.Status.Plan != nil || !hasFailedCheck(report.Checks, "resource-quota") {
					t.Fatalf("warm-copy ready=%t checks=%+v", report.Ready, report.Checks)
				}
			},
		)
	}
}
