package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
)

func TestPlanChecksSourceNodeAgainstEveryPV(t *testing.T) {
	for _, test := range []struct {
		name       string
		affinity   string
		secondOnly bool
		wantReady  bool
	}{
		{name: "unrestricted", wantReady: true},
		{name: "matching", affinity: "node-b", wantReady: true},
		{name: "conflicting", affinity: "node-a"},
		{name: "second volume conflicts", affinity: "node-a", secondOnly: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := plannerObjectsWithTwoPVCs(t)
			for _, object := range objects {
				pv, isPV := object.(*corev1.PersistentVolume)
				if !isPV || test.affinity == "" || (test.secondOnly && pv.Name == "pv-source") {
					continue
				}

				pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      corev1.LabelHostname,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{test.affinity},
							},
						}},
					},
				}}
			}

			plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
				SessionID:          "copy",
				Operation:          domain.OperationCopy,
				SourceNamespace:    "app",
				TemporaryNamespace: "system",
				StagingNamespace:   "system",
				SessionNamespace:   "system",
				SourcePVCs: []string{
					"data",
					"logs",
				},
				SourceNode:       "node-b",
				TargetNode:       "node-b",
				DestinationClass: "fast",
			})
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready != test.wantReady {
				t.Fatalf("ready=%t, want %t: %#v", plan.Ready, test.wantReady, plan.Checks)
			}

			if !test.wantReady {
				for _, check := range plan.Checks {
					if check.Name == domain.CheckNameSourceNode && !check.Passed &&
						strings.Contains(check.Message, "node affinity") {
						return
					}
				}

				t.Fatal("source PV affinity failure missing")
			}
		})
	}
}
