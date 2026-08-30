package planner

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

func TestPlanRejectsCustomPVCFinalizerBeforeRecreate(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok {
			pvc.Finalizers = []string{"apps.victoriametrics.com/finalizer"}
		}
	}

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		Operation:          domain.OperationMigrate,
		SessionID:          "migration-finalizer",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheckContaining(
		plan,
		"pvc-finalizers",
		"apps.victoriametrics.com/finalizer",
	) {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestPlanAllowsPVCProtectionFinalizerAndCopyKeepsSource(t *testing.T) {
	tests := []struct {
		name       string
		operation  domain.Operation
		finalizers []string
	}{
		{
			name:       "protection finalizer",
			operation:  domain.OperationMigrate,
			finalizers: []string{kube.PVCProtectionFinalizer},
		},
		{
			name:       "copy source with custom finalizer",
			operation:  domain.OperationCopy,
			finalizers: []string{"apps.victoriametrics.com/finalizer"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := plannerObjects("2Gi")
			for _, object := range objects {
				if pvc, ok := object.(*corev1.PersistentVolumeClaim); ok {
					pvc.Finalizers = tt.finalizers
				}
			}

			plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
				Operation:          tt.operation,
				SessionID:          "metadata-finalizer",
				SourceNamespace:    "app",
				TemporaryNamespace: "system",
				StagingNamespace:   "system",
				SessionNamespace:   "system",
				SourcePVCs:         []string{"data"},
				TargetNode:         "node-b",
				DestinationClass:   "fast",
			})
			if err != nil {
				t.Fatal(err)
			}

			if !plan.Ready || hasFailedCheck(plan, "pvc-finalizers") {
				t.Fatalf("plan checks=%#v", plan.Checks)
			}
		})
	}
}
