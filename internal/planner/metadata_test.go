package planner

import (
	"context"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
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

	plan, err := New(
		plannerClient(objects...),
		nil,
	).plan(context.Background(), domain.OperationMigrate, transferInput{
		Volumes: testSourceVolumes("data"), SessionID: "migration-finalizer",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",

		TransferOptions: v1alpha1.TransferOptions{
			TargetNode:              "node-b",
			DestinationStorageClass: "fast",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheckContaining(
		plan.Checks,
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

			plan, err := New(
				plannerClient(objects...),
				nil,
			).plan(context.Background(), tt.operation, transferInput{
				Volumes: testSourceVolumes("data"), SessionID: "metadata-finalizer",
				SourceNamespace:    "app",
				TemporaryNamespace: "system",
				StagingNamespace:   "system",
				SessionNamespace:   "system",

				TransferOptions: v1alpha1.TransferOptions{
					TargetNode:              "node-b",
					DestinationStorageClass: "fast",
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			if !plan.Ready || hasFailedCheck(
				plan.Checks, "pvc-finalizers",
			) {
				t.Fatalf("plan checks=%#v", plan.Checks)
			}
		})
	}
}
