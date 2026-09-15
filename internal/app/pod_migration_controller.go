package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

type workloadController interface {
	Pause(ctx context.Context, owner, namespace string, workload v1alpha1.WorkloadSpec,
		phase, resumeFrom v1alpha1.WorkflowPhase) (*v1alpha1.PodMigrationWorkloadStatus, error)
	ValidateResume(ctx context.Context, owner, namespace string, workload v1alpha1.WorkloadSpec,
		phase, resumeFrom v1alpha1.WorkflowPhase) error
	Resume(ctx context.Context, owner, namespace string, workload v1alpha1.WorkloadSpec,
		resumeNode string, phase, resumeFrom v1alpha1.WorkflowPhase,
		history []v1alpha1.WorkflowHistoryEntry) (*v1alpha1.PodMigrationWorkloadStatus, error)
	VerifyPaused(ctx context.Context, owner, namespace string, workload v1alpha1.WorkloadSpec,
		phase, resumeFrom v1alpha1.WorkflowPhase) error
	CurrentRollbackPods(
		ctx context.Context,
		owner, namespace string,
		workload v1alpha1.WorkloadSpec,
	) ([]v1alpha1.ObjectReference, error)
}
