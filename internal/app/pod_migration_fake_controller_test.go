package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

// fakeController is the default workload-controller double: it counts calls,
// records the workload identities it observed, and returns no recovery
// checkpoint. Test-local variants embed it and override single methods.
type fakeController struct {
	paused         int
	resumed        int
	seen           []v1alpha1.WorkloadSpec
	pausedWorkload v1alpha1.WorkloadSpec
	resumeErr      error
}

func (f *fakeController) Pause(
	_ context.Context,
	_, _ string,
	workload v1alpha1.WorkloadSpec,
	_, _ v1alpha1.WorkflowPhase,
) (*v1alpha1.PodMigrationWorkloadStatus, error) {
	f.paused++
	f.pausedWorkload = *workload.DeepCopy()
	f.seen = append(f.seen, *workload.DeepCopy())
	return nil, nil
}

func (f *fakeController) Resume(
	_ context.Context,
	_, _ string,
	workload v1alpha1.WorkloadSpec,
	_ string,
	_, _ v1alpha1.WorkflowPhase,
	_ []v1alpha1.WorkflowHistoryEntry,
) (*v1alpha1.PodMigrationWorkloadStatus, error) {
	f.resumed++
	f.seen = append(f.seen, *workload.DeepCopy())
	return nil, f.resumeErr
}

func (f *fakeController) ValidateResume(
	context.Context,
	string,
	string,
	v1alpha1.WorkloadSpec,
	v1alpha1.WorkflowPhase,
	v1alpha1.WorkflowPhase,
) error {
	return nil
}

func (f *fakeController) VerifyPaused(
	context.Context,
	string,
	string,
	v1alpha1.WorkloadSpec,
	v1alpha1.WorkflowPhase,
	v1alpha1.WorkflowPhase,
) error {
	return nil
}

func (f *fakeController) CurrentRollbackPods(
	context.Context,
	string,
	string,
	v1alpha1.WorkloadSpec,
) ([]v1alpha1.ObjectReference, error) {
	return nil, nil
}
