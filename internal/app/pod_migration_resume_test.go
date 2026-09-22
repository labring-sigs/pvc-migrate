package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type resumingPodController struct {
	fakeController
	checkpoint *v1alpha1.PodMigrationWorkloadStatus
	cause      error
	seen       []v1alpha1.WorkloadSpec
}

func (c *resumingPodController) Resume(
	_ context.Context,
	_, _ string,
	workload v1alpha1.WorkloadSpec,
	_ string,
	_, _ v1alpha1.WorkflowPhase,
	_ []v1alpha1.WorkflowHistoryEntry,
) (*v1alpha1.PodMigrationWorkloadStatus, error) {
	c.resumed++
	c.seen = append(c.seen, *workload.DeepCopy())
	return c.checkpoint, c.cause
}

func TestPodMigrationResumePersistsIdentityBeforeConvergenceFailure(t *testing.T) {
	executor, object, store, _ := podMigrationWarmFixture(t)
	executor.switcher = &scriptedSwitcher{client: executor.client}

	executor.workloads = &fakeController{}
	if err := executor.PauseAndFinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Activate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	planned := object.Status.Plan.DeepCopy()
	cause := errors.New("readiness timeout")
	controller := &resumingPodController{
		cause: cause,
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{
				Name:            "workload",
				UID:             "resumed",
				ResourceVersion: "7",
			},
		},
	}

	executor.workloads = controller
	if err := executor.ResumeWorkload(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error = %v", err)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Workload == nil || loaded.Status.Workload.Pod.UID != "resumed" ||
		loaded.Status.Phase != domain.PhaseFailed || loaded.Status.ResumeFrom != domain.PhaseResuming {
		t.Fatalf("lost recovery state: %+v", loaded.Status)
	}

	controller.cause = nil

	if err := executor.ResumeWorkload(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseCompleted || controller.resumed != 2 ||
		controller.seen[1].Pod.UID != "resumed" || controller.seen[1].Pod.ResourceVersion != "7" {
		t.Fatal("resume did not reuse checkpoint")
	}

	if !reflect.DeepEqual(planned, loaded.Status.Plan) {
		t.Fatal("resume mutated immutable plan")
	}

	controller.checkpoint.Pod.UID = "changed"
	if loaded.Status.Workload.Pod.UID != "resumed" {
		t.Fatal("controller output aliases durable state")
	}

	if err := executor.ResumeWorkload(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if controller.resumed != 2 {
		t.Fatal("completed workload resumed again")
	}
}

func TestPodMigrationResumeSaveFailureRestoresCheckpoint(t *testing.T) {
	executor, object, store, _ := podMigrationWarmFixture(t)
	executor.switcher = &scriptedSwitcher{client: executor.client}

	executor.workloads = &fakeController{}
	if err := executor.PauseAndFinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Activate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	cause := errors.New("resume identity save failed")
	store.err, store.failAt = cause, store.writes+2

	executor.workloads = &resumingPodController{
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{Name: "workload", UID: "resumed"},
		},
	}
	if err := executor.ResumeWorkload(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Workload != nil || object.Status.Phase != domain.PhaseResuming {
		t.Fatal("failed save leaked checkpoint or completed migration")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Workload != nil || loaded.Status.Phase != domain.PhaseResuming {
		t.Fatal("durable checkpoint disagrees")
	}

	if err := executor.ResumeWorkload(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseCompleted || loaded.Status.Workload.Pod.UID != "resumed" {
		t.Fatal("retry did not complete")
	}
}

func TestNamespacedPodMigrationResumePersistsIdentityBeforeConvergenceFailure(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationWarmFixture(t)
	executor.switcher = &scriptedSwitcher{client: executor.client}

	executor.workloads = &fakeController{}
	if err := executor.PauseAndFinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Activate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	planned := object.Status.Plan.DeepCopy()
	cause := errors.New("readiness timeout")
	controller := &resumingPodController{
		cause: cause,
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{
				Name:            "workload",
				UID:             "resumed",
				ResourceVersion: "7",
			},
		},
	}

	executor.workloads = controller
	if err := executor.ResumeWorkload(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error = %v", err)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Workload == nil || loaded.Status.Workload.Pod.UID != "resumed" ||
		loaded.Status.Phase != domain.PhaseFailed || loaded.Status.ResumeFrom != domain.PhaseResuming {
		t.Fatalf("lost recovery state: %+v", loaded.Status)
	}

	controller.cause = nil

	if err := executor.ResumeWorkload(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseCompleted || controller.resumed != 2 ||
		controller.seen[1].Pod.UID != "resumed" || controller.seen[1].Pod.ResourceVersion != "7" {
		t.Fatal("resume did not reuse checkpoint")
	}

	if !reflect.DeepEqual(planned, loaded.Status.Plan) {
		t.Fatal("resume mutated immutable plan")
	}

	controller.checkpoint.Pod.UID = "changed"
	if loaded.Status.Workload.Pod.UID != "resumed" {
		t.Fatal("controller output aliases durable state")
	}

	if err := executor.ResumeWorkload(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if controller.resumed != 2 {
		t.Fatal("completed workload resumed again")
	}
}

func TestNamespacedPodMigrationResumeSaveFailureRestoresCheckpoint(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationWarmFixture(t)
	executor.switcher = &scriptedSwitcher{client: executor.client}

	executor.workloads = &fakeController{}
	if err := executor.PauseAndFinalSync(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Activate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	cause := errors.New("resume identity save failed")
	store.err, store.failAt = cause, store.writes+2

	executor.workloads = &resumingPodController{
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{Name: "workload", UID: "resumed"},
		},
	}
	if err := executor.ResumeWorkload(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Workload != nil || object.Status.Phase != domain.PhaseResuming {
		t.Fatal("failed save leaked checkpoint or completed migration")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Workload != nil || loaded.Status.Phase != domain.PhaseResuming {
		t.Fatal("durable checkpoint disagrees")
	}

	if err := executor.ResumeWorkload(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseCompleted || loaded.Status.Workload.Pod.UID != "resumed" {
		t.Fatal("retry did not complete")
	}
}
