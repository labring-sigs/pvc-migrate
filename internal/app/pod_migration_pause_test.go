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

type checkpointPodController struct {
	fakeController
	checkpoint *v1alpha1.PodMigrationWorkloadStatus
	pauseErr   error
	seen       []v1alpha1.WorkloadSpec
}

func (c *checkpointPodController) Pause(
	_ context.Context,
	_, _ string,
	workload v1alpha1.WorkloadSpec,
	_, _ v1alpha1.WorkflowPhase,
) (*v1alpha1.PodMigrationWorkloadStatus, error) {
	c.paused++
	c.seen = append(c.seen, *workload.DeepCopy())
	return c.checkpoint, c.pauseErr
}

func (c *checkpointPodController) VerifyPaused(
	_ context.Context,
	_, _ string,
	workload v1alpha1.WorkloadSpec,
	_, _ v1alpha1.WorkflowPhase,
) error {
	c.seen = append(c.seen, *workload.DeepCopy())
	return nil
}

func TestNamespacedPodMigrationPausePersistsRecoveryBeforeFailure(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	planned := object.Status.Plan.DeepCopy()
	cause := errors.New("convergence interrupted")
	controller := &checkpointPodController{
		pauseErr: cause,
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{
				Name:            "workload",
				UID:             "replacement",
				ResourceVersion: "42",
				Kind:            "Pod",
				APIVersion:      "v1",
			},
		},
	}

	executor.workloads = controller
	if err := executor.Pause(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error = %v", err)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Workload == nil || loaded.Status.Workload.Pod.UID != "replacement" ||
		loaded.Status.Phase != domain.PhaseFailed || loaded.Status.ResumeFrom != domain.PhasePausing {
		t.Fatalf("checkpoint was not retained: %+v", loaded.Status)
	}

	if !reflect.DeepEqual(planned, object.Status.Plan) {
		t.Fatal("pause changed immutable plan")
	}

	controller.pauseErr = nil

	if err := executor.Pause(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhasePaused || controller.paused != 2 {
		t.Fatalf("retry failed: %+v", loaded.Status)
	}

	for _, workload := range controller.seen[1:] {
		if workload.Pod.UID != "replacement" || workload.Pod.ResourceVersion != "42" ||
			workload.Pod.Kind != "Pod" {
			t.Fatalf("retry lost identity: %+v", workload.Pod)
		}
	}

	controller.checkpoint.Pod.UID = "mutated"
	if loaded.Status.Workload.Pod.UID != "replacement" {
		t.Fatal("checkpoint aliases controller output")
	}

	if err := executor.Pause(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if controller.paused != 2 {
		t.Fatal("idempotent pause invoked controller mutation")
	}
}

func TestNamespacedPodMigrationPauseSaveFailureStopsBeforeVerification(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	cause := errors.New("checkpoint unavailable")
	store.err, store.failAt = cause, store.writes+2
	controller := &checkpointPodController{checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
		Pod: &v1alpha1.LocalResourceReference{Name: "workload", UID: "replacement"},
	}}

	executor.workloads = controller
	if err := executor.Pause(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Workload != nil || object.Status.Phase != domain.PhasePausing {
		t.Fatalf("failed save leaked state: %+v", object.Status)
	}

	if len(controller.seen) != 1 {
		t.Fatal("verification ran after save failure")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Workload != nil || loaded.Status.Phase != domain.PhasePausing {
		t.Fatal("durable checkpoint mismatch")
	}
}
