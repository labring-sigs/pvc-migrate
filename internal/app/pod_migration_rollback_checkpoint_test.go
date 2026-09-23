package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type rollbackCheckpointController struct {
	fakeController
	current        []v1alpha1.ObjectReference
	pausedWorkload v1alpha1.WorkloadSpec
}

func (c *rollbackCheckpointController) CurrentRollbackPods(
	context.Context,
	string,
	string,
	v1alpha1.WorkloadSpec,
) ([]v1alpha1.ObjectReference, error) {
	return c.current, nil
}

func (c *rollbackCheckpointController) Pause(
	_ context.Context,
	_, _ string,
	workload v1alpha1.WorkloadSpec,
	_, _ v1alpha1.WorkflowPhase,
) (*v1alpha1.PodMigrationWorkloadStatus, error) {
	c.paused++
	c.pausedWorkload = *workload.DeepCopy()
	return nil, nil
}

func TestNamespacedPodMigrationRollbackCheckpointsCurrentPodBeforePause(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}
	executor.transfer.copier = &concreteCopyEngine{}
	executor.transfer.config.Retries = 1
	switcher := &scriptedSwitcher{client: executor.client}

	executor.switcher = switcher
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	controller := &rollbackCheckpointController{
		current: []v1alpha1.ObjectReference{
			{Name: "workload", Namespace: object.Namespace, UID: "current", ResourceVersion: "17"},
		},
	}
	executor.workloads = controller
	cause := errors.New("current identity checkpoint unavailable")

	store.err, store.failAt = cause, store.writes+1
	if err := executor.Rollback(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}

	if object.Status.Workload != nil || object.Status.Phase != domain.PhaseCompleted ||
		controller.paused != 0 ||
		len(switcher.rollbackCalls) != 0 {
		t.Fatal("failed identity checkpoint reached pause or storage changes")
	}

	if err := executor.Rollback(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if controller.paused != 1 || controller.pausedWorkload.Pod.UID != "current" ||
		controller.pausedWorkload.Pod.ResourceVersion != "17" ||
		object.Status.Phase != domain.PhaseRolledBack {
		t.Fatal("rollback pause did not use the durable current identity")
	}
}

func TestNamespacedPodMigrationRollbackResumeFailureDoesNotPauseRestoredWorkloadAgain(
	t *testing.T,
) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}
	executor.transfer.copier = &concreteCopyEngine{}
	executor.transfer.config.Retries = 1
	switcher := &scriptedSwitcher{client: executor.client}

	executor.switcher = switcher
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	cause := errors.New("source workload not ready")
	controller := &resumingPodController{
		cause: cause,
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{
				Name:            "workload",
				UID:             "restored",
				ResourceVersion: "12",
			},
		},
	}
	executor.workloads = controller

	original := object.Status.Plan.DeepCopy()
	if err := executor.Rollback(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseRollingBack ||
		!executor.rollbackVolumesRestored(object) {
		t.Fatal("lost restored volumes")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Workload.Pod.UID != "restored" {
		t.Fatal("restored workload identity was not saved")
	}

	controller.cause = nil

	if err := executor.Run(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if controller.paused != 1 || controller.resumed != 2 || len(switcher.rollbackCalls) != 2 ||
		controller.seen[1].Pod.UID != "restored" ||
		loaded.Status.Phase != domain.PhaseRolledBack {
		t.Fatal("retry repeated pause/storage or discarded restored workload identity")
	}

	if !reflect.DeepEqual(original, loaded.Status.Plan) {
		t.Fatal("rollback changed plan")
	}
}

func TestNamespacedPodMigrationRollbackRejectsUnrelatedConsumerBeforePause(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}
	executor.transfer.copier = &concreteCopyEngine{}
	executor.transfer.config.Retries = 1
	switcher := &scriptedSwitcher{client: executor.client}

	executor.switcher = switcher
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	controller := &rollbackCheckpointController{}
	executor.workloads = controller

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: object.Namespace, UID: "foreign"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "a",
						},
					},
				},
			},
		},
	}
	if _, err := executor.client.CoreV1().
		Pods(object.Namespace).
		Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	writes := store.writes

	if err := executor.Rollback(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("error=%v", err)
	}

	if writes != store.writes || controller.paused != 0 || len(switcher.rollbackCalls) != 0 {
		t.Fatal("unrelated consumer reached destructive rollback")
	}
}
