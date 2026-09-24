package app

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// armTerminatingSourcePVC marks one of the fixture's seeded source PVCs as
// terminating, the state an armed deletion leaves while a finalizer holds it.
func armTerminatingSourcePVC(
	t *testing.T,
	executor *PodMigrationExecutor,
	namespace string,
	terminating string,
) {
	t.Helper()

	pvc, err := executor.client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(t.Context(), terminating, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pvc.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	if _, err := executor.client.CoreV1().
		PersistentVolumeClaims(namespace).
		Update(t.Context(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestFailSourceDeletedTerminatingSourceFollowsPhase(t *testing.T) {
	tests := []struct {
		name     string
		phase    v1alpha1.WorkflowPhase
		terminat bool
	}{
		{name: "planned", phase: domain.PhasePlanned, terminat: true},
		{name: "warm copying", phase: domain.PhaseWarmCopying, terminat: true},
		{name: "paused", phase: domain.PhasePaused, terminat: true},
		{name: "final syncing", phase: domain.PhaseFinalSyncing, terminat: true},
		{name: "final synced", phase: domain.PhaseFinalSynced},
		{name: "activating", phase: domain.PhaseActivating},
		{name: "activated", phase: domain.PhaseActivated},
		{name: "aborting", phase: domain.PhaseAborting},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			executor, object, _, _ := namespacedPodMigrationFixture(t)
			executor.workloads = &fakeController{}

			object.Status.Phase = testCase.phase
			armTerminatingSourcePVC(t, executor, object.Namespace, "a")

			err := executor.FailSourceDeleted(t.Context(), object)
			if testCase.terminat {
				if err == nil {
					t.Fatal("expected the terminating-source failure")
				}

				if !strings.Contains(err.Error(), "data/a is terminating") {
					t.Fatalf("failure does not name the terminating source: %v", err)
				}

				if object.Status.Phase != domain.PhaseFailed {
					t.Fatalf("phase = %s, want Failed", object.Status.Phase)
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if object.Status.Phase != testCase.phase {
				t.Fatalf(
					"phase = %s, want it preserved at %s",
					object.Status.Phase,
					testCase.phase,
				)
			}
		})
	}
}

func TestFailSourceDeletedDeletedWinsOverTerminating(t *testing.T) {
	executor, object, _, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}

	// Volume "a" lingers terminating while volume "b" is fully gone: the
	// harder loss must decide the failure, at every phase that probes at all.
	armTerminatingSourcePVC(t, executor, object.Namespace, "a")

	if err := executor.client.CoreV1().
		PersistentVolumeClaims(object.Namespace).
		Delete(t.Context(), "b", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	object.Status.Phase = domain.PhaseFinalSynced

	err := executor.FailSourceDeleted(t.Context(), object)
	if err == nil {
		t.Fatal("expected the recorded source-loss failure")
	}

	if !strings.Contains(err.Error(), "source PVC no longer exists") {
		t.Fatalf("failure does not report the deleted source: %v", err)
	}

	if object.Status.Phase != domain.PhaseFailed {
		t.Fatalf("phase = %s, want Failed", object.Status.Phase)
	}
}

func TestFailSourceDeletedStillFailsDeletedSourceAfterFinalSync(t *testing.T) {
	executor, object, _, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}

	deletePlannedSourcePVCs(t, executor.client, object.Namespace, object.Status.Plan.Volumes)
	object.Status.Phase = domain.PhaseFinalSynced

	if err := executor.FailSourceDeleted(t.Context(), object); err == nil {
		t.Fatal("expected the recorded source-loss failure")
	}

	if object.Status.Phase != domain.PhaseFailed {
		t.Fatalf("phase = %s, want Failed", object.Status.Phase)
	}
}

func TestPodMigrationValidateAbortRejectsTerminatingSource(t *testing.T) {
	executor, object, _, _ := namespacedPodMigrationFixture(t)
	executor.workloads = &fakeController{}

	// A paused workflow needs the workload resumed onto the source, and the
	// scheduler refuses pods mounting a claim that is being deleted.
	namespacedPausedCheckpointFixture(object)
	armTerminatingSourcePVC(t, executor, object.Namespace, "a")

	err := executor.ValidateAbort(t.Context(), object)
	if err == nil {
		t.Fatal("abort accepted a terminating source")
	}

	if !strings.Contains(err.Error(), "data/a is terminating") {
		t.Fatalf("failure does not name the terminating source: %v", err)
	}
}

func TestValidateFinalSyncRejectsLostSource(t *testing.T) {
	tests := []struct {
		name    string
		seed    string
		message string
	}{
		{
			name:    "terminating",
			seed:    "a",
			message: "data/a is terminating (deletion requested at",
		},
		{
			name:    "deleted",
			message: "source PVC no longer exists",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			executor, object, _, _ := namespacedPodMigrationFixture(t)
			executor.workloads = &fakeController{}
			object.Status.Phase = domain.PhasePaused

			for _, volume := range object.Status.Plan.Volumes {
				object.Status.Volumes = append(
					object.Status.Volumes,
					v1alpha1.PodMigrationVolumeStatus{
						VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
							SourcePVCName:     volume.SourcePVC.Name,
							Reserved:          true,
							DestinationPolicy: corev1.PersistentVolumeReclaimRetain,
							DestinationPVC: &v1alpha1.LocalResourceReference{
								Kind: "PersistentVolumeClaim",
								Name: volume.DestinationPVC.Name,
								UID:  types.UID(volume.DestinationPVC.Name),
							},
							DestinationPV: &v1alpha1.LocalResourceReference{
								Kind: "PersistentVolume",
								Name: "pv-" + volume.DestinationPVC.Name,
								UID:  types.UID("pv-" + volume.DestinationPVC.Name),
							},
						},
					},
				)
			}

			if testCase.seed != "" {
				armTerminatingSourcePVC(t, executor, object.Namespace, testCase.seed)
			} else {
				deletePlannedSourcePVCs(
					t,
					executor.client,
					object.Namespace,
					object.Status.Plan.Volumes,
				)
			}

			err := executor.ValidateFinalSync(t.Context(), object)
			if err == nil {
				t.Fatal("final sync accepted a lost source")
			}

			if !strings.Contains(err.Error(), testCase.message) {
				t.Fatalf("failure does not describe the lost source: %v", err)
			}

			if object.Status.Phase != domain.PhasePaused {
				t.Fatalf("validation mutated phase to %s", object.Status.Phase)
			}
		})
	}
}
