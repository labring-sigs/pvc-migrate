package app

import (
	"context"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// gateWorkloadController counts workload pauses so the gate tests can prove
// the workload was never touched.
type gateWorkloadController struct {
	pauseCalls int
}

func (g *gateWorkloadController) Pause(
	_ context.Context, _, _ string, _ v1alpha1.WorkloadSpec,
	_, _ v1alpha1.WorkflowPhase,
) (*v1alpha1.PodMigrationWorkloadStatus, error) {
	g.pauseCalls++

	return &v1alpha1.PodMigrationWorkloadStatus{}, nil
}

func (g *gateWorkloadController) ValidateResume(
	_ context.Context, _, _ string, _ v1alpha1.WorkloadSpec,
	_, _ v1alpha1.WorkflowPhase,
) error {
	return nil
}

func (g *gateWorkloadController) Resume(
	_ context.Context, _, _ string, _ v1alpha1.WorkloadSpec, _ string,
	_, _ v1alpha1.WorkflowPhase, _ []v1alpha1.WorkflowHistoryEntry,
) (*v1alpha1.PodMigrationWorkloadStatus, error) {
	return &v1alpha1.PodMigrationWorkloadStatus{}, nil
}

func (g *gateWorkloadController) VerifyPaused(
	_ context.Context, _, _ string, _ v1alpha1.WorkloadSpec,
	_, _ v1alpha1.WorkflowPhase,
) error {
	return nil
}

func (g *gateWorkloadController) CurrentRollbackPods(
	_ context.Context, _, _ string, _ v1alpha1.WorkloadSpec,
) ([]v1alpha1.ObjectReference, error) {
	return nil, nil
}

func warmCopiedMigration(object *v1alpha1.PodMigration) {
	object.Spec.PrecopyPasses = 1
	object.Status.Plan.PrecopyPasses = 1
	object.Status.WarmPassesCompleted = 1
	object.Status.Phase = domain.PhaseWarmCopied

	for _, volume := range object.Status.Plan.Volumes {
		object.Status.Volumes = append(object.Status.Volumes,
			v1alpha1.PodMigrationVolumeStatus{
				VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
					SourcePVCName: volume.SourcePVC.Name,
					Reserved:      true,
					DestinationPVC: &v1alpha1.LocalResourceReference{
						Name: "reserved-" + volume.SourcePVC.Name,
						UID:  types.UID("reserved-" + volume.SourcePVC.Name),
					},
					DestinationPV: &v1alpha1.LocalResourceReference{
						Name: "reserved-pv-" + volume.SourcePVC.Name,
						UID:  types.UID("reserved-pv-" + volume.SourcePVC.Name),
					},
					DestinationPolicy: corev1.PersistentVolumeReclaimRetain,
				},
				Sync: v1alpha1.PodMigrationSyncStatus{
					WarmCompletedAt: &metav1.Time{Time: time.Unix(100, 0).UTC()},
				},
			},
		)
	}
}

func cutoverGateFixture(
	t *testing.T,
	configure ...func(*v1alpha1.PodMigration),
) (*PodMigrationExecutor, *v1alpha1.PodMigration, *gateWorkloadController, *namespacedPodMigrationCheckpointStore) {
	t.Helper()

	executor, object, store, _ := namespacedPodMigrationFixture(t, configure...)
	workloads := &gateWorkloadController{}
	executor.workloads = workloads

	return executor, object, workloads, store
}

// TestPodMigrationManualCutoverHoldsAtWarmCopied pins the declarative gate:
// with spec.manualCutover the run rests at WarmCopied with the workload
// running, records why it is not advancing once, and reports success.
func TestPodMigrationManualCutoverHoldsAtWarmCopied(t *testing.T) {
	executor, object, workloads, _ := cutoverGateFixture(t, func(object *v1alpha1.PodMigration) {
		warmCopiedMigration(object)
		object.Spec.ManualCutover = true
	})

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatalf("held migration returned an error: %v", err)
	}

	if object.Status.Phase != domain.PhaseWarmCopied {
		t.Fatalf("phase = %s, want WarmCopied", object.Status.Phase)
	}

	if object.Status.Message != manualCutoverHoldMessage {
		t.Fatalf("hold message = %q, want %q", object.Status.Message, manualCutoverHoldMessage)
	}

	if workloads.pauseCalls != 0 {
		t.Fatalf("workload was paused %d time(s) under manual cutover", workloads.pauseCalls)
	}

	history := len(object.Status.History)

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatalf("second held run returned an error: %v", err)
	}

	if len(object.Status.History) != history {
		t.Fatalf(
			"repeated holds appended history entries: %d grew to %d",
			history, len(object.Status.History),
		)
	}
}

// TestPodMigrationPauseApprovalDeclinedKeepsWorkloadRunning pins the
// interactive gate: a declined approval leaves the record at WarmCopied with
// the workload untouched and surfaces ErrPauseApproval.
func TestPodMigrationPauseApprovalDeclinedKeepsWorkloadRunning(t *testing.T) {
	executor, object, workloads, _ := cutoverGateFixture(t, warmCopiedMigration)

	declined := errors.New("wrong value typed")
	err := executor.RunWithPauseApproval(
		t.Context(), object,
		func(context.Context, *v1alpha1.PodMigration) error { return declined },
	)

	if !errors.Is(err, ErrPauseApproval) || !errors.Is(err, declined) {
		t.Fatalf("declined approval error = %v, want ErrPauseApproval wrapping the cause", err)
	}

	if object.Status.Phase != domain.PhaseWarmCopied {
		t.Fatalf("phase = %s, want WarmCopied", object.Status.Phase)
	}

	if object.Status.Message == manualCutoverHoldMessage {
		t.Fatal("declined approval recorded the manual-cutover hold message")
	}

	if workloads.pauseCalls != 0 {
		t.Fatal("workload was paused after a declined approval")
	}
}

// TestPodMigrationPauseApprovalPassesToPause proves an approved callback lets
// the pause proceed: the workload controller is invoked exactly once.
func TestPodMigrationPauseApprovalPassesToPause(t *testing.T) {
	executor, object, workloads, _ := cutoverGateFixture(t, warmCopiedMigration)

	approved := false
	approve := func(context.Context, *v1alpha1.PodMigration) error {
		approved = true

		return nil
	}

	err := executor.RunWithPauseApproval(t.Context(), object, approve)
	if err != nil {
		t.Fatalf("approved migration failed: %v", err)
	}

	if !approved {
		t.Fatal("pause approval was never requested")
	}

	if workloads.pauseCalls != 1 {
		t.Fatalf("workload paused %d time(s), want exactly 1", workloads.pauseCalls)
	}

	if object.Status.Phase == domain.PhaseWarmCopied {
		t.Fatal("approved migration never left WarmCopied")
	}
}

// TestRequestResumeApprovesManualCutover pins the resume verb as the
// declaration that approves the held pause: the phase moves to Pausing, the
// approved edge the run loop then converges without any further gate.
func TestRequestResumeApprovesManualCutover(t *testing.T) {
	executor, object, workloads, _ := cutoverGateFixture(t, func(object *v1alpha1.PodMigration) {
		warmCopiedMigration(object)
		object.Spec.ManualCutover = true
	})

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatalf("held migration returned an error: %v", err)
	}

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatalf("resume request failed: %v", err)
	}

	if object.Status.Phase != domain.PhasePausing {
		t.Fatalf("phase = %s, want Pausing", object.Status.Phase)
	}

	if object.Status.Message != "manual cutover approved by resume request" {
		t.Fatalf("approval message = %q", object.Status.Message)
	}

	if workloads.pauseCalls != 0 {
		t.Fatal("resume request paused the workload itself")
	}
}

// TestPodMigrationClearingManualCutoverApprovesPause pins the declarative
// approval by spec edit: once the held workflow's manualCutover field is
// cleared through the API, the next loaded run pauses the workload without
// any further gate.
func TestPodMigrationClearingManualCutoverApprovesPause(t *testing.T) {
	executor, object, workloads, store := cutoverGateFixture(
		t,
		func(object *v1alpha1.PodMigration) {
			warmCopiedMigration(object)
			object.Spec.ManualCutover = true
		},
	)

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatalf("held migration returned an error: %v", err)
	}

	if workloads.pauseCalls != 0 {
		t.Fatal("held migration paused the workload")
	}

	edited := object.DeepCopy()

	edited.Spec.ManualCutover = false
	if err := store.client.Update(t.Context(), edited); err != nil {
		t.Fatalf("clearing manualCutover through the API failed: %v", err)
	}

	reloaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatalf("reload after the spec edit failed: %v", err)
	}

	if err := executor.Run(t.Context(), reloaded); err != nil {
		t.Fatalf("migration after clearing manualCutover failed: %v", err)
	}

	if workloads.pauseCalls != 1 {
		t.Fatalf("workload paused %d time(s) after approval, want exactly 1", workloads.pauseCalls)
	}

	if reloaded.Status.Phase == domain.PhaseWarmCopied {
		t.Fatal("approved migration never left WarmCopied")
	}
}

// TestRequestResumeIgnoresIncompleteWarmCopy keeps the resume request from
// approving a pause while warm-copy passes are still pending.
func TestRequestResumeIgnoresIncompleteWarmCopy(t *testing.T) {
	executor, object, _, _ := cutoverGateFixture(t, func(object *v1alpha1.PodMigration) {
		warmCopiedMigration(object)
		object.Spec.PrecopyPasses = 2
		object.Status.Plan.PrecopyPasses = 2
		object.Status.WarmPassesCompleted = 1
		object.Spec.ManualCutover = true
	})
	installPodWarmSourcePVs(t, executor.client, object.Status.Plan.Volumes)

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatalf("resume request failed: %v", err)
	}

	if object.Status.Phase != domain.PhaseWarmCopied {
		t.Fatalf("phase = %s, want WarmCopied", object.Status.Phase)
	}
}
