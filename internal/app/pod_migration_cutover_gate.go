package app

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ErrPauseApproval reports that the operator declined the approval that
// guards stopping the workload for the final sync. The workflow record is
// left at its warm-copy phase with the workload untouched and can be resumed.
var ErrPauseApproval = errors.New("workload pause approval declined")

// errPodMigrationHeld marks a run stopped by spec.manualCutover after the
// warm copy passes completed. It stays internal: the public entry points map
// it to a nil error, because the hold is a successful stopping point rather
// than a failure.
var errPodMigrationHeld = errors.New("pod migration held for manual cutover")

const manualCutoverHoldMessage = "warm copy complete; manual cutover holds the workload running until a resume request approves the pause and final sync"

// podCutoverGatePhase reports whether the workload is still running and its
// pause has not been attempted yet — exactly the states where stopping the
// workload still needs an approval. PhasePausing and later states have
// already been approved and simply converge.
func podCutoverGatePhase(phase v1alpha1.WorkflowPhase) bool {
	return phase == domain.PhaseReserved || phase == domain.PhaseWarmCopied
}

// podCutoverResumePending reports whether a manual-cutover workflow is
// resting ahead of the pause and a resume request is what approves it.
func podCutoverResumePending(object *v1alpha1.PodMigration) bool {
	phase := workflowResumePhase(object.Status.WorkflowStatus)

	return object.Spec.ManualCutover &&
		object.Status.Plan != nil &&
		podCutoverGatePhase(phase) &&
		object.Status.Plan.PrecopyPasses <= object.Status.WarmPassesCompleted
}

// cutoverGate applies the manual-cutover hold and the interactive pause
// approval before the workload stops. The gate only runs in gate phases, so
// an interrupted or already-approved pause converges without prompting.
func (m *PodMigrationExecutor) cutoverGate(
	ctx context.Context,
	object *v1alpha1.PodMigration,
	pauseApproval func(context.Context, *v1alpha1.PodMigration) error,
) error {
	if object.Spec.ManualCutover {
		if object.Status.Message == manualCutoverHoldMessage {
			return errPodMigrationHeld
		}

		// Record why the workflow is not advancing without inventing a new
		// phase; the message guard keeps controller requeues from rewriting
		// the checkpoint on every pass.
		previous := object.Status.Message
		object.Status.Message = domain.BoundWorkflowMessage(manualCutoverHoldMessage)
		object.Status.UpdatedAt = metav1.NewTime(m.now().UTC())

		if err := persistCheckpoint(
			ctx,
			func(ctx context.Context) error { return m.store.Save(ctx, object) },
		); err != nil {
			object.Status.Message = previous
			return err
		}

		return errPodMigrationHeld
	}

	if pauseApproval == nil {
		return nil
	}

	if err := pauseApproval(ctx, object); err != nil {
		return errors.Join(ErrPauseApproval, err)
	}

	return nil
}
