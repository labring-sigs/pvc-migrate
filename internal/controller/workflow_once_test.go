package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestReconcileUntilStableConsumesRequeue(t *testing.T) {
	passes := 0

	err := reconcileUntilStable(
		t.Context(),
		reconcile.Request{},
		func(context.Context, reconcile.Request) (reconcile.Result, error) {
			passes++
			if passes == 1 {
				return reconcile.Result{RequeueAfter: time.Millisecond}, nil
			}

			return reconcile.Result{}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if passes != 2 {
		t.Fatalf("reconcile passes=%d, want 2", passes)
	}
}

func TestReconcileUntilStableRejectsPermanentRequeue(t *testing.T) {
	passes := 0

	err := reconcileUntilStable(
		t.Context(),
		reconcile.Request{},
		func(context.Context, reconcile.Request) (reconcile.Result, error) {
			passes++
			return reconcile.Result{Requeue: true}, nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("permanent requeue error=%v", err)
	}

	if passes != maxOneShotReconciliations {
		t.Fatalf("reconcile passes=%d, want %d", passes, maxOneShotReconciliations)
	}
}

func TestReconcileUntilStableRejectsUnboundedDelay(t *testing.T) {
	err := reconcileUntilStable(
		t.Context(),
		reconcile.Request{},
		func(context.Context, reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{RequeueAfter: maxOneShotRequeueDelay + time.Second}, nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "exceeding") {
		t.Fatalf("unbounded delay error=%v", err)
	}
}

func TestRequireReconcilerCoversEveryWorkflowKind(t *testing.T) {
	tests := []struct {
		kind  domain.ControllerKind
		setup func(*WorkflowReconciler)
	}{
		{domain.ControllerKindBackup, func(r *WorkflowReconciler) { r.backup = &BackupReconciler{} }},
		{domain.ControllerKindRestore, func(r *WorkflowReconciler) { r.restore = &RestoreReconciler{} }},
		{domain.ControllerKindRename, func(r *WorkflowReconciler) { r.rename = &RenameReconciler{} }},
		{domain.ControllerKindMove, func(r *WorkflowReconciler) { r.move = &MoveReconciler{} }},
		{domain.ControllerKindReservation, func(r *WorkflowReconciler) { r.namespacedReservation = &ReservationReconciler{} }},
		{domain.ControllerKindClusterReservation, func(r *WorkflowReconciler) { r.reservation = &ClusterReservationReconciler{} }},
		{domain.ControllerKindCopy, func(r *WorkflowReconciler) { r.namespacedCopy = &CopyReconciler{} }},
		{domain.ControllerKindClusterCopy, func(r *WorkflowReconciler) { r.copy = &ClusterCopyReconciler{} }},
		{domain.ControllerKindMigration, func(r *WorkflowReconciler) { r.namespacedMigration = &MigrationReconciler{} }},
		{domain.ControllerKindClusterMigration, func(r *WorkflowReconciler) { r.migration = &ClusterMigrationReconciler{} }},
		{domain.ControllerKindPodMigration, func(r *WorkflowReconciler) { r.namespacedPodMigration = &PodMigrationReconciler{} }},
		{domain.ControllerKindClusterPodMigration, func(r *WorkflowReconciler) { r.podMigration = &ClusterPodMigrationReconciler{} }},
	}

	for _, test := range tests {
		t.Run(string(test.kind), func(t *testing.T) {
			reconciler := NewWorkflowReconciler()
			if err := reconciler.requireReconciler(test.kind); err == nil {
				t.Fatal("unconfigured reconciler was accepted")
			}

			test.setup(reconciler)
			if err := reconciler.requireReconciler(test.kind); err != nil {
				t.Fatalf("configured reconciler rejected: %v", err)
			}
		})
	}
}

func TestRequireReconcilerRejectsUnknownKind(t *testing.T) {
	if err := NewWorkflowReconciler().requireReconciler("Unknown"); err == nil {
		t.Fatal("unknown workflow kind was accepted")
	}
}

func TestOneShotInventoryUsesConcreteControllersAndContinuesAfterFailure(t *testing.T) {
	reservation := &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{Name: "unavailable", UID: "reservation"},
		Spec: v1alpha1.ClusterReservationSpec{
			SourceNamespace:  "source",
			SessionNamespace: "system",
		},
	}
	deleting := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "deleted",
			UID:        "copy",
			Finalizers: []string{kube.SessionFinalizer},
		},
		Spec: v1alpha1.ClusterCopySpec{SourceNamespace: "system"},
	}

	r, client := newTransferControllerFixture(t, reservation, deleting)
	if err := client.Delete(t.Context(), deleting); err != nil {
		t.Fatal(err)
	}
	// No legacy session store or service is configured: either fallback would
	// fail instead of preserving the operation's concrete failure checkpoint.
	err := r.reconcileInventory(t.Context(), client)
	if err == nil || !strings.Contains(err.Error(), "source unavailable") {
		t.Fatalf("one-shot omitted business failure: %v", err)
	}

	if err := client.Get(
		t.Context(),
		crclient.ObjectKeyFromObject(reservation),
		reservation,
	); err != nil {
		t.Fatal(err)
	}

	if reservation.Status.Phase != domain.PhaseFailed ||
		reservation.Status.ResumeFrom != domain.PhasePlanned {
		t.Fatal("one-shot did not persist concrete planning failure")
	}

	if err := client.Get(
		t.Context(),
		crclient.ObjectKeyFromObject(deleting),
		deleting,
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("one-shot skipped independent deletion: %v", err)
	}
}

func TestWorkflowQueuesAdmitHandoffMetadataAndRepeatedCopyPass(t *testing.T) {
	previous := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy"},
		Status: v1alpha1.ClusterCopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseWarmCopied},
		},
	}
	current := previous.DeepCopy()

	current.Status.Phase = domain.PhaseWarmCopying
	if !workflowQueuePredicate(
		false,
		func(crclient.Object) {},
	).Update(event.UpdateEvent{ObjectOld: previous, ObjectNew: current}) {
		t.Fatal("explicit repeated copy pass did not wake execution")
	}

	current = previous.DeepCopy()
	current.Annotations = map[string]string{"migrate.sealos.io/reservation-copy-pending": "handoff"}

	update := event.UpdateEvent{ObjectOld: previous, ObjectNew: current}
	if !workflowRecoveryQueuePredicate(func(crclient.Object) {}).Update(update) {
		t.Fatal("metadata-only handoff did not wake recovery")
	}

	if workflowQueuePredicate(false, func(crclient.Object) {}).Update(update) {
		t.Fatal("pending handoff entered ordinary execution queue")
	}

	current = previous.DeepCopy()

	current.Annotations = map[string]string{"example.com/note": "unrelated"}
	if workflowEventPredicate().Update(event.UpdateEvent{ObjectOld: previous, ObjectNew: current}) {
		t.Fatal("unrelated annotation triggered execution")
	}
}
