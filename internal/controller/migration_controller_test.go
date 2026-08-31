package controller

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestWorkflowReconcilerFiltersInstalledKinds(t *testing.T) {
	all := NewWorkflowReconciler(nil, nil)
	if !all.supportsKind("Migration") || !all.supportsKind("Backup") {
		t.Fatal("default reconciler must support the complete workflow set")
	}

	partial := NewWorkflowReconciler(nil, nil).WithSupportedKinds([]domain.ControllerKind{
		domain.ControllerKindMigration,
		domain.ControllerKindCopy,
	})
	if !partial.supportsKind("Migration") || !partial.supportsKind("Copy") {
		t.Fatal("partial reconciler omitted an installed kind")
	}

	if partial.supportsKind("Backup") {
		t.Fatal("partial reconciler included a missing kind")
	}
}

func TestKubeBlocksProtocolMappings(t *testing.T) {
	if got := kubeBlocksClusterField(
		"apps.kubeblocks.io/v1alpha1",
	); got != kubeBlocksFieldClusterRef {
		t.Fatalf("legacy cluster field=%q", got)
	}

	if got := kubeBlocksClusterField(kubeBlocksOpsAPIVersion); got != kubeBlocksFieldClusterName {
		t.Fatalf("component-scoped cluster field=%q", got)
	}

	for _, phase := range []kubeBlocksPhase{
		kubeBlocksPhaseFailed,
		kubeBlocksPhaseCancelled,
		kubeBlocksPhaseAborted,
	} {
		if !kubeBlocksOpsFailed(string(phase)) {
			t.Fatalf("phase %q must be retryable failure", phase)
		}
	}

	if kubeBlocksOpsFailed(string(kubeBlocksPhaseSucceeded)) {
		t.Fatal("successful phase classified as failure")
	}
}
