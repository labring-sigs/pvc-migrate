package planner

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type scopedOwnerFinder struct {
	owner *kube.WorkflowOwner
}

func (f scopedOwnerFinder) Find(
	_ context.Context,
	_ string,
	namespaces ...string,
) (*kube.WorkflowOwner, error) {
	scopedOwnerFinderSink = append([]string(nil), namespaces...)

	return f.owner, nil
}

var scopedOwnerFinderSink []string

func ownershipCheckFixture() (*Planner, *domain.PlanSummary, *corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "data",
			Labels:    map[string]string{kube.SessionKey: "owner-session"},
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pv-data",
			Labels: map[string]string{kube.SessionKey: "owner-session"},
		},
	}

	return &Planner{}, &domain.PlanSummary{}, pvc, pv
}

// TestSessionOwnershipSearchesSessionRecordNamespace pins that local CLI
// planning — whose namespaced roles all derive from the workload namespace —
// still resolves owners from the namespace session records persist in.
func TestSessionOwnershipSearchesSessionRecordNamespace(t *testing.T) {
	scopedOwnerFinderSink = nil

	resource, ok := domain.ControllerResourceForKind(domain.ControllerKindMigration)
	if !ok {
		t.Fatal("migration resource is not registered")
	}

	planner, plan, pvc, pv := ownershipCheckFixture()
	planner.sessionRecordNamespace = "pvc-migrate-system"
	planner.workflowOwners = scopedOwnerFinder{owner: &kube.WorkflowOwner{
		ID:               "owner-session",
		SessionNamespace: "pvc-migrate-system",
		Backend:          kube.SessionBackendConfigMap,
		Resource:         resource,
		Phase:            domain.PhaseFinalSyncing,
	}}

	planner.checkSessionOwnership(t.Context(), plan, "app", pvc, pv)

	if !containsNamespace(scopedOwnerFinderSink, "pvc-migrate-system") {
		t.Fatalf("record namespace not searched: %v", scopedOwnerFinderSink)
	}

	checks := plan.Summary().Checks
	if len(checks) != 1 ||
		!strings.Contains(checks[0].Message, "belongs to session owner-session") {
		t.Fatalf("live record not resolved: %v", checks)
	}
}

// TestSessionOwnershipTreatsRemovedKindRecordsAsOrphan keeps upgrade behavior
// honest: a record whose kind this build removed cannot be driven by any
// lifecycle command, so its ownership must route to orphan recovery.
func TestSessionOwnershipTreatsRemovedKindRecordsAsOrphan(t *testing.T) {
	scopedOwnerFinderSink = nil

	planner, plan, pvc, pv := ownershipCheckFixture()
	planner.sessionRecordNamespace = "pvc-migrate-system"
	planner.workflowOwners = scopedOwnerFinder{owner: &kube.WorkflowOwner{
		ID:               "owner-session",
		SessionNamespace: "pvc-migrate-system",
		Backend:          kube.SessionBackendConfigMap,
		Resource:         domain.ControllerResource{},
		Phase:            domain.PhaseCompleted,
	}}

	planner.checkSessionOwnership(t.Context(), plan, "app", pvc, pv)

	checks := plan.Summary().Checks
	if len(checks) != 1 ||
		!strings.Contains(checks[0].Message, "orphan ownership from session owner-session") {
		t.Fatalf("removed-kind record not routed to orphan recovery: %v", checks)
	}

	if strings.Contains(checks[0].Message, "--session-namespace app") {
		t.Fatalf("orphan advice points at the workload namespace: %s", checks[0].Message)
	}
}

func containsNamespace(namespaces []string, want string) bool {
	return slices.Contains(namespaces, want)
}
