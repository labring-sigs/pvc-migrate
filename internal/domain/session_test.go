package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func testSession(t *testing.T) *Session {
	t.Helper()
	return NewSession("test-123", NewSessionSpec(OperationMigrate, SessionCommon{
		SourceNamespace:      "app",
		TemporaryNamespace:   "pvc-migrate-system",
		DestinationNamespace: "app",
		SessionNamespace:     "pvc-migrate-system",
		Volumes: []VolumeSpec{{
			SourcePVC:      ObjectReference{Name: "data", Namespace: "app"},
			SourcePV:       ObjectReference{Name: "pv-source"},
			SourcePVCSpec:  corev1.PersistentVolumeClaimSpec{},
			DestinationPVC: ObjectReference{Name: "target", Namespace: "pvc-migrate-system"},
		}},
	}, WorkloadSpec{Adapter: WorkloadNone}, false), time.Unix(100, 0))
}

func TestSessionSpecUsesConcretePayload(t *testing.T) {
	common := SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app", SessionNamespace: "system",
		Volumes: []VolumeSpec{{SourcePVC: ObjectReference{Namespace: "app", Name: "data"}}},
	}
	spec := NewSessionSpec(OperationCopy, common, WorkloadSpec{Adapter: WorkloadNone}, true)
	if spec.Type != SessionTypeCopy || spec.Copy == nil || !spec.Copy.Online || spec.Migrate != nil || spec.Rename != nil || spec.Move != nil {
		t.Fatalf("copy payload = %#v", spec)
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(raw)
	for _, forbidden := range []string{"\"operation\"", "\"workload\"", "\"migrate\"", "\"rename\"", "\"move\""} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("copy payload contains %s: %s", forbidden, encoded)
		}
	}
	if !strings.Contains(encoded, "\"type\":\"Copy\"") || !strings.Contains(encoded, "\"copy\":{\"deleteExtraneous\":false,\"online\":true}") {
		t.Fatalf("copy payload JSON = %s", encoded)
	}
}

func TestSessionWorkflowOptionsArePersistedInsideConcretePayload(t *testing.T) {
	common := SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app", SessionNamespace: "system",
		Volumes: []VolumeSpec{{SourcePVC: ObjectReference{Namespace: "app", Name: "data"}}},
	}
	spec := NewSessionSpec(OperationMigrate, common, WorkloadSpec{Adapter: WorkloadNone}, false, SessionWorkflowOptions{
		SourceNode: "source-node", TargetNode: "target-node", Strategies: []string{"mount", "clusterip"}, VerifyChecksum: true, DeleteExtraneous: true,
	})
	if got := spec.WorkflowOptions(); got.SourceNode != "source-node" || got.TargetNode != "target-node" || !got.VerifyChecksum || !got.DeleteExtraneous || len(got.Strategies) != 2 || got.Strategies[0] != "mount" || got.Strategies[1] != "clusterip" {
		t.Fatalf("workflow options = %#v", got)
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if _, exists := document["sourceNode"]; exists {
		t.Fatalf("sourceNode leaked into SessionCommon: %s", raw)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(document["migrate"], &payload); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"sourceNode", "targetNode", "strategies", "verifyChecksum", "deleteExtraneous", "workload"} {
		if _, exists := payload[field]; !exists {
			t.Fatalf("migration payload lacks %s: %s", field, raw)
		}
	}
}

func TestSessionWorkflowOptionsCloneStrategies(t *testing.T) {
	common := SessionCommon{Volumes: []VolumeSpec{{SourcePVC: ObjectReference{Name: "data"}}}}
	strategies := []string{"mount", "clusterip"}
	spec := NewSessionSpec(OperationMigrate, common, WorkloadSpec{}, false, SessionWorkflowOptions{Strategies: strategies})
	strategies[0] = "mutated"
	if got := spec.WorkflowOptions().Strategies[0]; got != "mount" {
		t.Fatalf("constructor retained caller slice: %q", got)
	}
	options := spec.WorkflowOptions()
	options.Strategies[1] = "mutated"
	if got := spec.WorkflowOptions().Strategies[1]; got != "clusterip" {
		t.Fatalf("value accessor exposed payload slice: %q", got)
	}
}

func TestSessionRejectsMixedConcretePayloads(t *testing.T) {
	session := testSession(t)
	session.Spec.Copy = &CopySessionSpec{}
	if err := session.Validate(); CategoryOf(err) != ErrorValidation {
		t.Fatalf("mixed payload error=%v category=%q", err, CategoryOf(err))
	}
	session.Spec.Migrate = nil
	session.Spec.Copy = nil
	session.Spec.Type = SessionTypeCopy
	if err := session.Validate(); CategoryOf(err) != ErrorValidation {
		t.Fatalf("missing copy payload error=%v category=%q", err, CategoryOf(err))
	}
}

func TestSessionTransitions(t *testing.T) {
	session := testSession(t)
	for _, phase := range []Phase{
		PhaseReserving,
		PhaseReserved,
		PhaseWarmCopying,
		PhaseWarmCopied,
		PhasePausing,
		PhasePaused,
		PhaseFinalSyncing,
		PhaseFinalSynced,
		PhaseActivating,
		PhaseActivated,
		PhaseResuming,
		PhaseCompleted,
	} {
		if err := session.Transition(phase, string(phase), time.Now()); err != nil {
			t.Fatalf("transition to %s: %v", phase, err)
		}
	}
	if session.Status.CompletedAt == nil {
		t.Fatal("completed session lacks completion time")
	}
}

func TestSessionRejectsUnsafeTransition(t *testing.T) {
	session := testSession(t)
	err := session.Transition(PhaseActivating, "skip safety gates", time.Now())
	if CategoryOf(err) != ErrorConflict {
		t.Fatalf("category = %q, want %q; error=%v", CategoryOf(err), ErrorConflict, err)
	}
}

func TestMoveSessionUsesDedicatedRebindPhase(t *testing.T) {
	session := testSession(t)
	session.Spec = NewSessionSpec(OperationMove, session.Spec.SessionCommon, WorkloadSpec{Adapter: WorkloadNone}, false)
	session.Spec.SourceNamespace = "app"
	session.Spec.DestinationNamespace = "archive"
	if err := session.Transition(PhaseMoving, "move", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := session.Transition(PhaseCompleted, "complete", time.Now()); err != nil {
		t.Fatal(err)
	}
	if !OperationMove.RebindsPVC() || OperationMigrate.RebindsPVC() {
		t.Fatal("PVC rebind operation classification is incorrect")
	}
}

func TestFailedSessionRecordsResumePhase(t *testing.T) {
	session := testSession(t)
	if err := session.Transition(PhaseReserving, "reserve", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := session.Transition(PhaseFailed, "api timeout", time.Now()); err != nil {
		t.Fatal(err)
	}
	if session.Status.ResumeFrom != PhaseReserving {
		t.Fatalf("resume phase = %q, want %q", session.Status.ResumeFrom, PhaseReserving)
	}
	if err := session.Transition(PhaseReserving, "retry", time.Now()); err != nil {
		t.Fatalf("resume: %v", err)
	}
}

func TestVolumeStatusBySourceName(t *testing.T) {
	session := testSession(t)
	status, err := session.VolumeStatus("data")
	if err != nil {
		t.Fatal(err)
	}
	status.Reserved = true
	if !session.Status.Volumes[0].Reserved {
		t.Fatal("returned status is not backed by session state")
	}
}
