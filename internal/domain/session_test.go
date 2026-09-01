package domain_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	. "github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
)

func testSession(t *testing.T) *Session {
	t.Helper()

	return NewSession("test-123", NewPodMigrationSessionSpec(SessionCommon{
		SourceNamespace:      "app",
		TemporaryNamespace:   "pvc-migrate-system",
		DestinationNamespace: "app",
		SessionNamespace:     "pvc-migrate-system",
		Volumes: []VolumeSpec{{
			SourcePVC:      ObjectReference{Name: "data", Namespace: "app", UID: "source-pvc-uid"},
			SourcePV:       ObjectReference{Name: "pv-source", UID: "source-pv-uid"},
			SourcePVCSpec:  corev1.PersistentVolumeClaimSpec{},
			DestinationPVC: ObjectReference{Name: "target", Namespace: "pvc-migrate-system"},
		}},
	}, WorkloadSpec{Adapter: WorkloadStandalone, Pod: ObjectReference{
		Namespace: "app", Name: "database-0", UID: "database-0-uid",
	}}, SessionWorkflowOptions{}, 1, false), time.Unix(100, 0))
}

func TestSessionSpecUsesConcretePayload(t *testing.T) {
	common := SessionCommon{
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "app",
		SessionNamespace:     "system",
		Volumes: []VolumeSpec{
			{SourcePVC: ObjectReference{Namespace: "app", Name: "data"}},
		},
	}

	spec := NewSessionSpec(
		OperationCopy,
		common,

		true,
		SessionWorkflowOptions{},
	)

	if spec.Type != SessionTypeCopy || spec.Copy == nil || !spec.Copy.Online ||
		spec.Migrate != nil ||
		spec.Rename != nil ||
		spec.Move != nil {
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

	if !strings.Contains(encoded, "\"type\":\"Copy\"") ||
		!strings.Contains(encoded, "\"copy\":{\"deleteExtraneous\":false,\"online\":true}") {
		t.Fatalf("copy payload JSON = %s", encoded)
	}
}

func TestSessionConstructorsOwnMutableInputs(t *testing.T) {
	storageClass := "fast"
	replicas := int32(3)
	ordinal := int32(1)
	common := SessionCommon{
		SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "system",
		Volumes: []VolumeSpec{{
			SourcePVCSpec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
			SourcePVCMetadata: PVCMetadata{Labels: map[string]string{"owner": "source"}},
			AccessModes:       []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			TransferScope:     &TransferScope{SourcePath: "/source"},
		}},
	}
	workload := WorkloadSpec{
		Adapter:          WorkloadStatefulSet,
		OriginalReplicas: &replicas,
		Ordinal:          &ordinal,
		AffectedPods:     []ObjectReference{{Name: "db-1"}},
		OriginalObject:   json.RawMessage("invalid but durable"),
	}
	spec := NewPodMigrationSessionSpec(
		common,
		workload,
		SessionWorkflowOptions{Strategies: []string{"mount"}},
		1,
		false,
	)

	storageClass = "slow"
	common.Volumes[0].SourcePVCSpec.AccessModes[0] = corev1.ReadWriteMany
	common.Volumes[0].SourcePVCMetadata.Labels["owner"] = "changed"
	common.Volumes[0].TransferScope.SourcePath = "/changed"
	workload.OriginalReplicas = new(int32(9))
	workload.AffectedPods[0].Name = "changed"
	workload.OriginalObject[0] = 'X'

	if *spec.Volumes[0].SourcePVCSpec.StorageClassName != "fast" ||
		spec.Volumes[0].SourcePVCSpec.AccessModes[0] != corev1.ReadWriteOnce ||
		spec.Volumes[0].SourcePVCMetadata.Labels["owner"] != "source" ||
		spec.Volumes[0].TransferScope.SourcePath != "/source" ||
		*spec.Workload().OriginalReplicas != 3 ||
		spec.Workload().AffectedPods[0].Name != "db-1" ||
		string(spec.Workload().OriginalObject) != "invalid but durable" {
		t.Fatalf("constructor retained mutable input state: %#v", spec)
	}

	clonedSpec := spec.DeepCopy()
	*clonedSpec.WorkloadPtr().OriginalReplicas = 7
	clonedSpec.WorkloadPtr().AffectedPods[0].Name = "copy"
	clonedSpec.WorkloadPtr().OriginalObject[0] = 'Y'

	clonedSpec.Volumes[0].SourcePVCMetadata.Labels["owner"] = "copy"
	if *spec.Workload().OriginalReplicas != 3 || spec.Workload().AffectedPods[0].Name != "db-1" ||
		string(spec.Workload().OriginalObject) != "invalid but durable" ||
		spec.Volumes[0].SourcePVCMetadata.Labels["owner"] != "source" {
		t.Fatal("DeepCopy shared mutable session state")
	}

	session := NewSession("owned", spec, time.Unix(200, 0))

	spec.Volumes[0].SourcePVCMetadata.Labels["owner"] = "after session"
	if session.Spec.Volumes[0].SourcePVCMetadata.Labels["owner"] != "source" {
		t.Fatal("NewSession shared spec ownership with its caller")
	}
}

func TestSessionWorkflowOptionsArePersistedInsideConcretePayload(t *testing.T) {
	common := SessionCommon{
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "app",
		SessionNamespace:     "system",
		Volumes: []VolumeSpec{
			{SourcePVC: ObjectReference{Namespace: "app", Name: "data"}},
		},
	}

	spec := NewPodMigrationSessionSpec(
		common,
		WorkloadSpec{Adapter: WorkloadStandalone, Pod: ObjectReference{
			Namespace: "app", Name: "database-0", UID: "database-0-uid",
		}},
		SessionWorkflowOptions{
			SourceNode:       "source-node",
			TargetNode:       "target-node",
			ToolImage:        "registry.example/pvc-migrate:aio",
			Strategies:       []string{"mount", "clusterip"},
			VerifyChecksum:   true,
			DeleteExtraneous: true,
		},
		2,
		true,
	)
	if got := spec.WorkflowOptions(); got.SourceNode != "source-node" ||
		got.TargetNode != "target-node" ||
		got.ToolImage != "registry.example/pvc-migrate:aio" ||
		!got.VerifyChecksum ||
		!got.DeleteExtraneous ||
		len(got.Strategies) != 2 ||
		got.Strategies[0] != "mount" ||
		got.Strategies[1] != "clusterip" {
		t.Fatalf("workflow options = %#v", got)
	}

	if spec.PrecopyPasses() != 2 {
		t.Fatalf("precopy passes = %d", spec.PrecopyPasses())
	}

	if !spec.OpenEBSLVMSharedMountEnabled() {
		t.Fatal("real-time shared-mount authorization was not persisted")
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
	if err := json.Unmarshal(document["migratePod"], &payload); err != nil {
		t.Fatal(err)
	}

	for _, field := range []string{"sourceNode", "targetNode", "toolImage", "strategies", "verifyChecksum", "deleteExtraneous", "precopyPasses", "openebsLvmEnableShared", "workload"} {
		if _, exists := payload[field]; !exists {
			t.Fatalf("migration payload lacks %s: %s", field, raw)
		}
	}
}

func TestOfflineMigrationPayloadExcludesRealtimeState(t *testing.T) {
	spec := NewOfflineMigrationSessionSpec(
		SessionCommon{SourceNamespace: "app", DestinationNamespace: "app"},
		SessionWorkflowOptions{TargetNode: "node-b"},
	)

	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}

	for _, forbidden := range []string{"workload", "precopyPasses", "openebsLvmEnableShared", "offline"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("offline payload contains %s: %s", forbidden, raw)
		}
	}
}

func TestSessionWorkflowOptionsCloneStrategies(t *testing.T) {
	common := SessionCommon{Volumes: []VolumeSpec{{SourcePVC: ObjectReference{Name: "data"}}}}
	strategies := []string{"mount", "clusterip"}
	spec := NewOfflineMigrationSessionSpec(
		common,
		SessionWorkflowOptions{Strategies: strategies},
	)
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

func TestSessionTracksCompletedWarmPasses(t *testing.T) {
	session := testSession(t)
	if got := session.Status.WarmPassesCompleted; got != 0 {
		t.Fatalf("initial completed warm passes=%d", got)
	}

	session.CompleteWarmPass()

	if got := session.Status.WarmPassesCompleted; got != 1 {
		t.Fatalf("completed warm passes=%d", got)
	}
}

func TestVolumeSpecRequiresConcurrentRWOMount(t *testing.T) {
	tests := []struct {
		name     string
		volume   VolumeSpec
		required bool
	}{
		{
			name: "multiple RWO consumers",
			volume: VolumeSpec{
				ConcurrentConsumers: 2,
				AccessModes:         []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
			required: true,
		},
		{
			name: "single RWO consumer",
			volume: VolumeSpec{
				ConcurrentConsumers: 1,
				AccessModes:         []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
		},
		{
			name: "multiple RWX consumers",
			volume: VolumeSpec{
				ConcurrentConsumers: 3,
				AccessModes:         []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.volume.RequiresConcurrentRWOMount(); got != test.required {
				t.Fatalf("RequiresConcurrentRWOMount()=%t, want %t", got, test.required)
			}
		})
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

func TestSessionPersistsValidTransferScopeAndOmitsFullVolumeDefault(t *testing.T) {
	session := testSession(t)

	raw, err := json.Marshal(session.Spec)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(raw), "transferScope") {
		t.Fatalf("full-volume session contains transfer scope: %s", raw)
	}

	session.Spec.Volumes[0].TransferScope = &TransferScope{
		SourcePath:      "mysql/data",
		DestinationPath: ".",
	}
	if err := session.Validate(); err != nil {
		t.Fatal(err)
	}

	raw, err = json.Marshal(session.Spec)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(
		string(raw),
		`"transferScope":{"sourcePath":"mysql/data","destinationPath":"."}`,
	) {
		t.Fatalf("partial session JSON=%s", raw)
	}

	session.Spec.Volumes[0].TransferScope.SourcePath = "../outside"
	if err := session.Validate(); CategoryOf(err) != ErrorValidation {
		t.Fatalf("unsafe transfer error=%v category=%s", err, CategoryOf(err))
	}
}

func TestKubeBlocksSpecOmitsUnusedSwitchoverStrategy(t *testing.T) {
	raw, err := json.Marshal(KubeBlocksSpec{})
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(raw), "switchoverStrategy") {
		t.Fatalf("KubeBlocks spec contains unused switchover strategy: %s", raw)
	}

	var legacy KubeBlocksSpec
	if err := json.Unmarshal([]byte(`{"switchoverStrategy":"opsrequest"}`), &legacy); err != nil {
		t.Fatal(err)
	}

	if legacy.SwitchoverStrategy != KubeBlocksSwitchoverOpsRequest {
		t.Fatalf("legacy switchover strategy=%q", legacy.SwitchoverStrategy)
	}
}

func TestSessionRejectsTransferScopeForIdentityOnlyOperations(t *testing.T) {
	session := testSession(t)
	session.Spec = NewSessionSpec(
		OperationRename,
		session.Spec.SessionCommon,

		false,
		SessionWorkflowOptions{},
	)

	session.Spec.Volumes[0].TransferScope = &TransferScope{SourcePath: "data", DestinationPath: "."}
	if err := session.Validate(); CategoryOf(err) != ErrorValidation {
		t.Fatalf("rename transfer error=%v category=%s", err, CategoryOf(err))
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
	session.Spec = NewSessionSpec(
		OperationMove,
		session.Spec.SessionCommon,

		false,
		SessionWorkflowOptions{},
	)

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

	for _, operation := range []Operation{OperationMigrate, OperationMigratePod, OperationRename, OperationMove} {
		if !operation.RecreatesPVC() {
			t.Fatalf("operation %s must recreate PVC identity", operation)
		}
	}

	for _, operation := range []Operation{OperationReserve, OperationCopy, OperationBackup, OperationRestore} {
		if operation.RecreatesPVC() {
			t.Fatalf("operation %s must preserve PVC identity", operation)
		}
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

	session.Status.FailureReason = FailureDestinationCapacityExhausted
	if err := session.Transition(PhaseReserving, "retry", time.Now()); err != nil {
		t.Fatalf("resume: %v", err)
	}

	if session.Status.FailureReason != "" {
		t.Fatalf("failure reason was not cleared: %q", session.Status.FailureReason)
	}
}

func TestFailedSessionReactivatesOnlyThroughExplicitResume(t *testing.T) {
	session := testSession(t)
	if err := session.Transition(PhaseReserving, "reserve", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(PhaseFailed, "api timeout", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := session.Reactivate("resume requested", time.Unix(123, 0)); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != PhaseReserving || session.Status.ResumeFrom != PhaseReserving ||
		session.Status.FailureReason != "" || session.Status.CompletedAt != nil {
		t.Fatalf("reactivated status=%#v", session.Status)
	}

	if got := session.Status.History[len(session.Status.History)-1].Message; got != "resume requested" {
		t.Fatalf("last history message=%q", got)
	}
}

func TestSessionRejectsInvalidFailureReasonState(t *testing.T) {
	for _, test := range []struct {
		name   string
		phase  Phase
		reason SessionFailureReason
	}{
		{name: "unknown reason", phase: PhaseFailed, reason: "Unknown"},
		{name: "reason outside failure", phase: PhasePlanned, reason: FailureDestinationCapacityExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := testSession(t)
			session.Status.Phase = test.phase

			session.Status.FailureReason = test.reason
			if err := session.Validate(); CategoryOf(err) != ErrorValidation {
				t.Fatalf("error=%v category=%s", err, CategoryOf(err))
			}
		})
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
