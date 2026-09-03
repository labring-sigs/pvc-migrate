package domain_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	. "github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestNewSessionInitializesDeterministicState(t *testing.T) {
	now := time.Date(2026, time.August, 7, 10, 11, 12, 345, time.FixedZone("UTC+8", 8*60*60))
	spec := testSession(t).Spec
	spec.Volumes = append(spec.Volumes, VolumeSpec{SourcePVC: ObjectReference{Name: "logs"}})

	session := NewSession("mig-1", spec, now)
	if session.APIVersion != SessionAPIVersion || session.Kind != SessionKind ||
		session.Generation != 1 {
		t.Fatalf(
			"schema identity = %s %s generation=%d",
			session.APIVersion,
			session.Kind,
			session.Generation,
		)
	}

	if session.Status.Phase != PhasePlanned || session.Status.Message != "" {
		t.Fatalf("initial status = %#v", session.Status)
	}

	wantTime := now.UTC()
	if !session.Status.StartedAt.Time.Equal(wantTime) ||
		!session.Status.UpdatedAt.Time.Equal(wantTime) {
		t.Fatalf(
			"timestamps = started %s updated %s, want %s",
			session.Status.StartedAt.Time,
			session.Status.UpdatedAt.Time,
			wantTime,
		)
	}

	if len(session.Status.History) != 1 || session.Status.History[0].Phase != PhasePlanned ||
		session.Status.History[0].Message != "MigratePod session planned" {
		t.Fatalf("initial history = %#v", session.Status.History)
	}

	if got := []string{
		session.Status.Volumes[0].SourcePVCName,
		session.Status.Volumes[1].SourcePVCName,
	}; strings.Join(
		got,
		",",
	) != "data,logs" {
		t.Fatalf("volume status names = %v", got)
	}
}

func TestEveryDeclaredTransitionIsAccepted(t *testing.T) {
	transitions := map[Phase][]Phase{
		PhasePlanned: {
			PhaseReserving,
			PhaseRenaming,
			PhaseMoving,
			PhaseAborting,
			PhaseFailed,
		},
		PhaseRenaming:    {PhaseCompleted, PhaseAborting, PhaseFailed},
		PhaseMoving:      {PhaseCompleted, PhaseAborting, PhaseFailed},
		PhaseReserving:   {PhaseReserved, PhaseAborting, PhaseFailed},
		PhaseReserved:    {PhaseWarmCopying, PhasePausing, PhaseAborting, PhaseFailed},
		PhaseWarmCopying: {PhaseWarmCopied, PhaseAborting, PhaseFailed},
		PhaseWarmCopied:  {PhaseWarmCopying, PhasePausing, PhaseAborting, PhaseFailed},
		PhasePausing:     {PhasePaused, PhaseAborting, PhaseFailed},
		PhasePaused: {
			PhaseFinalSyncing,
			PhaseResuming,
			PhaseRollingBack,
			PhaseAborting,
			PhaseFailed,
		},
		PhaseFinalSyncing: {PhaseFinalSynced, PhaseAborting, PhaseFailed},
		PhaseFinalSynced: {
			PhaseFinalSyncing,
			PhaseActivating,
			PhaseRollingBack,
			PhaseAborting,
			PhaseFailed,
		},
		PhaseActivating: {PhaseActivated, PhaseRollingBack, PhaseFailed},
		PhaseActivated:  {PhaseResuming, PhaseRollingBack, PhaseFailed},
		PhaseResuming:   {PhaseCompleted, PhaseRolledBack, PhaseFailed},
		PhaseCompleted:  {PhaseRollingBack},
		PhaseAborting:   {PhaseAborted, PhaseFailed},
		PhaseFailed: {
			PhaseReserving,
			PhaseWarmCopying,
			PhasePausing,
			PhaseFinalSyncing,
			PhaseActivating,
			PhaseResuming,
			PhaseRollingBack,
			PhaseAborting,
			PhaseRenaming,
			PhaseMoving,
		},
		PhaseRollingBack: {PhaseRolledBack, PhaseFailed},
	}

	for current, nextPhases := range transitions {
		for _, next := range nextPhases {
			t.Run(string(current)+"_to_"+string(next), func(t *testing.T) {
				session := testSession(t)
				session.Status.Phase = current
				session.Status.History = nil

				now := time.Date(2026, time.August, 7, 1, 2, 3, 0, time.UTC)
				if err := session.Transition(next, "move", now); err != nil {
					t.Fatalf("Transition() error = %v", err)
				}

				if session.Status.Phase != next || session.Status.Message != "move" ||
					!session.Status.UpdatedAt.Time.Equal(now) {
					t.Fatalf("status after transition = %#v", session.Status)
				}

				if len(session.Status.History) != 1 || session.Status.History[0].Phase != next {
					t.Fatalf("history after transition = %#v", session.Status.History)
				}

				terminal := next == PhaseCompleted || next == PhaseAborted ||
					next == PhaseRolledBack
				if terminal != (session.Status.CompletedAt != nil) {
					t.Fatalf(
						"CompletedAt terminal=%t value=%v",
						terminal,
						session.Status.CompletedAt,
					)
				}

				shouldRecordResume := next == PhaseFailed ||
					((next == PhaseAborting || next == PhaseRollingBack) && current != PhaseFailed)
				if shouldRecordResume && session.Status.ResumeFrom != current {
					t.Fatalf("ResumeFrom = %q, want %q", session.Status.ResumeFrom, current)
				}
			})
		}
	}
}

func TestTransitionIdempotencyAndUnknownPhase(t *testing.T) {
	session := testSession(t)
	originalUpdated := session.Status.UpdatedAt

	originalHistory := len(session.Status.History)
	if err := session.Transition(PhasePlanned, "ignored", time.Now()); err != nil {
		t.Fatal(err)
	}

	if !session.Status.UpdatedAt.Equal(&originalUpdated) ||
		len(session.Status.History) != originalHistory {
		t.Fatalf("idempotent transition mutated status: %#v", session.Status)
	}

	session.Status.Phase = Phase("Corrupt")

	err := session.Transition(PhasePlanned, "recover", time.Now())
	if CategoryOf(err) != ErrorConflict || !strings.Contains(err.Error(), "Corrupt") {
		t.Fatalf("unknown phase transition error = %v category=%q", err, CategoryOf(err))
	}
}

func TestTransitionClearsCompletionTimeWhenRollbackStarts(t *testing.T) {
	session := testSession(t)
	session.Status.Phase = PhaseCompleted
	completed := metav1.NewTime(time.Unix(200, 0))

	session.Status.CompletedAt = &completed
	if err := session.Transition(PhaseRollingBack, "rollback", time.Unix(300, 0)); err != nil {
		t.Fatal(err)
	}

	if session.Status.CompletedAt != nil {
		t.Fatalf("rollback retained stale completion time %v", session.Status.CompletedAt)
	}
}

func TestSetConditionAddsAndReplacesByType(t *testing.T) {
	session := testSession(t)
	first := Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Copying"}
	second := Condition{Type: "Healthy", Status: metav1.ConditionTrue}
	replacement := Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Complete"}

	session.SetCondition(first)
	session.SetCondition(second)
	session.SetCondition(replacement)

	if len(session.Status.Conditions) != 2 {
		t.Fatalf("condition count = %d", len(session.Status.Conditions))
	}

	if session.Status.Conditions[0] != replacement || session.Status.Conditions[1] != second {
		t.Fatalf("conditions = %#v", session.Status.Conditions)
	}
}

func TestWorkflowStatusFieldsStayBoundedAndUTF8Safe(t *testing.T) {
	session := testSession(t)

	longMessage := strings.Repeat("状态", MaxWorkflowMessageBytes)
	if err := session.Transition(PhaseReserving, longMessage, time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(session.Status.Message) > MaxWorkflowMessageBytes ||
		!utf8.ValidString(session.Status.Message) {
		t.Fatalf("status message is not bounded UTF-8: bytes=%d", len(session.Status.Message))
	}

	longType := strings.Repeat("T", MaxWorkflowConditionTypeBytes+20)
	longReason := strings.Repeat("R", MaxWorkflowReasonBytes+20)
	session.SetCondition(Condition{Type: longType, Reason: longReason, Message: longMessage})

	condition := session.Status.Conditions[0]
	if len(condition.Type) != MaxWorkflowConditionTypeBytes ||
		len(condition.Reason) != MaxWorkflowReasonBytes ||
		len(condition.Message) > MaxWorkflowMessageBytes ||
		!utf8.ValidString(condition.Message) {
		t.Fatalf("condition fields are not bounded: type=%d reason=%d message=%d",
			len(condition.Type), len(condition.Reason), len(condition.Message))
	}
}

func TestWorkflowHistoryRetainsOnlyRecentEntries(t *testing.T) {
	session := testSession(t)

	session.Status.History = make([]HistoryEntry, MaxWorkflowHistoryEntries+17)
	for i := range session.Status.History {
		session.Status.History[i].Phase = PhasePlanned
		session.Status.History[i].Message = string(rune(i))
	}

	if err := session.Transition(PhaseReserving, "latest", time.Now()); err != nil {
		t.Fatal(err)
	}

	if len(session.Status.History) != MaxWorkflowHistoryEntries {
		t.Fatalf(
			"history length=%d, want %d",
			len(session.Status.History),
			MaxWorkflowHistoryEntries,
		)
	}

	if got := session.Status.History[len(session.Status.History)-1].Message; got != "latest" {
		t.Fatalf("latest history message=%q", got)
	}
}

func TestWorkflowConditionsRetainOnlyRecentTypes(t *testing.T) {
	session := testSession(t)
	for i := range MaxWorkflowConditions + 5 {
		session.SetCondition(Condition{Type: fmt.Sprintf("Condition-%d", i)})
	}

	if len(session.Status.Conditions) != MaxWorkflowConditions {
		t.Fatalf(
			"condition length=%d, want %d",
			len(session.Status.Conditions),
			MaxWorkflowConditions,
		)
	}

	if got := session.Status.Conditions[0].Type; got != "Condition-5" {
		t.Fatalf("oldest retained condition=%q, want Condition-5", got)
	}
}

func TestVolumeStatusMissingIsClassified(t *testing.T) {
	_, err := testSession(t).VolumeStatus("missing")
	if CategoryOf(err) != ErrorInternal || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("VolumeStatus() error = %v category=%q", err, CategoryOf(err))
	}
}

func TestSessionValidateRejectsInvalidPersistentShapes(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Session)
		text   string
	}{
		{
			name:   "api version",
			mutate: func(s *Session) { s.APIVersion = "v2" },
			text:   "unsupported session schema",
		},
		{
			name:   "kind",
			mutate: func(s *Session) { s.Kind = "Other" },
			text:   "unsupported session schema",
		},
		{name: "ID", mutate: func(s *Session) { s.ID = "" }, text: "identity and namespaces"},
		{
			name:   "source namespace",
			mutate: func(s *Session) { s.Spec.SourceNamespace = "" },
			text:   "identity and namespaces",
		},
		{
			name:   "session namespace",
			mutate: func(s *Session) { s.Spec.SessionNamespace = "" },
			text:   "identity and namespaces",
		},
		{
			name:   "Pod migration destination namespace",
			mutate: func(s *Session) { s.Spec.DestinationNamespace = "other" },
			text:   "pod migration must keep workload and PVC identities in the source namespace",
		},
		{
			name:   "volumes",
			mutate: func(s *Session) { s.Spec.Volumes = nil; s.Status.Volumes = nil },
			text:   "contains no volumes",
		},
		{
			name:   "status count",
			mutate: func(s *Session) { s.Status.Volumes = append(s.Status.Volumes, VolumeStatus{}) },
			text:   "counts differ",
		},
		{
			name:   "phase",
			mutate: func(s *Session) { s.Status.Phase = Phase("Unknown") },
			text:   "unsupported session phase",
		},
		{
			name:   "empty source PVC",
			mutate: func(s *Session) { s.Spec.Volumes[0].SourcePVC.Name = "" },
			text:   "source PVC namespace, name, and UID are required",
		},
		{
			name:   "source PVC UID",
			mutate: func(s *Session) { s.Spec.Volumes[0].SourcePVC.UID = "" },
			text:   "source PVC namespace, name, and UID are required",
		},
		{
			name:   "source PV UID",
			mutate: func(s *Session) { s.Spec.Volumes[0].SourcePV.UID = "" },
			text:   "source PV name and UID are required",
		},
		{
			name:   "destination PVC name",
			mutate: func(s *Session) { s.Spec.Volumes[0].DestinationPVC.Name = "" },
			text:   "destination PVC namespace and name are required",
		},
		{
			name:   "destination PV UID",
			mutate: func(s *Session) { s.Spec.Volumes[0].DestinationPV.Name = "pv-destination" },
			text:   "destination PV name and UID must be recorded together",
		},
		{
			name:   "reserved destination identity",
			mutate: func(s *Session) { s.Status.Volumes[0].Reserved = true },
			text:   "reserved destination PVC and PV identities are incomplete",
		},
		{name: "active PVC UID", mutate: func(s *Session) {
			s.Status.Volumes[0].Activation.ActivePVC = ObjectReference{
				Namespace: "app",
				Name:      "data",
			}
		}, text: "active PVC namespace, name, and UID must be recorded together"},
		{
			name:   "status alignment",
			mutate: func(s *Session) { s.Status.Volumes[0].SourcePVCName = "other" },
			text:   "does not match source PVC",
		},
		{name: "duplicate source PVC", mutate: func(s *Session) {
			s.Spec.Volumes = append(s.Spec.Volumes, s.Spec.Volumes[0])
			s.Status.Volumes = append(s.Status.Volumes, s.Status.Volumes[0])
		}, text: "duplicate source PVC"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			session := testSession(t)
			tc.mutate(session)

			err := session.Validate()
			if CategoryOf(err) != ErrorValidation || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("Validate() error = %v category=%q", err, CategoryOf(err))
			}
		})
	}

	if err := testSession(t).Validate(); err != nil {
		t.Fatalf("valid session rejected: %v", err)
	}
}

func TestSessionSerializationRoundTripOmitsStoreVersion(t *testing.T) {
	session := testSession(t)
	session.ResourceVersion = "sensitive-store-version"

	data, err := json.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(data), "sensitive-store-version") ||
		strings.Contains(string(data), "resourceVersion") {
		t.Fatalf("JSON contains store resource version: %s", data)
	}

	var decoded Session
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if err := decoded.Validate(); err != nil {
		t.Fatalf("decoded session invalid: %v", err)
	}

	if decoded.ID != session.ID || decoded.Status.Phase != PhasePlanned ||
		decoded.Spec.Volumes[0].SourcePVC.Name != "data" {
		t.Fatalf("decoded session = %#v", decoded)
	}

	yamlData, err := yaml.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(yamlData), "sensitive-store-version") ||
		strings.Contains(string(yamlData), "resourceVersion") {
		t.Fatalf("YAML contains store resource version: %s", yamlData)
	}
}

func TestSessionRejectsIncompleteWorkloadIdentity(t *testing.T) {
	ref := ObjectReference{Namespace: "app", Name: "workload", UID: "workload-uid"}
	replicas := int32(1)

	for _, test := range []struct {
		name     string
		workload WorkloadSpec
		mutate   func(*WorkloadSpec)
	}{
		{name: "standalone Pod", workload: WorkloadSpec{Adapter: WorkloadStandalone, Pod: ref}, mutate: func(workload *WorkloadSpec) { workload.Pod.UID = "" }},
		{name: "Deployment replicas", workload: WorkloadSpec{Adapter: WorkloadDeployment, Pod: ref, Controller: ref, OriginalReplicas: &replicas, AffectedPods: []ObjectReference{ref}}, mutate: func(workload *WorkloadSpec) { workload.OriginalReplicas = nil }},
		{name: "Deployment affected Pods", workload: WorkloadSpec{Adapter: WorkloadDeployment, Pod: ref, Controller: ref, OriginalReplicas: &replicas, AffectedPods: []ObjectReference{ref}}, mutate: func(workload *WorkloadSpec) { workload.AffectedPods = nil }},
		{name: "Deployment selected Pod", workload: WorkloadSpec{Adapter: WorkloadDeployment, Pod: ref, Controller: ref, OriginalReplicas: &replicas, AffectedPods: []ObjectReference{ref}}, mutate: func(workload *WorkloadSpec) { workload.Pod.UID = "different-uid" }},
		{name: "StatefulSet controller", workload: WorkloadSpec{Adapter: WorkloadStatefulSet, Pod: ref, Controller: ref}, mutate: func(workload *WorkloadSpec) { workload.Controller.UID = "" }},
		{name: "affected Pod", workload: WorkloadSpec{Adapter: WorkloadVictoriaLogs, Pod: ref, Controller: ref, AffectedPods: []ObjectReference{ref}}, mutate: func(workload *WorkloadSpec) { workload.AffectedPods[0].UID = "" }},
		{name: "KubeBlocks Cluster", workload: WorkloadSpec{Adapter: WorkloadKubeBlocks, Pod: ref, Controller: ref, KubeBlocks: &KubeBlocksSpec{Cluster: "cluster", ClusterUID: "cluster-uid"}}, mutate: func(workload *WorkloadSpec) { workload.KubeBlocks.ClusterUID = "" }},
		{name: "VMCluster", workload: WorkloadSpec{Adapter: WorkloadVMCluster, Pod: ref, Controller: ref, VMCluster: &VMClusterSpec{Name: "cluster", UID: "cluster-uid"}}, mutate: func(workload *WorkloadSpec) { workload.VMCluster.UID = "" }},
		{name: "Grafana", workload: WorkloadSpec{Adapter: WorkloadGrafana, Pod: ref, Controller: ref, Grafana: &GrafanaSpec{Name: "grafana", UID: "grafana-uid"}}, mutate: func(workload *WorkloadSpec) { workload.Grafana.UID = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := testSession(t)
			if err := session.Spec.SetWorkload(test.workload); err != nil {
				t.Fatal(err)
			}

			if err := session.Validate(); err != nil {
				t.Fatalf("valid workload rejected: %v", err)
			}

			workload := session.Spec.WorkloadPtr()
			test.mutate(workload)

			if err := session.Validate(); CategoryOf(err) != ErrorValidation {
				t.Fatalf("category=%s error=%v", CategoryOf(err), err)
			}
		})
	}
}

func TestSessionAllowsPartialDeploymentPodSetDuringRecovery(t *testing.T) {
	selected := ObjectReference{Namespace: "app", Name: "web-new-1", UID: "web-new-1-uid"}
	replicas := int32(3)

	session := testSession(t)
	if err := session.Spec.SetWorkload(WorkloadSpec{
		Adapter:          WorkloadDeployment,
		Pod:              selected,
		Controller:       ObjectReference{Namespace: "app", Name: "web", UID: "web-uid"},
		OriginalReplicas: &replicas,
		AffectedPods:     []ObjectReference{selected},
	}); err != nil {
		t.Fatal(err)
	}

	if err := session.Validate(); err != nil {
		t.Fatalf("partial replacement Pod set rejected: %v", err)
	}
}

func TestMigrationPlanReadinessIsMonotonic(t *testing.T) {
	plan := &MigrationPlan{Ready: true}
	plan.AddCheck(Check{Name: "warning", Severity: SeverityWarning, Passed: false})
	plan.AddCheck(Check{Name: "passing error", Severity: SeverityError, Passed: true})

	if !plan.Ready || len(plan.Checks) != 2 {
		t.Fatalf("plan after advisory checks = %#v", plan)
	}

	plan.AddCheck(Check{Name: "quota", Severity: SeverityError, Passed: false})

	if plan.Ready {
		t.Fatal("failed error check left plan ready")
	}

	plan.AddCheck(Check{Name: "later pass", Severity: SeverityError, Passed: true})

	if plan.Ready {
		t.Fatal("later passing check reset failed readiness")
	}
}

func TestNewSessionIDUsesUTCAndDNSCompatibleShape(t *testing.T) {
	now := time.Date(2026, time.August, 7, 23, 4, 5, 0, time.FixedZone("UTC+8", 8*60*60))

	id, err := NewSessionID(now)
	if err != nil {
		t.Fatal(err)
	}

	if matched := regexp.MustCompile(`^mig-20260807-150405-[0-9a-f]{8}$`).
		MatchString(id); !matched {
		t.Fatalf("session ID = %q", id)
	}
}

func TestControllerWorkflowRegistryCoversEveryLocalWorkflow(t *testing.T) {
	workflows := ControllerWorkflows()
	if len(workflows) != 8 {
		t.Fatalf("controller workflow count=%d, want 8", len(workflows))
	}

	seenTypes := make(map[SessionType]struct{}, len(workflows))
	seenKinds := make(map[ControllerKind]struct{}, len(workflows)*2)
	seenResources := make(map[string]struct{}, len(workflows)*2)

	for _, workflow := range workflows {
		if workflow.Type == "" || workflow.Kind == "" && workflow.ClusterKind == "" {
			t.Fatalf("incomplete controller workflow descriptor=%#v", workflow)
		}

		if workflow.Kind == "" != (workflow.Resource == "") ||
			workflow.Kind == "" != (workflow.Singular == "") ||
			workflow.ClusterKind == "" != (workflow.ClusterResource == "") ||
			workflow.ClusterKind == "" != (workflow.ClusterSingular == "") {
			t.Fatalf("inconsistent controller workflow scope descriptor=%#v", workflow)
		}

		if _, exists := seenTypes[workflow.Type]; exists {
			t.Fatalf("duplicate controller workflow type=%q", workflow.Type)
		}

		seenTypes[workflow.Type] = struct{}{}
		for _, kind := range []ControllerKind{workflow.Kind, workflow.ClusterKind} {
			if kind == "" {
				continue
			}

			if _, exists := seenKinds[kind]; exists {
				t.Fatalf("duplicate controller workflow kind=%q", kind)
			}

			seenKinds[kind] = struct{}{}

			byKind, ok := ControllerWorkflowForKind(kind)
			if !ok || byKind.Type != workflow.Type {
				t.Fatalf("kind lookup for %q returned %#v, found=%t", kind, byKind, ok)
			}
		}

		for _, resource := range []string{workflow.Resource, workflow.ClusterResource} {
			if resource == "" {
				continue
			}

			if _, exists := seenResources[resource]; exists {
				t.Fatalf("duplicate controller workflow resource=%q", resource)
			}

			seenResources[resource] = struct{}{}
		}

		byType, ok := ControllerWorkflowForType(workflow.Type)
		if !ok || byType.Kind != workflow.Kind {
			t.Fatalf("type lookup for %q returned %#v, found=%t", workflow.Type, byType, ok)
		}
	}

	resources := ControllerWorkflowResources()
	if len(resources) != 12 {
		t.Fatalf(
			"controller resources=%v, want 7 namespaced and 5 cluster resources",
			resources,
		)
	}

	backup, _ := ControllerWorkflowForType(SessionTypeBackup)
	if backup.ClusterKind != "" {
		t.Fatalf("Backup unexpectedly exposes cluster API: %#v", backup)
	}

	move, _ := ControllerWorkflowForType(SessionTypeMove)
	if move.Kind != "" || move.ClusterKind != ControllerKindMove ||
		move.ClusterResource != MoveResource {
		t.Fatalf("Move scope contract=%#v", move)
	}

	originalKind := workflows[0].Kind
	workflows[0].Kind = ControllerKind("mutated")

	fresh, ok := ControllerWorkflowForType(workflows[0].Type)
	if !ok || fresh.Kind != originalKind {
		t.Fatalf("controller workflow registry was exposed for mutation: %#v, found=%t", fresh, ok)
	}

	resources[0] = "mutated"

	if ControllerWorkflowResources()[0] == "mutated" {
		t.Fatal("controller workflow resource registry was exposed for mutation")
	}
}

func TestControllerResourceScopeUsesOperationNamespaceRoles(t *testing.T) {
	for _, sessionType := range []SessionType{SessionTypeReserve, SessionTypeCopy} {
		spec := SessionSpec{
			SessionCommon: SessionCommon{
				SourceNamespace:      "tenant",
				TemporaryNamespace:   "tenant",
				DestinationNamespace: "unused-final-destination",
				SessionNamespace:     "tenant",
			},
			Type: sessionType,
		}

		resource, ok := ControllerResourceForSpec(spec)
		if !ok || resource.Cluster {
			t.Fatalf("%s resource=%#v found=%t, want namespaced", sessionType, resource, ok)
		}
	}

	spec := SessionSpec{
		SessionCommon: SessionCommon{
			SourceNamespace:      "tenant",
			TemporaryNamespace:   "system",
			DestinationNamespace: "tenant",
			SessionNamespace:     "system",
		},
		Type: SessionTypeMigratePod,
	}

	resource, ok := ControllerResourceForSpec(spec)
	if !ok || !resource.Cluster || resource.Kind != ControllerKindClusterPodMigration {
		t.Fatalf("Pod migration resource=%#v found=%t, want ClusterPodMigration", resource, ok)
	}

	for _, destinationNamespace := range []string{"tenant", "archive"} {
		move := NewSessionSpec(OperationMove, SessionCommon{
			SourceNamespace:      "tenant",
			TemporaryNamespace:   destinationNamespace,
			DestinationNamespace: destinationNamespace,
			SessionNamespace:     "system",
		}, false, SessionWorkflowOptions{})

		resource, ok := ControllerResourceForSpec(move)
		if !ok || !resource.Cluster || resource.Kind != ControllerKindMove ||
			resource.Resource != MoveResource {
			t.Fatalf(
				"Move destination namespace %q resource=%#v found=%t",
				destinationNamespace,
				resource,
				ok,
			)
		}
	}
}

func TestObjectStoreSessionsRejectUnsupportedBackend(t *testing.T) {
	tests := []struct {
		name string
		spec SessionSpec
	}{
		{
			name: "backup",
			spec: NewSessionSpec(OperationBackup, SessionCommon{
				SourceNamespace: "app", SessionNamespace: "system",
			}, false, SessionWorkflowOptions{}),
		},
		{
			name: "restore",
			spec: NewSessionSpec(OperationRestore, SessionCommon{
				SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "system",
			}, false, SessionWorkflowOptions{}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := test.spec
			if spec.Backup != nil {
				spec.Backup.SourcePVC = ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       "pvc-uid",
				}
				spec.Backup.SourcePV = ObjectReference{Name: "pv-data", UID: "pv-uid"}
				spec.Backup.Backend = "gcs"
				spec.Backup.Bucket = "backups"
				spec.Backup.Name = "daily"
			} else {
				spec.Restore.DestinationPVC = ObjectReference{Namespace: "app", Name: "data"}
				spec.Restore.Backend = "gcs"
				spec.Restore.Bucket = "backups"
				spec.Restore.Name = "daily"
			}

			if err := NewSession(
				"backend-test",
				spec,
				time.Now(),
			).Validate(); CategoryOf(
				err,
			) != ErrorValidation {
				t.Fatalf("unsupported backend error=%v category=%q", err, CategoryOf(err))
			}
		})
	}
}

func TestRepositoryBackedSessionsDoNotDeclareADataPlaneBackend(t *testing.T) {
	tests := []struct {
		name string
		spec SessionSpec
	}{
		{
			name: "backup",
			spec: NewSessionSpec(OperationBackup, SessionCommon{
				SourceNamespace: "app", SessionNamespace: "app",
			}, false, SessionWorkflowOptions{}),
		},
		{
			name: "restore",
			spec: NewSessionSpec(OperationRestore, SessionCommon{
				SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "app",
			}, false, SessionWorkflowOptions{}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := test.spec
			if spec.Backup != nil {
				spec.Backup.SourcePVC = ObjectReference{
					Namespace: "app", Name: "data", UID: "pvc-uid",
				}
				spec.Backup.SourcePV = ObjectReference{Name: "pv-data", UID: "pv-uid"}
				spec.Backup.Name = "daily"
				spec.Backup.BackupRepository = "archive"
			} else {
				spec.Restore.DestinationPVC = ObjectReference{Namespace: "app", Name: "data"}
				spec.Restore.Name = "daily"
				spec.Restore.BackupRepository = "archive"
			}

			session := NewSession("repository-test", spec, time.Now())
			if err := session.Validate(); err != nil {
				t.Fatalf("repository-backed session rejected: %v", err)
			}

			if spec.Backup != nil {
				session.Spec.Backup.Backend = BackupBackendS3
			} else {
				session.Spec.Restore.Backend = BackupBackendS3
			}

			if err := session.Validate(); CategoryOf(err) != ErrorValidation {
				t.Fatalf("mixed repository/backend error=%v category=%q", err, CategoryOf(err))
			}
		})
	}
}
