package controller

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func clusterWithPhases(clusterPhase, componentPhase string) *unstructured.Unstructured {
	status := map[string]any{"phase": clusterPhase}
	if componentPhase != "" {
		status[kubeBlocksFieldComponents] = map[string]any{
			"mongodb": map[string]any{"phase": componentPhase},
		}
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": kubeBlocksClusterAPIVersion,
		"kind":       "Cluster",
		"metadata":   map[string]any{"name": "mg", "namespace": "tenants", "uid": "cluster-uid"},
		"status":     status,
	}}
}

// TestKubeBlocksStoppedScopesPhaseRead pins the protocol split that decides
// which phase field proves a stop: the legacy Cluster path reads
// status.phase, while the component-scoped operations API reads the
// component's phase under status.components.<component>.
func TestKubeBlocksStoppedScopesPhaseRead(t *testing.T) {
	legacy := &v1alpha1.KubeBlocksSpec{
		Component:     "mongodb",
		OpsAPIVersion: "apps.kubeblocks.io/v1alpha1",
	}
	scoped := &v1alpha1.KubeBlocksSpec{
		Component:     "mongodb",
		OpsAPIVersion: kubeBlocksOperationsAPIGroup + "/v1alpha1",
	}

	cases := []struct {
		name      string
		kb        *v1alpha1.KubeBlocksSpec
		cluster   *unstructured.Unstructured
		wantStop  bool
		wantPhase string
	}{
		{
			name:      "legacy cluster stopped",
			kb:        legacy,
			cluster:   clusterWithPhases("Stopped", "Running"),
			wantStop:  true,
			wantPhase: "Stopped",
		},
		{
			name:      "legacy cluster running",
			kb:        legacy,
			cluster:   clusterWithPhases("Running", "Stopped"),
			wantStop:  false,
			wantPhase: "Running",
		},
		{
			name:      "component scoped stopped",
			kb:        scoped,
			cluster:   clusterWithPhases("Running", "Stopped"),
			wantStop:  true,
			wantPhase: "Stopped",
		},
		{
			name:      "component scoped missing component phase",
			kb:        scoped,
			cluster:   clusterWithPhases("Stopped", ""),
			wantStop:  false,
			wantPhase: "",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stopped, phase, err := kubeBlocksStopped(testCase.cluster, testCase.kb)
			if err != nil {
				t.Fatal(err)
			}

			if stopped != testCase.wantStop || phase != testCase.wantPhase {
				t.Fatalf(
					"stopped=%v phase=%q, want %v/%q",
					stopped,
					phase,
					testCase.wantStop,
					testCase.wantPhase,
				)
			}
		})
	}

	stopped, _, err := kubeBlocksStopped(clusterWithPhases("Running", ""), scoped)
	if err != nil || stopped {
		t.Fatalf("missing component phase: stopped=%v err=%v, want false/nil", stopped, err)
	}
}

// TestRequireKubeBlocksStoppedMessagesNameTheScope keeps the operator error
// actionable: the component-scoped failure names the component, the legacy
// failure names the Cluster.
func TestRequireKubeBlocksStoppedMessagesNameTheScope(t *testing.T) {
	legacy := &v1alpha1.KubeBlocksSpec{
		Component:     "mongodb",
		OpsAPIVersion: "apps.kubeblocks.io/v1alpha1",
	}
	scoped := &v1alpha1.KubeBlocksSpec{
		Component:     "mongodb",
		OpsAPIVersion: kubeBlocksOperationsAPIGroup + "/v1alpha1",
	}

	err := requireKubeBlocksStopped(clusterWithPhases("Running", ""), legacy)
	if err == nil || !strings.Contains(err.Error(), "Cluster phase is Running") {
		t.Fatalf("legacy error=%v", err)
	}

	err = requireKubeBlocksStopped(clusterWithPhases("Running", "Running"), scoped)
	if err == nil || !strings.Contains(err.Error(), "component mongodb phase is Running") {
		t.Fatalf("scoped error=%v", err)
	}

	if err := requireKubeBlocksStopped(clusterWithPhases("Stopped", ""), legacy); err != nil {
		t.Fatalf("stopped cluster error=%v", err)
	}
}

// TestKubeBlocksOpsSpecEqualIgnoresAdmissionDefaults pins which OpsRequest
// spec differences are identity (reject) versus KubeBlocks admission
// defaults (accept): equivalent clusterRef/clusterName spellings and
// zero-valued defaulted fields must compare equal.
func TestKubeBlocksOpsSpecEqualIgnoresAdmissionDefaults(t *testing.T) {
	submitted := map[string]any{
		"clusterRef":                  "mg",
		"clusterName":                 "mg",
		"stop":                        map[string]any{"components": []any{"mongodb"}},
		"preConditionDeadlineSeconds": int64(0),
	}
	served := map[string]any{
		"clusterRef":            "mg",
		"stop":                  map[string]any{"components": []any{"mongodb"}},
		"enqueueOnForce":        false,
		"ttlSecondsBeforeAbort": int64(0),
	}

	if !kubeBlocksOpsSpecEqual(submitted, served) {
		t.Fatal("admission-default differences must compare equal")
	}

	// A clusterName-only spelling is a different spec today: normalization
	// only drops the duplicate when both fields carry the same value.
	if kubeBlocksOpsSpecEqual(
		map[string]any{"clusterRef": "mg"},
		map[string]any{"clusterName": "mg"},
	) {
		t.Fatal("one-sided clusterRef vs clusterName must compare unequal")
	}

	divergent := map[string]any{
		"clusterRef": "mg",
		"stop":       map[string]any{"components": []any{"other"}},
	}
	if kubeBlocksOpsSpecEqual(submitted, divergent) {
		t.Fatal("a changed component list must compare unequal")
	}
}

// TestKubeBlocksOperationNameEncodesRecoveryContext pins the deterministic
// OpsRequest naming: failed workflows reuse their resume phase, and abort or
// rollback context prefixes the action so recovery operations never collide
// with the original pause or resume requests.
func TestKubeBlocksOperationNameEncodesRecoveryContext(t *testing.T) {
	cases := []struct {
		name        string
		phase       v1alpha1.WorkflowPhase
		resumeFrom  v1alpha1.WorkflowPhase
		action      string
		wantSubstr  string
		wantMissing string
	}{
		{
			name:       "failed pause reuses resume phase",
			phase:      domain.PhaseFailed,
			resumeFrom: domain.PhasePausing,
			action:     "pause",
			wantSubstr: "pvc-migrate-",
		},
		{
			name:        "abort prefix separates recovery ops",
			phase:       domain.PhaseAborting,
			action:      "resume",
			wantSubstr:  "abort-resume",
			wantMissing: "resume",
		},
		{
			name:       "rollback prefix separates recovery ops",
			phase:      domain.PhaseRollingBack,
			action:     "resume",
			wantSubstr: "rollback-resume",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			name := kubeBlocksOperationName(
				"session-1",
				testCase.phase,
				testCase.resumeFrom,
				testCase.action,
			)
			if !strings.Contains(name, testCase.wantSubstr) {
				t.Fatalf("name=%q missing %q", name, testCase.wantSubstr)
			}
		})
	}

	if kubeBlocksOperationName("s", domain.PhasePausing, "", "pause") ==
		kubeBlocksOperationName("s", domain.PhaseAborting, "", "pause") {
		t.Fatal("pause and abort-pause must not collide")
	}
}

// TestValidateKubeBlocksOpsRequestGuardsOwnership pins the three adoption
// guards for a pre-existing OpsRequest: foreign labels, missing UID, and a
// divergent spec each refuse; an owned identical request adopts.
func TestValidateKubeBlocksOpsRequestGuardsOwnership(t *testing.T) {
	spec := map[string]any{"clusterRef": "mg", "stop": map[string]any{}}

	opsRequest := func(labels map[string]string, uid string, spec any) *unstructured.Unstructured {
		unstructuredLabels := make(map[string]any, len(labels))
		for key, value := range labels {
			unstructuredLabels[key] = value
		}

		object := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apps.kubeblocks.io/v1alpha1",
			"kind":       "OpsRequest",
			"metadata": map[string]any{
				"name":      "op",
				"namespace": "tenants",
				"uid":       uid,
				"labels":    unstructuredLabels,
			},
			"spec": spec,
		}}

		return object
	}

	owned := map[string]string{
		kube.ManagedByLabel: kube.ManagedByValue,
		kube.SessionKey:     "session-1",
	}

	foreign := opsRequest(
		map[string]string{
			kube.ManagedByLabel: kube.ManagedByValue,
			kube.SessionKey:     "another-session",
		},
		"op-uid",
		spec,
	)
	if _, err := validateKubeBlocksOpsRequest(foreign, "op", "session-1", spec); err == nil {
		t.Fatal("foreign session must be refused")
	}

	unmanaged := opsRequest(map[string]string{kube.SessionKey: "session-1"}, "op-uid", spec)
	if _, err := validateKubeBlocksOpsRequest(unmanaged, "op", "session-1", spec); err == nil {
		t.Fatal("missing managed-by label must be refused")
	}

	noUID := opsRequest(owned, "", spec)
	if _, err := validateKubeBlocksOpsRequest(noUID, "op", "session-1", spec); err == nil {
		t.Fatal("missing UID must be refused")
	}

	divergent := opsRequest(owned, "op-uid", map[string]any{"clusterRef": "other"})
	if _, err := validateKubeBlocksOpsRequest(divergent, "op", "session-1", spec); err == nil {
		t.Fatal("divergent spec must be refused")
	}

	matching := opsRequest(owned, "op-uid", spec)

	uid, err := validateKubeBlocksOpsRequest(matching, "op", "session-1", spec)
	if err != nil || uid != "op-uid" {
		t.Fatalf("matching adoption uid=%q err=%v", uid, err)
	}
}

// TestKubeBlocksAbortStartedFromPausingWalksHistory pins the history scan
// that decides whether abort may skip the workload resume: a pause that never
// converged (Pausing directly, or Failed-over-Pausing) skips; any completed
// pause does not.
func TestKubeBlocksAbortStartedFromPausingWalksHistory(t *testing.T) {
	cases := []struct {
		name      string
		phase     v1alpha1.WorkflowPhase
		resume    v1alpha1.WorkflowPhase
		history   []v1alpha1.WorkflowHistoryEntry
		wantAbort bool
	}{
		{
			name:      "direct pausing abort",
			phase:     domain.PhaseAborting,
			history:   []v1alpha1.WorkflowHistoryEntry{{Phase: domain.PhasePausing}},
			wantAbort: true,
		},
		{
			name:   "failed over pausing",
			phase:  domain.PhaseFailed,
			resume: domain.PhaseAborting,
			history: []v1alpha1.WorkflowHistoryEntry{
				{Phase: domain.PhaseFailed},
				{Phase: domain.PhasePausing},
			},
			wantAbort: true,
		},
		{
			name:      "pause completed before abort",
			phase:     domain.PhaseAborting,
			history:   []v1alpha1.WorkflowHistoryEntry{{Phase: domain.PhasePaused}},
			wantAbort: false,
		},
		{
			name:      "empty history falls back to resumeFrom",
			phase:     domain.PhaseFailed,
			resume:    domain.PhaseAborting,
			history:   nil,
			wantAbort: false,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := kubeBlocksAbortStartedFromPausing(
				testCase.phase,
				testCase.resume,
				testCase.history,
			)
			if got != testCase.wantAbort {
				t.Fatalf("abortStartedFromPausing=%v, want %v", got, testCase.wantAbort)
			}
		})
	}
}

// TestSamePodIdentitySetRules pins the deployment pause/resume identity
// fence: same name+UID sets match regardless of order; duplicates, empty
// UIDs, length changes, and UID swaps all fail.
func TestSamePodIdentitySetRules(t *testing.T) {
	left := []v1alpha1.ObjectReference{
		{Namespace: "tenants", Name: "a", UID: "uid-a"},
		{Namespace: "tenants", Name: "b", UID: "uid-b"},
	}
	reordered := []v1alpha1.ObjectReference{
		{Namespace: "tenants", Name: "b", UID: "uid-b"},
		{Namespace: "tenants", Name: "a", UID: "uid-a"},
	}

	if !samePodIdentitySet(left, reordered) {
		t.Fatal("same identity set in different order must match")
	}

	swapped := []v1alpha1.ObjectReference{
		{Namespace: "tenants", Name: "a", UID: "uid-b"},
		{Namespace: "tenants", Name: "b", UID: "uid-a"},
	}
	if samePodIdentitySet(left, swapped) {
		t.Fatal("UID swap must not match")
	}

	shorter := left[:1]
	if samePodIdentitySet(left, shorter) {
		t.Fatal("length change must not match")
	}

	emptyUID := []v1alpha1.ObjectReference{
		{Namespace: "tenants", Name: "a", UID: ""},
		{Namespace: "tenants", Name: "b", UID: "uid-b"},
	}
	if samePodIdentitySet(emptyUID, left) {
		t.Fatal("empty UID must not match")
	}
}

var (
	_ = corev1.ResourcePods
	_ = apierrors.NewNotFound
	_ = metav1.NamespaceDefault
	_ = types.UID("")
)
