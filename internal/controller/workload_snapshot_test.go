package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestEnsureStandalonePodSnapshotCapturesLivePod(t *testing.T) {
	live := trustedSnapshotPod("tenant-a")
	client := kubernetesfake.NewClientset(live)
	store := &runnerSessionStore{}
	session := standaloneSnapshotSession(nil)

	reconciler := &WorkflowReconciler{kubeClient: client, store: store}
	if err := reconciler.ensureStandalonePodSnapshot(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.OriginalPodSnapshotHash == "" {
		t.Fatal("controller did not persist a snapshot hash")
	}

	var captured corev1.Pod
	if err := json.Unmarshal(session.Spec.Workload().OriginalObject, &captured); err != nil {
		t.Fatal(err)
	}

	if captured.Namespace != live.Namespace || captured.Name != live.Name ||
		captured.UID != live.UID {
		t.Fatalf("captured identity=%s/%s uid=%s", captured.Namespace, captured.Name, captured.UID)
	}

	if len(store.updates) != 1 || store.updates[0].Status.OriginalPodSnapshotHash == "" {
		t.Fatalf("snapshot persistence updates=%d", len(store.updates))
	}
}

func TestPodSnapshotHashStableAfterAPIRoundTrip(t *testing.T) {
	live := trustedSnapshotPod("tenant-a")
	raw, err := json.Marshal(live)
	if err != nil {
		t.Fatal(err)
	}

	unstructuredPod, err := runtime.DefaultUnstructuredConverter.ToUnstructured(live)
	if err != nil {
		t.Fatal(err)
	}
	var roundTripped corev1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		unstructuredPod,
		&roundTripped,
	); err != nil {
		t.Fatal(err)
	}
	roundTrippedRaw, err := json.Marshal(&roundTripped)
	if err != nil {
		t.Fatal(err)
	}

	if podSnapshotHash(raw) != podSnapshotHash(roundTrippedRaw) {
		t.Fatal("Pod snapshot hash changed after API object round-trip")
	}
}

func TestEnsureStandalonePodSnapshotUsesSourceNamespaceForClusterWorkflow(t *testing.T) {
	live := trustedSnapshotPod("tenant-a")
	session := standaloneSnapshotSession(nil)
	session.BackendResource = domain.ControllerKindClusterPodMigration
	session.Spec.SessionNamespace = "control"
	session.Spec.SourceNamespace = "tenant-a"
	session.Spec.WorkloadPtr().Pod.Namespace = "tenant-a"

	reconciler := &WorkflowReconciler{
		kubeClient: kubernetesfake.NewClientset(live),
		store:      &runnerSessionStore{},
	}
	if err := reconciler.ensureStandalonePodSnapshot(context.Background(), session); err != nil {
		t.Fatalf("valid cluster standalone Pod rejected: %v", err)
	}
}

func TestEnsureStandalonePodSnapshotRejectsInjectedIdentity(t *testing.T) {
	live := trustedSnapshotPod("tenant-a")
	foreign := trustedSnapshotPod("tenant-b")

	raw, err := json.Marshal(foreign)
	if err != nil {
		t.Fatal(err)
	}

	session := standaloneSnapshotSession(raw)
	reconciler := &WorkflowReconciler{
		kubeClient: kubernetesfake.NewClientset(live),
		store:      &runnerSessionStore{},
	}

	if err := reconciler.ensureStandalonePodSnapshot(context.Background(), session); err == nil ||
		!strings.Contains(err.Error(), "identity does not match") {
		t.Fatalf("injected snapshot accepted: %v", err)
	}
}

func TestEnsureStandalonePodSnapshotRequiresCaptureBeforePodDisappears(t *testing.T) {
	session := standaloneSnapshotSession(nil)
	reconciler := &WorkflowReconciler{
		kubeClient: kubernetesfake.NewClientset(),
		store:      &runnerSessionStore{},
	}

	if err := reconciler.ensureStandalonePodSnapshot(context.Background(), session); err == nil ||
		!strings.Contains(err.Error(), "missing before its snapshot was captured") {
		t.Fatalf("missing Pod accepted: %v", err)
	}
}

func TestEnsureStandalonePodSnapshotRejectsDigestDrift(t *testing.T) {
	live := trustedSnapshotPod("tenant-a")

	raw, err := json.Marshal(live)
	if err != nil {
		t.Fatal(err)
	}

	session := standaloneSnapshotSession(raw)
	session.Status.OriginalPodSnapshotHash = podSnapshotHash(raw)
	session.Spec.WorkloadPtr().OriginalObject = []byte(
		`{"metadata":{"name":"worker","namespace":"tenant-a"}}`,
	)
	reconciler := &WorkflowReconciler{
		kubeClient: kubernetesfake.NewClientset(live),
		store:      &runnerSessionStore{},
	}

	if err := reconciler.ensureStandalonePodSnapshot(context.Background(), session); err == nil ||
		!strings.Contains(err.Error(), "does not match the controller-captured digest") {
		t.Fatalf("digest drift accepted: %v", err)
	}
}

func TestEnsureStandalonePodSnapshotRejectsOversizedLivePod(t *testing.T) {
	live := trustedSnapshotPod("tenant-a")
	live.Annotations = map[string]string{
		"pvc-migrate.example/large": strings.Repeat("x", domain.MaxOriginalPodSnapshotBytes),
	}
	session := standaloneSnapshotSession(nil)
	reconciler := &WorkflowReconciler{
		kubeClient: kubernetesfake.NewClientset(live),
		store:      &runnerSessionStore{},
	}

	if err := reconciler.ensureStandalonePodSnapshot(context.Background(), session); err == nil ||
		!strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized live Pod accepted: %v", err)
	}
}

func trustedSnapshotPod(namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "worker",
			UID:       "worker-uid",
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "worker", Image: "example.test/worker:latest",
		}}},
	}
}

func standaloneSnapshotSession(raw []byte) *domain.Session {
	const (
		namespace = "tenant-a"
		name      = "worker"
	)

	uid := types.UID("worker-uid")

	return domain.NewSession(
		"snapshot-session",
		domain.NewPodMigrationSessionSpec(
			domain.SessionCommon{
				SourceNamespace:  namespace,
				SessionNamespace: namespace,
			},
			domain.WorkloadSpec{
				Adapter: domain.WorkloadStandalone,
				Pod: domain.ObjectReference{
					APIVersion: "v1", Kind: "Pod", Namespace: namespace,
					Name: name, UID: uid,
				},
				OriginalObject: raw,
			},
			domain.SessionWorkflowOptions{},
			0,
			false,
		),
		time.Now(),
	)
}
