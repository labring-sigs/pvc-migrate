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
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestEnsureStandalonePodSnapshotCapturesLivePod(t *testing.T) {
	live := trustedSnapshotPod("tenant-a", "worker", "worker-uid")
	client := kubernetesfake.NewClientset(live)
	store := &runnerSessionStore{}
	session := standaloneSnapshotSession("tenant-a", "worker", "worker-uid", nil)

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
	if captured.Namespace != live.Namespace || captured.Name != live.Name || captured.UID != live.UID {
		t.Fatalf("captured identity=%s/%s uid=%s", captured.Namespace, captured.Name, captured.UID)
	}
	if len(store.updates) != 1 || store.updates[0].Status.OriginalPodSnapshotHash == "" {
		t.Fatalf("snapshot persistence updates=%d", len(store.updates))
	}
}

func TestEnsureStandalonePodSnapshotRejectsInjectedIdentity(t *testing.T) {
	live := trustedSnapshotPod("tenant-a", "worker", "worker-uid")
	foreign := trustedSnapshotPod("tenant-b", "worker", "worker-uid")
	raw, err := json.Marshal(foreign)
	if err != nil {
		t.Fatal(err)
	}
	session := standaloneSnapshotSession("tenant-a", "worker", "worker-uid", raw)
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
	session := standaloneSnapshotSession("tenant-a", "worker", "worker-uid", nil)
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
	live := trustedSnapshotPod("tenant-a", "worker", "worker-uid")
	raw, err := json.Marshal(live)
	if err != nil {
		t.Fatal(err)
	}
	session := standaloneSnapshotSession("tenant-a", "worker", "worker-uid", raw)
	session.Status.OriginalPodSnapshotHash = podSnapshotHash(raw)
	session.Spec.WorkloadPtr().OriginalObject = []byte(`{"metadata":{"name":"worker","namespace":"tenant-a"}}`)
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
	live := trustedSnapshotPod("tenant-a", "worker", "worker-uid")
	live.Annotations = map[string]string{
		"pvc-migrate.example/large": strings.Repeat("x", domain.MaxOriginalPodSnapshotBytes),
	}
	session := standaloneSnapshotSession("tenant-a", "worker", "worker-uid", nil)
	reconciler := &WorkflowReconciler{
		kubeClient: kubernetesfake.NewClientset(live),
		store:      &runnerSessionStore{},
	}

	if err := reconciler.ensureStandalonePodSnapshot(context.Background(), session); err == nil ||
		!strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized live Pod accepted: %v", err)
	}
}

func trustedSnapshotPod(namespace, name string, uid types.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       uid,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "worker", Image: "example.test/worker:latest",
		}}},
	}
}

func standaloneSnapshotSession(
	namespace, name string,
	uid types.UID,
	raw []byte,
) *domain.Session {
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
