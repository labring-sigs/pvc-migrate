package controller

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestReconcileDeletingWorkflowReleasesOnlyTerminatingNamespace(t *testing.T) {
	tests := []struct {
		name        string
		namespace   *corev1.Namespace
		request     reconcile.Request
		kind        domain.ControllerKind
		wantDeleted bool
	}{
		{
			name:      "active namespace keeps explicit cleanup protection",
			namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
			request: reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: "system",
				Name:      "workflow",
			}},
			kind: domain.ControllerKindCopy,
		},
		{
			name: "terminating namespace releases protection",
			namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
				Name:              "system",
				DeletionTimestamp: func() *metav1.Time { value := metav1.Now(); return &value }(),
			}},
			request: reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: "system",
				Name:      "workflow",
			}},
			kind:        domain.ControllerKindCopy,
			wantDeleted: true,
		},
		{
			name: "missing namespace releases stale protection",
			request: reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: "system",
				Name:      "workflow",
			}},
			kind:        domain.ControllerKindCopy,
			wantDeleted: true,
		},
		{
			name:    "cluster workflow keeps explicit cleanup protection",
			request: reconcile.Request{NamespacedName: types.NamespacedName{Name: "workflow"}},
			kind:    domain.ControllerKindClusterCopy,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &runnerSessionStore{}

			reconciler := NewWorkflowReconciler(nil, store)
			if test.namespace != nil {
				reconciler.WithKubernetesClient(clientfake.NewSimpleClientset(test.namespace))
			} else {
				reconciler.WithKubernetesClient(clientfake.NewSimpleClientset())
			}

			session := newRunnerSession("workflow")
			session.Deleting = true

			session.BackendResource = test.kind
			if err := reconciler.reconcileDeletingWorkflow(
				context.Background(),
				test.request,
				session,
				test.kind,
			); err != nil {
				t.Fatal(err)
			}

			if got := len(store.deleted) == 1; got != test.wantDeleted {
				t.Fatalf("workflow deleted=%t, want %t", got, test.wantDeleted)
			}
		})
	}
}

func TestStartManagerRequiresPinnedToolImage(t *testing.T) {
	for name, image := range map[string]string{
		"missing":    "",
		"whitespace": "   ",
		"digest":     "registry.example/pvc-migrate@sha256:abc",
		"untagged":   "registry.example/pvc-migrate",
	} {
		t.Run(name, func(t *testing.T) {
			err := StartManagerWithKinds(
				context.Background(),
				&rest.Config{},
				nil,
				nil,
				"controller-system",
				nil,
				nil,
				"",
				"",
				nil,
				image,
			)
			if err == nil {
				t.Fatal("manager accepted an untrusted tool image")
			}
		})
	}
}

func TestValidateTrustedToolImageAcceptsOnlyPinnedReferences(t *testing.T) {
	if err := ValidateTrustedToolImage("registry.example/pvc-migrate:v1"); err != nil {
		t.Fatalf("valid image rejected: %v", err)
	}

	for _, image := range []string{"", "   ", "registry.example/pvc-migrate", "registry.example/pvc-migrate@sha256:abc"} {
		if err := ValidateTrustedToolImage(image); err == nil {
			t.Fatalf("image %q accepted", image)
		}
	}
}

func TestRunnerWithKubeconfigTrimsAndStoresConnection(t *testing.T) {
	runner := NewRunner(nil, nil, "system").WithKubeconfig(
		"  /etc/pvc-migrate/kubeconfig  ",
		"  tenant-context  ",
	)

	if runner.kubeconfigPath != "/etc/pvc-migrate/kubeconfig" {
		t.Fatalf("kubeconfig path=%q", runner.kubeconfigPath)
	}

	if runner.kubeContext != "tenant-context" {
		t.Fatalf("kube context=%q", runner.kubeContext)
	}
}

func TestWorkflowSpecMutationError(t *testing.T) {
	for name, session := range map[string]*domain.Session{
		"nil": nil,
		"unobserved": func() *domain.Session {
			s := newRunnerSession("unobserved")
			s.Generation = 2
			return s
		}(),
		"same generation": func() *domain.Session {
			s := newRunnerSession("same")
			s.Generation = 3
			s.Status.ObservedGeneration = 3
			return s
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := workflowSpecMutationError(session); err != nil {
				t.Fatalf("unexpected mutation error: %v", err)
			}
		})
	}

	session := newRunnerSession("changed")
	session.Generation = 4

	session.Status.ObservedGeneration = 3
	if err := workflowSpecMutationError(session); err == nil {
		t.Fatal("spec generation drift was accepted")
	}
}

func TestInitializeUnobservedStatus(t *testing.T) {
	session := newRunnerSession("unobserved-status")
	session.Status.Phase = domain.PhaseCompleted
	session.Status.ObservedGeneration = 0
	store := &runnerSessionStore{latest: session}

	if err := initializeUnobservedStatus(
		context.Background(), store, session,
	); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhasePlanned {
		t.Fatalf("phase=%s, want %s", session.Status.Phase, domain.PhasePlanned)
	}

	if len(store.updates) != 1 {
		t.Fatalf("updates=%d, want one status checkpoint", len(store.updates))
	}
}

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
		t.Fatalf("cluster field=%q", got)
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
