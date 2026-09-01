package kube

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	discoveryfake "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestEnsureNamespaceCreateExistingAndDryRun(t *testing.T) {
	ctx := context.Background()

	client := fake.NewClientset()
	if err := EnsureNamespace(ctx, client, "system", "session", false); err != nil {
		t.Fatal(err)
	}

	namespace, err := client.CoreV1().Namespaces().Get(ctx, "system", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if namespace.Labels[ManagedByLabel] != ManagedByValue ||
		namespace.Labels[SessionKey] != "session" {
		t.Fatalf("namespace labels: %#v", namespace.Labels)
	}

	if err := EnsureNamespace(ctx, client, "system", "other-session", false); err != nil {
		t.Fatalf("existing namespace: %v", err)
	}

	dryRunClient := fake.NewClientset()

	var options metav1.CreateOptions
	dryRunClient.PrependReactor(
		"create",
		"namespaces",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			options = testutil.MustType[interface {
				GetCreateOptions() metav1.CreateOptions
			}](t, action).GetCreateOptions()

			return true, testutil.MustActionObject[runtime.Object](t, action), nil
		},
	)

	if err := EnsureNamespace(ctx, dryRunClient, "dry-run", "", true); err != nil {
		t.Fatal(err)
	}

	if len(options.DryRun) != 1 || options.DryRun[0] != metav1.DryRunAll {
		t.Fatalf("dry-run options: %#v", options.DryRun)
	}

	if err := EnsureNamespace(
		ctx,
		client,
		"",
		"session",
		false,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("empty namespace category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestNewClientsLoadsExplicitKubeconfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")

	config := []byte(`apiVersion: v1
kind: Config
clusters:
- name: local
  cluster:
    server: https://127.0.0.1:6443
    insecure-skip-tls-verify: true
users:
- name: test
  user: {}
contexts:
- name: local
  context:
    cluster: local
    user: test
current-context: local
`)
	if err := os.WriteFile(path, config, 0o600); err != nil {
		t.Fatal(err)
	}

	clients, err := NewClients(path, "local")
	if err != nil {
		t.Fatal(err)
	}

	if clients.Kubernetes == nil || clients.Dynamic == nil || clients.Discovery == nil ||
		clients.RESTConfig == nil {
		t.Fatalf("clients: %#v", clients)
	}

	if clients.RESTConfig.Host != "https://127.0.0.1:6443" ||
		clients.RESTConfig.UserAgent != "pvc-migrate/dev" ||
		clients.RESTConfig.QPS != 30 ||
		clients.RESTConfig.Burst != 60 {
		t.Fatalf("REST config: %#v", clients.RESTConfig)
	}

	if _, err := NewClients(
		filepath.Join(t.TempDir(), "missing"),
		"",
	); domain.CategoryOf(err) != domain.ErrorValidation ||
		!strings.Contains(err.Error(), "no such file") {
		t.Fatalf("missing kubeconfig category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := NewClients(
		path,
		"missing",
	); domain.CategoryOf(err) != domain.ErrorValidation ||
		!strings.Contains(err.Error(), `context "missing" does not exist`) {
		t.Fatalf("missing context category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestHasAPIResource(t *testing.T) {
	if HasAPIResource(nil, "apps/v1", "statefulsets") {
		t.Fatal("nil discovery client reported a resource")
	}

	client := fake.NewClientset()
	discovery := testutil.MustType[*discoveryfake.FakeDiscovery](t, client.Discovery())

	discovery.Resources = []*metav1.APIResourceList{{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{{Name: "statefulsets"}},
	}}
	if !HasAPIResource(discovery, "apps/v1", "statefulsets") {
		t.Fatal("statefulsets resource was not found")
	}

	if HasAPIResource(discovery, "apps/v1", "deployments") {
		t.Fatal("unexpected deployments resource")
	}

	if HasAPIResource(discovery, "missing/v1", "statefulsets") {
		t.Fatal("resource returned for missing group version")
	}
}

func TestAvailableControllerWorkflowKindsSupportsPartialInstall(t *testing.T) {
	client := fake.NewClientset()
	discovery := testutil.MustType[*discoveryfake.FakeDiscovery](t, client.Discovery())
	discovery.Resources = []*metav1.APIResourceList{{
		GroupVersion: domain.SessionAPIVersion,
		APIResources: []metav1.APIResource{
			{Name: domain.MigrationResource},
			{Name: domain.CopyResource},
		},
	}}

	kinds := AvailableControllerWorkflowKinds(discovery)
	if len(kinds) != 2 || kinds[0] != "Migration" || kinds[1] != "Copy" {
		t.Fatalf("available workflow kinds=%v", kinds)
	}
}

func TestObjectStoreProfileAvailableIsIndependentFromWorkflowKinds(t *testing.T) {
	client := fake.NewClientset()
	discovery := testutil.MustType[*discoveryfake.FakeDiscovery](t, client.Discovery())
	discovery.Resources = []*metav1.APIResourceList{{
		GroupVersion: domain.SessionAPIVersion,
		APIResources: []metav1.APIResource{{Name: domain.ObjectStoreProfileResource}},
	}}

	if !ObjectStoreProfileAvailable(discovery) {
		t.Fatal("ObjectStoreProfile resource was not detected")
	}

	discovery.Resources[0].APIResources = nil
	if ObjectStoreProfileAvailable(discovery) {
		t.Fatal("ObjectStoreProfile resource reported after removal")
	}
}

func TestWaitForReadyErrorAndTimeout(t *testing.T) {
	calls := 0
	if err := WaitFor(
		context.Background(),
		time.Millisecond,
		"ready",
		func(context.Context) (bool, error) {
			calls++
			return calls == 2, nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if calls != 2 {
		t.Fatalf("condition calls=%d", calls)
	}

	sentinel := errors.New("condition failed")
	if err := WaitFor(
		context.Background(),
		time.Millisecond,
		"error",
		func(context.Context) (bool, error) {
			return false, sentinel
		},
	); !errors.Is(
		err,
		sentinel,
	) {
		t.Fatalf("condition error=%v", err)
	}

	t.Run("pre-canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		calls = 0

		err := WaitFor(
			ctx,
			time.Millisecond,
			"resource readiness",
			func(context.Context) (bool, error) {
				calls++
				return false, nil
			},
		)
		if domain.CategoryOf(err) != domain.ErrorTimeout || !errors.Is(err, context.Canceled) ||
			!strings.Contains(err.Error(), "canceled while waiting for resource readiness") {
			t.Fatalf("cancellation category=%s error=%v", domain.CategoryOf(err), err)
		}

		if calls != 0 {
			t.Fatalf("pre-canceled condition calls=%d", calls)
		}
	})

	t.Run("canceled after condition", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls = 0

		err := WaitFor(ctx, time.Hour, "resource deletion", func(context.Context) (bool, error) {
			calls++

			cancel()
			return false, nil
		})
		if domain.CategoryOf(err) != domain.ErrorTimeout || !errors.Is(err, context.Canceled) ||
			!strings.Contains(err.Error(), "canceled while waiting for resource deletion") {
			t.Fatalf("cancellation category=%s error=%v", domain.CategoryOf(err), err)
		}

		if calls != 1 {
			t.Fatalf("condition calls=%d", calls)
		}
	})

	t.Run("deadline exceeded", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		calls = 0

		err := WaitFor(
			ctx,
			time.Millisecond,
			"resource readiness",
			func(context.Context) (bool, error) {
				calls++
				return false, nil
			},
		)
		if domain.CategoryOf(err) != domain.ErrorTimeout ||
			!errors.Is(err, context.DeadlineExceeded) ||
			!strings.Contains(err.Error(), "timed out waiting for resource readiness") {
			t.Fatalf("deadline category=%s error=%v", domain.CategoryOf(err), err)
		}

		if calls != 0 {
			t.Fatalf("expired-deadline condition calls=%d", calls)
		}
	})
}

func TestWaitForRejectsInvalidArguments(t *testing.T) {
	if err := WaitFor(
		context.Background(),
		0,
		"ready",
		func(context.Context) (bool, error) { return true, nil },
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("zero interval category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := WaitFor(
		context.Background(),
		time.Millisecond,
		"ready",
		nil,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("nil condition category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestParseGroupVersionResource(t *testing.T) {
	gvr, err := ParseGroupVersionResource("apps/v1", "statefulsets")
	if err != nil {
		t.Fatal(err)
	}

	if gvr.Group != "apps" || gvr.Version != "v1" || gvr.Resource != "statefulsets" {
		t.Fatalf("GVR=%#v", gvr)
	}

	coreGVR, err := ParseGroupVersionResource("v1", "pods")
	if err != nil {
		t.Fatal(err)
	}

	if coreGVR.Group != "" || coreGVR.Version != "v1" || coreGVR.Resource != "pods" {
		t.Fatalf("core GVR=%#v", coreGVR)
	}

	if _, err := ParseGroupVersionResource(
		"too/many/segments",
		"pods",
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("invalid apiVersion category=%s error=%v", domain.CategoryOf(err), err)
	}
}
