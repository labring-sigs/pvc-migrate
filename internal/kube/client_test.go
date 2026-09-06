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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestRequireNamespaceNeverCreatesOrModifiesNamespaces(t *testing.T) {
	for _, name := range []string{"existing", "missing", "forbidden", ""} {
		t.Run(name, func(t *testing.T) {
			client := fake.NewClientset(
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "existing"}},
			)
			client.PrependReactor(
				"get",
				"namespaces",
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					if name == "forbidden" {
						return true, nil, apierrors.NewForbidden(
							schema.GroupResource{Resource: "namespaces"},
							name,
							nil,
						)
					}

					return false, nil, nil
				},
			)

			err := RequireNamespace(context.Background(), client, name)
			switch name {
			case "existing":
				if err != nil {
					t.Fatal(err)
				}
			case "missing":
				if domain.CategoryOf(err) != domain.ErrorPrecondition ||
					!strings.Contains(err.Error(), "namespace missing does not exist") {
					t.Fatalf("missing namespace: %v", err)
				}
			case "forbidden":
				if domain.CategoryOf(err) != domain.ErrorKubernetes || !apierrors.IsForbidden(err) {
					t.Fatalf("forbidden namespace: %v", err)
				}
			case "":
				if domain.CategoryOf(err) != domain.ErrorValidation {
					t.Fatalf("empty namespace: %v", err)
				}
			}

			for _, action := range client.Actions() {
				if action.GetVerb() != "get" || action.GetResource().Resource != "namespaces" {
					t.Fatalf("namespace check performed unexpected action: %#v", action)
				}
			}
		})
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
			{Name: domain.MigrationResource + "/status"},
			{Name: domain.CopyResource},
			{Name: domain.CopyResource + "/status"},
		},
	}}

	kinds := AvailableControllerWorkflowKinds(discovery)
	if len(kinds) != 2 || kinds[0] != "Migration" || kinds[1] != "Copy" {
		t.Fatalf("available workflow kinds=%v", kinds)
	}
}

func TestAvailableControllerWorkflowKindsRequiresStatusSubresource(t *testing.T) {
	client := fake.NewClientset()
	discovery := testutil.MustType[*discoveryfake.FakeDiscovery](t, client.Discovery())
	discovery.Resources = []*metav1.APIResourceList{{
		GroupVersion: domain.SessionAPIVersion,
		APIResources: []metav1.APIResource{{Name: domain.MigrationResource}},
	}}

	if kinds := AvailableControllerWorkflowKinds(discovery); len(kinds) != 0 {
		t.Fatalf("workflow without status subresource reported as available: %v", kinds)
	}
}

func TestBackupRepositoryAvailableIsIndependentFromWorkflowKinds(t *testing.T) {
	client := fake.NewClientset()
	discovery := testutil.MustType[*discoveryfake.FakeDiscovery](t, client.Discovery())
	discovery.Resources = []*metav1.APIResourceList{{
		GroupVersion: domain.SessionAPIVersion,
		APIResources: []metav1.APIResource{{Name: domain.BackupRepositoryResource}},
	}}

	if !BackupRepositoryAvailable(discovery) {
		t.Fatal("BackupRepository resource was not detected")
	}

	discovery.Resources[0].APIResources = nil
	if BackupRepositoryAvailable(discovery) {
		t.Fatal("BackupRepository resource reported after removal")
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
