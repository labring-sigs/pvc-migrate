package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Use real manager/informer startup with a delayed API snapshot and a Lease
// held by another replica. Fake clients alone cannot exercise cache readiness.
func TestWorkflowCacheSyncsBeforeStandbyReady(t *testing.T) {
	gate := make(chan struct{})
	requested := make(chan struct{}, 2)

	var release sync.Once

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := "Copy"
		switch r.URL.Path {
		case "/apis/migrate.sealos.io/v1alpha1/copies":
		case "/apis/migrate.sealos.io/v1alpha1/clustercopies":
			kind = "ClusterCopy"
		default:
			t.Errorf("unexpected API request: %s", r.URL)
			http.NotFound(w, r)
			return
		}

		select {
		case requested <- struct{}{}:
		default:
		}

		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}

		serveWorkflowSnapshot(t, w, r, kind)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { release.Do(func() { close(gate) }) })

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{v1alpha1.GroupVersion})
	mapper.Add(v1alpha1.GroupVersion.WithKind("Copy"), meta.RESTScopeNamespace)
	mapper.Add(v1alpha1.GroupVersion.WithKind("ClusterCopy"), meta.RESTScopeRoot)
	mapper.Add(v1alpha1.GroupVersion.WithKind("BackupRepository"), meta.RESTScopeNamespace)

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "system"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: new("other-replica"), LeaseDurationSeconds: new(int32(3600)),
			RenewTime: &metav1.MicroTime{Time: time.Now()},
		},
	}
	leaseClient := clientfake.NewClientset(lease)

	manager, err := ctrl.NewManager(&rest.Config{Host: server.URL}, ctrl.Options{
		Scheme:                 scheme,
		MapperProvider:         func(*rest.Config, *http.Client) (meta.RESTMapper, error) { return mapper, nil },
		Cache:                  workflowCacheOptions(),
		Logger:                 logr.Discard(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         true,
		LeaderElectionResourceLockInterface: &resourcelock.LeaseLock{
			LeaseMeta: lease.ObjectMeta, Client: leaseClient.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: "standby"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	reconciler := NewWorkflowReconciler(nil, nil).WithSupportedKinds([]domain.ControllerKind{
		domain.ControllerKindCopy, domain.ControllerKindClusterCopy,
	})
	if err := reconciler.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}

	readiness := &cacheReadiness{}
	if err := manager.Add(readiness); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)

	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx) }()

	t.Cleanup(func() {
		cancel()

		if err := <-done; err != nil {
			t.Errorf("manager shutdown: %v", err)
		}
	})

	for range 2 {
		select {
		case <-requested:
		case <-ctx.Done():
			t.Fatal("standby did not start both workflow informers")
		}
	}

	if err := readiness.Check(nil); err == nil {
		t.Fatal("standby reported Ready before receiving its initial snapshots")
	}

	release.Do(func() { close(gate) })

	for readiness.Check(nil) != nil {
		select {
		case <-ctx.Done():
			t.Fatal("standby did not become Ready after cache sync")
		case <-time.After(time.Millisecond):
		}
	}

	select {
	case <-manager.Elected():
		t.Fatal("cache readiness required taking leadership")
	default:
	}

	for _, obj := range []crclient.Object{&v1alpha1.Copy{}, &v1alpha1.ClusterCopy{}} {
		key := types.NamespacedName{Name: "workflow"}
		if _, namespaced := obj.(*v1alpha1.Copy); namespaced {
			key.Namespace = "tenant"
		}

		if err := manager.GetClient().Get(ctx, key, obj); err != nil {
			t.Fatal(err)
		}

		if len(obj.GetManagedFields()) != 0 || workflowStatusPhase(obj) != domain.PhaseFailed {
			t.Fatalf("cache transform lost checkpoint or retained managed fields: %+v", obj)
		}

		obj.SetLabels(map[string]string{"mutated": "true"})

		if err := manager.GetClient().Get(ctx, key, obj); err != nil {
			t.Fatal(err)
		}

		if obj.GetLabels()["mutated"] != "" {
			t.Fatal("cache reads share mutable state")
		}
	}

	err = manager.GetClient().
		Get(ctx, types.NamespacedName{Namespace: "tenant", Name: "repository"}, &v1alpha1.BackupRepository{})

	if _, ok := errors.AsType[*cache.ErrResourceNotCached](err); !ok {
		t.Fatalf("unexpected resource silently started an informer: %v", err)
	}
}

func serveWorkflowSnapshot(t *testing.T, w http.ResponseWriter, r *http.Request, kind string) {
	t.Helper()

	metadata := map[string]any{
		"name": "workflow", "resourceVersion": "1", "generation": 1,
		"managedFields": []any{map[string]any{
			"manager": "kubectl", "operation": "Update", "apiVersion": "migrate.sealos.io/v1alpha1",
			"fieldsType": "FieldsV1", "fieldsV1": map[string]any{"f:status": map[string]any{}},
		}},
	}
	if kind == "Copy" {
		metadata["namespace"] = "tenant"
	}

	object := map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(), "kind": kind, "metadata": metadata,
		"status": map[string]any{"phase": "Failed", "resumeFrom": "WarmCopying"},
	}

	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)

	encode := func(value any) {
		if err := encoder.Encode(value); err != nil && r.Context().Err() == nil {
			t.Errorf("encode API response: %v", err)
		}
	}
	if r.URL.Query().Get("watch") != "true" {
		encode(map[string]any{
			"apiVersion": v1alpha1.GroupVersion.String(), "kind": kind + "List",
			"metadata": map[string]any{"resourceVersion": "1"}, "items": []any{object},
		})

		return
	}

	if r.URL.Query().Get("sendInitialEvents") == "true" {
		encode(map[string]any{"type": watch.Added, "object": object})
		encode(map[string]any{"type": watch.Bookmark, "object": map[string]any{
			"apiVersion": v1alpha1.GroupVersion.String(), "kind": kind,
			"metadata": map[string]any{"resourceVersion": "1", "annotations": map[string]string{
				metav1.InitialEventsAnnotationKey: "true",
			}},
		}})
	}

	if err := http.NewResponseController(w).Flush(); err != nil {
		t.Errorf("flush watch: %v", err)
	}

	<-r.Context().Done()
}
