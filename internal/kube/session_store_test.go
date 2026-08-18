package kube

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func storeTestSession() *domain.Session {
	return domain.NewSession("alpha", domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "app",
		SessionNamespace:     "system",
		Volumes: []domain.VolumeSpec{{
			SourcePVC:      domain.ObjectReference{Name: "data", Namespace: "app", UID: "source-pvc-uid"},
			SourcePV:       domain.ObjectReference{Name: "source-pv", UID: "source-pv-uid"},
			SourcePVCSpec:  corev1.PersistentVolumeClaimSpec{},
			DestinationPVC: domain.ObjectReference{Name: "dest", Namespace: "system"},
		}},
	}, domain.WorkloadSpec{Adapter: domain.WorkloadNone}, false, domain.SessionWorkflowOptions{}), time.Unix(100, 0))
}

func TestConfigMapSessionStoreRoundTripAndConflict(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	store := NewConfigMapSessionStore(client)
	session := storeTestSession()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps("system").Get(ctx, SessionConfigMapName("alpha"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(cm.Finalizers) != 1 || cm.Finalizers[0] != SessionFinalizer {
		t.Fatalf("session finalizers=%v", cm.Finalizers)
	}
	cm.Finalizers = append(cm.Finalizers, "example.com/external-protection")
	cm.Annotations = map[string]string{"example.com/audit": "keep"}
	cm.ResourceVersion = "1"
	if _, err := client.CoreV1().ConfigMaps("system").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Get(ctx, "system", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	stale := *loaded
	if err := loaded.Transition(domain.PhaseReserving, "reserving", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, loaded); err != nil {
		t.Fatal(err)
	}
	updatedMetadata, err := client.CoreV1().ConfigMaps("system").Get(ctx, SessionConfigMapName("alpha"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(updatedMetadata.Finalizers, SessionFinalizer) || !containsString(updatedMetadata.Finalizers, "example.com/external-protection") || updatedMetadata.Annotations["example.com/audit"] != "keep" {
		t.Fatalf("session metadata was not preserved: finalizers=%v annotations=%v", updatedMetadata.Finalizers, updatedMetadata.Annotations)
	}
	client.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "configmaps"},
			SessionConfigMapName("alpha"),
			fmt.Errorf("resourceVersion is stale"),
		)
	})
	if err := stale.Transition(domain.PhaseReserving, "stale", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, &stale); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("stale update category = %q, error=%v", domain.CategoryOf(err), err)
	}
	listed, err := store.List(ctx, "system")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Status.Phase != domain.PhaseReserving {
		t.Fatalf("unexpected sessions: %#v", listed)
	}
}

func TestConfigMapSessionStoreDeleteUsesUIDPrecondition(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	store := NewConfigMapSessionStore(client)
	session := storeTestSession()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps("system").Get(ctx, SessionConfigMapName("alpha"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cm.UID = "session-configmap-uid"
	cm.ResourceVersion = "session-configmap-rv"
	if _, err := client.CoreV1().ConfigMaps("system").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	var preconditions *metav1.Preconditions
	client.PrependReactor("delete", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		preconditions = action.(clienttesting.DeleteAction).GetDeleteOptions().Preconditions
		return false, nil, nil
	})
	if err := store.Delete(ctx, session); err != nil {
		t.Fatal(err)
	}
	if preconditions == nil || preconditions.UID == nil || *preconditions.UID != "session-configmap-uid" || preconditions.ResourceVersion == nil || *preconditions.ResourceVersion == "" {
		t.Fatalf("delete preconditions: %#v", preconditions)
	}
	updated, err := client.CoreV1().ConfigMaps("system").Get(ctx, SessionConfigMapName(session.ID), metav1.GetOptions{})
	if err == nil && containsString(updated.Finalizers, SessionFinalizer) {
		t.Fatalf("session finalizer remains before delete: %v", updated.Finalizers)
	}
	_, err = client.CoreV1().ConfigMaps("system").Get(ctx, SessionConfigMapName("alpha"), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("ConfigMap still exists: %v", err)
	}
	if err := store.Delete(ctx, session); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestConfigMapSessionStoreDeleteRejectsChangedSession(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	store := NewConfigMapSessionStore(client)
	session := storeTestSession()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	session.ResourceVersion = "loaded-resource-version"
	current, err := client.CoreV1().ConfigMaps("system").Get(ctx, SessionConfigMapName(session.ID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	current.UID = "replacement-session-uid"
	current.ResourceVersion = "replacement-resource-version"
	if _, err := client.CoreV1().ConfigMaps("system").Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, session); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := client.CoreV1().ConfigMaps("system").Get(ctx, current.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("replacement session ConfigMap was changed: %v", err)
	}
}

func TestConfigMapSessionStoreDeleteMapsFinalizerConflict(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	store := NewConfigMapSessionStore(client)
	session := storeTestSession()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		updated := action.(clienttesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if !containsString(updated.Finalizers, SessionFinalizer) {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, updated.Name, fmt.Errorf("session changed"))
		}
		return false, nil, nil
	})
	if err := store.Delete(ctx, session); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := client.CoreV1().ConfigMaps("system").Get(ctx, SessionConfigMapName(session.ID), metav1.GetOptions{}); err != nil {
		t.Fatalf("session ConfigMap disappeared after conflict: %v", err)
	}
}

func TestConfigMapSessionStoreRejectsOwnershipMetadataMismatch(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	store := NewConfigMapSessionStore(client)
	session := storeTestSession()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps("system").Get(ctx, SessionConfigMapName("alpha"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	delete(cm.Labels, ManagedByLabel)
	if _, err := client.CoreV1().ConfigMaps("system").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "system", "alpha"); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("get category=%s error=%v", domain.CategoryOf(err), err)
	}
	if err := store.Delete(ctx, session); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("delete category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := client.CoreV1().ConfigMaps("system").Get(ctx, SessionConfigMapName("alpha"), metav1.GetOptions{}); err != nil {
		t.Fatalf("ownership-mismatched ConfigMap was deleted: %v", err)
	}
}
