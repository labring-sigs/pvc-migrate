package kube

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func sessionConfigMap(t *testing.T, session *domain.Session, managed bool) *corev1.ConfigMap {
	t.Helper()

	data, err := json.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}

	labels := map[string]string{}
	if managed {
		labels[ManagedByLabel] = ManagedByValue
		labels[SessionKey] = session.ID
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: session.Spec.SessionNamespace,
			Name:      SessionConfigMapName(session.ID),
			Labels:    labels,
		},
		Data: map[string]string{SessionDataKey: string(data)},
	}
}

func TestConfigMapSessionStoreListSortsAndFilters(t *testing.T) {
	older := storeTestSession()
	older.ID = "older"
	older.Status.StartedAt = metav1.NewTime(time.Unix(100, 0))
	newer := storeTestSession()
	newer.ID = "newer"
	newer.Status.StartedAt = metav1.NewTime(time.Unix(200, 0))
	unmanaged := storeTestSession()
	unmanaged.ID = "unmanaged"
	client := fake.NewClientset(
		sessionConfigMap(t, newer, true),
		sessionConfigMap(t, older, true),
		sessionConfigMap(t, unmanaged, false),
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Namespace: "system",
			Name:      "managed-without-session",
			Labels:    map[string]string{ManagedByLabel: ManagedByValue},
		}},
	)

	sessions, err := NewConfigMapSessionStore(client).List(context.Background(), "system")
	if err != nil {
		t.Fatal(err)
	}

	if len(sessions) != 2 || sessions[0].ID != "older" || sessions[1].ID != "newer" {
		t.Fatalf("listed sessions: %#v", sessions)
	}
}

func TestDecodeSessionRejectsMissingInvalidAndUnsupportedData(t *testing.T) {
	tests := []struct {
		name string
		data map[string]string
	}{
		{name: "missing data", data: nil},
		{name: "empty data", data: map[string]string{SessionDataKey: "  "}},
		{name: "invalid JSON", data: map[string]string{SessionDataKey: "{"}},
		{name: "invalid session", data: map[string]string{SessionDataKey: `{}`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeSession(
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "session"},
					Data:       test.data,
				},
			)
			if domain.CategoryOf(err) != domain.ErrorValidation {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDecodeSessionRejectsPreviousAPIVersion(t *testing.T) {
	session := storeTestSession()
	session.APIVersion = "pvc-migrate.io/v1alpha1"

	data, err := json.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}

	_, err = decodeSession(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: session.Spec.SessionNamespace,
			Name:      SessionConfigMapName(session.ID),
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     session.ID,
			},
		},
		Data: map[string]string{SessionDataKey: string(data)},
	})
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("previous API version category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestConfigMapSessionStoreListRejectsUnsupportedSchema(t *testing.T) {
	session := storeTestSession()
	session.APIVersion = "pvc-migrate.io/v1alpha1"
	client := fake.NewClientset(sessionConfigMap(t, session, true))

	_, err := NewConfigMapSessionStore(client).List(context.Background(), "system")
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("unsupported schema list category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestConfigMapSessionStoreCreateGetAndUpdateConflicts(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	store := NewConfigMapSessionStore(client)

	session := storeTestSession()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	if err := store.Create(
		ctx,
		storeTestSession(),
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("duplicate create category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := store.Get(
		ctx,
		"system",
		"missing",
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("missing get category=%s error=%v", domain.CategoryOf(err), err)
	}

	withoutVersion := storeTestSession()
	if err := store.Update(ctx, withoutVersion); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("empty resourceVersion category=%s error=%v", domain.CategoryOf(err), err)
	}

	loaded, err := store.Get(ctx, "system", session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.CoreV1().
		ConfigMaps("system").
		Delete(ctx, SessionConfigMapName(session.ID), metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := store.Update(ctx, loaded); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("disappeared ConfigMap category=%s error=%v", domain.CategoryOf(err), err)
	}
}
