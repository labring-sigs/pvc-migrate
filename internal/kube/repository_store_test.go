package kube

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func repositoryStoreFixture() (*fake.Clientset, *v1alpha1.BackupRepository, v1alpha1.ObjectReference) {
	owner := v1alpha1.ObjectReference{Namespace: "state", Name: "backup", UID: "workflow"}
	client := fake.NewClientset(
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: owner.Namespace,
				Name:      SessionConfigMapName(owner.Name),
				UID:       owner.UID,
				Labels:    sessionLabels(owner.Name),
			},
		},
	)
	client.PrependReactor(
		"create",
		"configmaps",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(ktesting.CreateAction)
			if !ok {
				return true, nil, errors.New("expected ConfigMap creation")
			}

			cm, ok := create.GetObject().(*corev1.ConfigMap)
			if !ok {
				return true, nil, errors.New("expected ConfigMap object")
			}

			cm.UID, cm.ResourceVersion = "repository", "1"

			return false, nil, nil
		},
	)

	object := &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "archive", Namespace: "data"},
		Spec: v1alpha1.BackupRepositorySpec{
			Type: v1alpha1.BackupRepositoryTypeS3,
			S3: &v1alpha1.S3BackupRepositorySpec{
				Bucket:            "backups",
				CredentialsSecret: v1alpha1.BackupRepositorySecretReference{Name: "credentials"},
			},
		},
	}

	return client, object, owner
}

func TestRepositoryStorePersistsConcreteAPIAndFencesCleanup(t *testing.T) {
	client, object, owner := repositoryStoreFixture()

	store := NewConfigMapRepositoryStore(client)
	if err := store.Create(t.Context(), object, owner); err != nil {
		t.Fatal(err)
	}

	if object.UID == "" || object.ResourceVersion == "" || object.Generation != 1 {
		t.Fatal("repository has no durable identity")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(loaded, object) {
		t.Fatalf("API roundtrip changed repository: %+v", loaded)
	}

	loaded.Spec.S3.Bucket = "modified"

	again, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil || again.Spec.S3.Bucket != "backups" {
		t.Fatalf("aliased storage: %v", err)
	}

	foreign := owner

	foreign.UID = "another-workflow"
	if err := store.Delete(
		t.Context(),
		object,
		foreign,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("foreign owner accepted: %v", err)
	}

	stale := object.DeepCopy()

	stale.ResourceVersion = "older"
	if err := store.Delete(
		t.Context(),
		stale,
		owner,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("stale version accepted: %v", err)
	}

	cm, err := client.CoreV1().
		ConfigMaps(object.Namespace).
		Get(t.Context(), repositoryConfigMapName(object.Name), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if len(cm.OwnerReferences) != 0 || cm.Immutable == nil || !*cm.Immutable {
		t.Fatal("cross-namespace ownership or mutable repository snapshot")
	}

	for range 2 {
		if err := store.Delete(t.Context(), object, owner); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := store.Load(
		t.Context(),
		crclient.ObjectKeyFromObject(object),
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("repository retained: %v", err)
	}
}

func TestRepositoryStoreRejectsReplacedOwnerAndUnknownFields(t *testing.T) {
	client, object, owner := repositoryStoreFixture()
	store := NewConfigMapRepositoryStore(client)
	stale := owner

	stale.UID = "replaced"
	if err := store.Create(
		t.Context(),
		object,
		stale,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("stale owner accepted: %v", err)
	}

	if err := store.Create(t.Context(), object, owner); err != nil {
		t.Fatal(err)
	}

	cm, err := client.CoreV1().
		ConfigMaps(object.Namespace).
		Get(t.Context(), repositoryConfigMapName(object.Name), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	cm.Data[repositoryDataKey] = `{"apiVersion":"migrate.sealos.io/v1alpha1","kind":"BackupRepository","metadata":{"name":"archive","namespace":"data"},"request":{}}`
	if _, err := decodeRepositoryConfigMap(cm, crclient.ObjectKeyFromObject(object)); err == nil {
		t.Fatal("old request envelope accepted")
	}
}

func TestRepositoryStoreCancellationStopsCreation(t *testing.T) {
	client, object, owner := repositoryStoreFixture()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := NewConfigMapRepositoryStore(
		client,
	).Create(ctx, object, owner); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("error=%v", err)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			t.Fatal("canceled creation wrote repository")
		}
	}
}

func TestRepositoryStoreLostFenceStopsCreation(t *testing.T) {
	client, object, owner := repositoryStoreFixture()
	lost := errors.New("lease lost")

	ctx := WithLeaseFence(t.Context(), &testLeaseFence{err: lost})
	if err := NewConfigMapRepositoryStore(
		client,
	).Create(ctx, object, owner); !errors.Is(
		err,
		lost,
	) {
		t.Fatalf("error=%v", err)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			t.Fatal("lost lease wrote repository")
		}
	}
}
