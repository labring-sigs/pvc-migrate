package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRepositoryResolverUsesConfigMapAPIWithoutCRDClient(t *testing.T) {
	owner := v1alpha1.ObjectReference{Namespace: "state", Name: "backup", UID: "workflow"}
	client := fake.NewClientset(
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: owner.Namespace,
				Name:      kube.SessionConfigMapName(owner.Name),
				UID:       owner.UID,
				Labels: map[string]string{
					kube.SessionKey:     owner.Name,
					kube.ManagedByLabel: kube.ManagedByValue,
				},
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

	repository := s3BindingRepository()
	repository.UID, repository.ResourceVersion = "", ""
	repository.Spec.S3.Provider = "Minio"
	repository.Spec.S3.CredentialsSecret.Name = kube.BackupCredentialsSecretName(owner.Name)

	store := kube.NewConfigMapRepositoryStore(client)
	if err := store.Create(t.Context(), repository, owner); err != nil {
		t.Fatal(err)
	}

	writeErr := errors.New("credential creation response lost")
	client.PrependReactor(
		"create",
		"secrets",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(ktesting.CreateAction)
			if !ok {
				return true, nil, errors.New("expected Secret creation")
			}

			secret, ok := create.GetObject().(*corev1.Secret)
			if !ok {
				return true, nil, errors.New("expected Secret object")
			}

			secret.UID = "credentials"
			if err := client.Tracker().Add(secret.DeepCopy()); err != nil {
				return true, nil, err
			}

			return true, nil, writeErr
		},
	)

	credentials := map[string][]byte{
		kube.BackupAccessKeyDataKey: []byte("recovery-access-value"),
		kube.BackupSecretKeyDataKey: []byte("recovery-secret-value"),
	}
	if err := store.CreateCredentials(
		t.Context(),
		repository.Namespace,
		owner,
		credentials,
	); !errors.Is(
		err,
		writeErr,
	) {
		t.Fatalf("ambiguous credential creation error: %v", err)
	}

	configMaps, err := client.CoreV1().ConfigMaps("").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := json.Marshal(configMaps)
	if err != nil {
		t.Fatal(err)
	}

	for _, value := range credentials {
		if bytes.Contains(encoded, value) {
			t.Fatal("credential material persisted in a ConfigMap")
		}
	}

	var resolved objectstore.Config

	resolver := NewS3RepositoryResolver(
		store.Load,
		client,
		"cluster",
		func(_ context.Context, config objectstore.Config) (*objectstore.Store, error) {
			resolved = config
			return objectstore.NewConfigOnly(config)
		},
	)

	_, binding, err := resolveTransferRepository(
		t.Context(),
		resolver,
		"tenant",
		"archive",
		"daily",
	)
	if err != nil {
		t.Fatal(err)
	}

	if resolved.AccessKey != string(credentials[kube.BackupAccessKeyDataKey]) ||
		resolved.SecretKey != string(credentials[kube.BackupSecretKeyDataKey]) ||
		resolved.Name != "daily" ||
		resolved.Provider != "Minio" || binding.Type != v1alpha1.BackupRepositoryTypeS3 ||
		binding.UID != repository.UID ||
		binding.S3.CredentialsSecretUID != "credentials" {
		t.Fatal("repository storage lost routing or binding identity")
	}

	object := plannedBackupObject()
	checkpoints := &backupCheckpointStore{object: object.DeepCopy(), err: writeErr}

	save := func(ctx context.Context) error { return checkpoints.Save(ctx, object) }
	if err := PinRepository(
		t.Context(),
		binding,
		&object.Status.Repository,
		save,
	); !errors.Is(
		err,
		writeErr,
	) {
		t.Fatalf("repository checkpoint error: %v", err)
	}

	if object.Status.Repository != nil {
		t.Fatal("failed binding checkpoint changed local state")
	}

	checkpoints.err = nil

	_, binding, err = resolveTransferRepository(
		t.Context(),
		resolver,
		repository.Namespace,
		repository.Name,
		"daily",
	)
	if err != nil {
		t.Fatalf("ambiguous write lost recoverable credentials: %v", err)
	}

	if err := PinRepository(t.Context(), binding, &object.Status.Repository, save); err != nil {
		t.Fatal(err)
	}

	if checkpoints.object.Status.Repository.S3.CredentialsSecretUID != "credentials" {
		t.Fatal("retry did not checkpoint recovered credential identity")
	}
}

func TestRepositoryLoaderCannotRedirectCredentialNamespace(t *testing.T) {
	client := fake.NewClientset()
	repository := s3BindingRepository()
	repository.Namespace = "other-tenant"

	_, _, err := resolveStoredS3Repository(
		t.Context(),
		func(context.Context, crclient.ObjectKey) (*v1alpha1.BackupRepository, error) { return repository, nil },
		client,
		crclient.ObjectKey{
			Namespace: "tenant",
			Name:      repository.Name,
		},
		"tenant",
		"cluster",
		"daily",
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("redirected repository accepted: %v", err)
	}

	if len(client.Actions()) != 0 {
		t.Fatal("foreign credentials were accessed")
	}
}
