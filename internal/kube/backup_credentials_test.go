package kube

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBackupCredentialsSecretLifecycle(t *testing.T) {
	client := fake.NewClientset()

	secret, err := CreateBackupCredentialsSecret(
		context.Background(),
		client,
		"sessions",
		"backup-test",
		map[string][]byte{
			BackupAccessKeyDataKey: []byte("access"),
			BackupSecretKeyDataKey: []byte("secret"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if secret.Immutable == nil || !*secret.Immutable || secret.Labels[SessionKey] != "backup-test" {
		t.Fatalf("credentials Secret metadata = %#v", secret)
	}

	ref := domain.ObjectReference{Namespace: secret.Namespace, Name: secret.Name, UID: secret.UID}

	loaded, err := GetBackupCredentialsSecret(context.Background(), client, ref, "backup-test")
	if err != nil {
		t.Fatal(err)
	}

	if string(loaded.Data[BackupAccessKeyDataKey]) != "access" ||
		string(loaded.Data[BackupSecretKeyDataKey]) != "secret" {
		t.Fatalf("credentials data = %#v", loaded.Data)
	}

	if _, err := GetBackupCredentialsSecret(
		context.Background(),
		client,
		ref,
		"other-session",
	); err == nil {
		t.Fatal("expected ownership mismatch")
	}

	if err := DeleteBackupCredentialsSecret(
		context.Background(),
		client,
		ref,
		"backup-test",
	); err != nil {
		t.Fatal(err)
	}
}

func TestBackupCredentialsSecretRejectsReplacementAndWrongName(t *testing.T) {
	client := fake.NewClientset()

	secret, err := CreateBackupCredentialsSecret(
		context.Background(),
		client,
		"sessions",
		"backup-test",
		map[string][]byte{BackupAccessKeyDataKey: []byte("access")},
	)
	if err != nil {
		t.Fatal(err)
	}

	ref := domain.ObjectReference{Namespace: secret.Namespace, Name: secret.Name, UID: secret.UID}

	secret.Labels[SessionKey] = "replacement"
	if _, err := client.CoreV1().
		Secrets(secret.Namespace).
		Update(context.Background(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := GetBackupCredentialsSecret(
		context.Background(),
		client,
		ref,
		"backup-test",
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("replacement category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := ValidateBackupCredentialsSecretCleanup(
		context.Background(),
		client,
		ref,
		"backup-test",
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("cleanup validation category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := DeleteBackupCredentialsSecret(
		context.Background(),
		client,
		ref,
		"backup-test",
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("cleanup category=%s error=%v", domain.CategoryOf(err), err)
	}

	ref.Name = "unrelated-secret"
	if err := DeleteBackupCredentialsSecret(
		context.Background(),
		client,
		ref,
		"backup-test",
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("wrong-name category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestBackupCredentialsSecretMissingIsActionable(t *testing.T) {
	client := fake.NewClientset()

	ref := domain.ObjectReference{
		Namespace: "sessions",
		Name:      BackupCredentialsSecretName("backup-test"),
	}
	if _, err := GetBackupCredentialsSecret(
		context.Background(),
		client,
		ref,
		"backup-test",
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("missing Secret category=%q error=%v", domain.CategoryOf(err), err)
	}

	if err := ValidateBackupCredentialsSecretCleanup(
		context.Background(),
		client,
		ref,
		"backup-test",
	); err != nil {
		t.Fatalf("missing Secret cleanup validation: %v", err)
	}
}
