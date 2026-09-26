package kube

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestBackupCredentialsSecretNameIsStableHash(t *testing.T) {
	first := BackupCredentialsSecretName("backup-test")
	second := BackupCredentialsSecretName("backup-test")

	if first == "" || first != second {
		t.Fatalf("name not deterministic: %q vs %q", first, second)
	}

	if first == BackupCredentialsSecretName("other-session") {
		t.Fatal("distinct sessions must derive distinct Secret names")
	}

	if len(first) <= len(BackupCredentialsSecretPrefix) {
		t.Fatalf("name %q carries no digest", first)
	}
}

func TestValidateS3CredentialsData(t *testing.T) {
	valid := map[string][]byte{
		BackupAccessKeyDataKey: []byte("access"),
		BackupSecretKeyDataKey: []byte("secret"),
	}
	if err := ValidateS3CredentialsData(valid); err != nil {
		t.Fatal(err)
	}

	withToken := map[string][]byte{
		BackupAccessKeyDataKey:    []byte("access"),
		BackupSecretKeyDataKey:    []byte("secret"),
		BackupSessionTokenDataKey: []byte("token"),
	}
	if err := ValidateS3CredentialsData(withToken); err != nil {
		t.Fatal(err)
	}

	missing := map[string][]byte{BackupAccessKeyDataKey: []byte("access")}
	if err := ValidateS3CredentialsData(
		missing,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("missing secretKey category=%s error=%v", domain.CategoryOf(err), err)
	}

	unsafe := map[string][]byte{
		BackupAccessKeyDataKey: []byte("access"),
		BackupSecretKeyDataKey: []byte("se\rcret"),
	}
	if err := ValidateS3CredentialsData(unsafe); domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("control character category=%s error=%v", domain.CategoryOf(err), err)
	}
}
