package backup

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
)

type repositoryStoreStub struct{ backend string }

func (s repositoryStoreStub) Backend() string   { return s.backend }
func (repositoryStoreStub) Destination() string { return "repository://stub" }

func (repositoryStoreStub) Manifest(context.Context) (*objectstore.Manifest, error) {
	return nil, nil
}

func (repositoryStoreStub) PutManifest(context.Context, objectstore.Manifest) error {
	return nil
}

func (repositoryStoreStub) Inventory(context.Context) (objectstore.Inventory, error) {
	return objectstore.Inventory{}, nil
}

func (repositoryStoreStub) VerifyInventory(context.Context, objectstore.Manifest) error {
	return nil
}

func (repositoryStoreStub) AcquireLock(context.Context, string, time.Duration) (string, error) {
	return "", nil
}

func (repositoryStoreStub) ReleaseLock(context.Context, string) error { return nil }

func (repositoryStoreStub) RenewLock(
	context.Context,
	string,
	string,
	time.Duration,
) (string, error) {
	return "", nil
}

type s3RepositoryStoreStub struct{ repositoryStoreStub }

func (s3RepositoryStoreStub) Config() objectstore.Config { return objectstore.Config{} }

func (s3RepositoryStoreStub) Credentials() objectstore.Credentials {
	return objectstore.Credentials{}
}

func (s3RepositoryStoreStub) RemotePath() string { return "" }

func (s3RepositoryStoreStub) RcloneConfig() string { return "" }

var (
	_ RepositoryStore   = repositoryStoreStub{}
	_ S3RepositoryStore = s3RepositoryStoreStub{}
)

func TestRequireS3RepositoryBackend(t *testing.T) {
	t.Parallel()

	if err := requireS3RepositoryBackend(nil); err == nil {
		t.Fatal("nil repository store was accepted")
	}

	if err := requireS3RepositoryBackend(repositoryStoreStub{backend: "s3"}); err != nil {
		t.Fatalf("matching backend rejected: %v", err)
	}

	err := requireS3RepositoryBackend(repositoryStoreStub{backend: "pvc"})
	if err == nil || !strings.Contains(
		err.Error(),
		`repository backend "pvc" is not supported by the S3 transfer path`,
	) {
		t.Fatalf("unexpected non-S3 backend error: %v", err)
	}
}

func TestRepositoryStoreDoesNotRequireS3Capabilities(t *testing.T) {
	t.Parallel()

	store := repositoryStoreStub{backend: "pvc"}
	if _, ok := any(store).(S3RepositoryStore); ok {
		t.Fatal("backend-neutral repository unexpectedly implements the S3 adapter contract")
	}
}
