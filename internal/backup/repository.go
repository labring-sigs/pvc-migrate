package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
)

// RepositoryStore is the data-plane contract used by backup and restore.
//
// Backend-neutral operations stay here so future PVC or snapshot adapters do
// not have to expose object-store credentials or rclone configuration.
type RepositoryStore interface {
	Backend() string
	Destination() string

	Manifest(ctx context.Context) (*objectstore.Manifest, error)
	PutManifest(ctx context.Context, manifest objectstore.Manifest) error
	Inventory(ctx context.Context) (objectstore.Inventory, error)
	VerifyInventory(ctx context.Context, manifest objectstore.Manifest) error
	AcquireLock(ctx context.Context, holder string, ttl time.Duration) (string, error)
	ReleaseLock(ctx context.Context, etag string) error
	RenewLock(ctx context.Context, holder, etag string, ttl time.Duration) (string, error)
}

// S3RepositoryStore is the existing object-store adapter used by the rclone
// transfer path. Other repository types get their own adapter contract and
// controller dispatch path.
type S3RepositoryStore interface {
	RepositoryStore

	Config() objectstore.Config
	Credentials() objectstore.Credentials
	RemotePath() string
	RcloneConfig() string
}

func requireS3RepositoryBackend(store RepositoryStore) error {
	if store == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"backup repository",
			"repository store is required",
		)
	}

	if store.Backend() == string(domain.BackupBackendS3) {
		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"backup repository",
		fmt.Sprintf(
			"repository backend %q is not supported by the S3 transfer path",
			store.Backend(),
		),
	)
}
