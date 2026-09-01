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
// The original implementation exposed *objectstore.Store throughout the
// workflow code, which made every future repository backend look like S3.
// Keeping the contract here lets each BackupRepository type provide its own
// implementation while the workflow state machine continues to own locking,
// checkpoints, and PVC safety. Config/RcloneConfig are currently needed by
// the S3 transfer adapter; non-S3 adapters should return their own transfer
// contract once their data mover is enabled.
type RepositoryStore interface {
	Backend() string
	Config() objectstore.Config
	Credentials() objectstore.Credentials
	RemotePath() string
	Destination() string
	RcloneConfig() string

	Manifest(ctx context.Context) (*objectstore.Manifest, error)
	PutManifest(ctx context.Context, manifest objectstore.Manifest) error
	Inventory(ctx context.Context) (objectstore.Inventory, error)
	VerifyInventory(ctx context.Context, manifest objectstore.Manifest) error
	AcquireLock(ctx context.Context, holder string, ttl time.Duration) (string, error)
	ReleaseLock(ctx context.Context, etag string) error
	RenewLock(ctx context.Context, holder, etag string, ttl time.Duration) (string, error)
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
