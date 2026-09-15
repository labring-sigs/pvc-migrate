package objectstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/stretchr/testify/require"
)

// conditionalWriteIgnoringS3 models an S3 backend that silently ignores
// If-None-Match (the observed MinIO pre-2023-12 behavior on non-versioned
// buckets): every PutObject overwrites whatever is stored.
type conditionalWriteIgnoringS3 struct{ objectstore.API }

func (c conditionalWriteIgnoringS3) PutObject(
	ctx context.Context,
	input *s3.PutObjectInput,
	options ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	return c.API.PutObject(ctx, input, options...)
}

// AcquireLock must read the lock back after a successful conditional put and
// refuse ownership when the backend let a concurrent holder's lock win —
// failing closed instead of holding a lock two workers believe they own.
func TestAcquireLockDetectsIgnoredConditionalWrite(t *testing.T) {
	base := newFakeS3()
	client := conditionalWriteIgnoringS3{API: base}
	store := newTestStore(t, client)
	ctx := context.Background()

	firstEtag, err := store.AcquireLock(ctx, "holder-first", time.Minute)
	require.NoError(t, err)
	require.NotEmpty(t, firstEtag)

	_, err = store.AcquireLock(ctx, "holder-second", time.Minute)
	require.Error(t, err)
	require.Contains(t, err.Error(), "locked by holder-first")
}
