package objectstore_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startMinIO returns a real S3 endpoint. It prefers an externally
// provisioned MinIO/S3 endpoint supplied through PVC_MIGRATE_TEST_S3_ENDPOINT
// (plus PVC_MIGRATE_TEST_S3_ACCESS_KEY / PVC_MIGRATE_TEST_S3_SECRET_KEY),
// which keeps the suite runnable where Docker Hub is unreachable. Otherwise
// it launches a throwaway MinIO container via testcontainers.
func startMinIO(t *testing.T) (endpoint, accessKey, secretKey string) {
	t.Helper()

	if env := os.Getenv("PVC_MIGRATE_TEST_S3_ENDPOINT"); env != "" {
		return env,
			envOr("PVC_MIGRATE_TEST_S3_ACCESS_KEY", "minioadmin"),
			envOr("PVC_MIGRATE_TEST_S3_SECRET_KEY", "minioadmin")
	}

	ctx := context.Background()

	access := envOr("PVC_MIGRATE_TEST_MINIO_ROOT_USER", "minioadmin")
	secret := envOr("PVC_MIGRATE_TEST_MINIO_ROOT_PASSWORD", access)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "quay.io/minio/minio:latest",
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     access,
				"MINIO_ROOT_PASSWORD": secret,
			},
			Cmd: []string{"server", "/data"},
			WaitingFor: wait.ForHTTP("/minio/health/ready").
				WithPort("9000/tcp").
				WithStatusCodeMatcher(func(code int) bool { return code == http.StatusOK }),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("docker unavailable or MinIO failed to start: %v", err)
	}

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_ = container.Terminate(shutdownCtx)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)

	return "http://" + host + ":" + port.Port(), access, secret
}

func createBucket(t *testing.T, endpoint, access, secret string) {
	t.Helper()

	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(access, secret, ""),
		),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("pvc-migrate-unit-tests"),
	})
	if err != nil {
		if _, asErr := errors.AsType[*s3types.BucketAlreadyOwnedByYou](err); !asErr {
			require.NoError(t, err)
		}
	}
}

func newExternalS3Store(t *testing.T) *objectstore.Store {
	t.Helper()

	endpoint, access, secret := startMinIO(t)
	createBucket(t, endpoint, access, secret)

	store, err := objectstore.New(context.Background(), objectstore.Config{
		Bucket:                "pvc-migrate-unit-tests",
		Name:                  "rp-" + time.Now().UTC().Format("20060102T150405.000000000"),
		Provider:              "Minio",
		Endpoint:              endpoint,
		AccessKey:             access,
		SecretKey:             secret,
		AllowInsecureEndpoint: true,
		ForcePathStyle:        true,
	})
	require.NoError(t, err)

	return store
}

func TestStoreAccessors(t *testing.T) {
	store := newExternalS3Store(t)

	if got := store.Backend(); got != "s3" {
		t.Errorf("Backend()=%q want s3", got)
	}

	if store.Config().Bucket != "pvc-migrate-unit-tests" {
		t.Errorf("Config().Bucket=%q", store.Config().Bucket)
	}

	if !strings.HasPrefix(store.Config().Name, "rp-") {
		t.Errorf("Config().Name=%q want rp- prefix", store.Config().Name)
	}

	creds := store.Credentials()
	if creds.AccessKey == "" || creds.SecretKey == "" {
		t.Errorf("Credentials() lost static keys: %+v", creds)
	}

	if got := store.RemotePath(); got == "" {
		t.Error("RemotePath() is empty")
	}

	if got := store.Destination(); got == "" {
		t.Error("Destination() is empty")
	}
}

func TestStoreManifestRoundTrip(t *testing.T) {
	store := newExternalS3Store(t)
	ctx := context.Background()

	got, err := store.Manifest(ctx)
	if err != nil {
		t.Fatalf("pre-publish Manifest: %v", err)
	}

	if got != nil {
		t.Fatalf("pre-publish Manifest should be nil, got %+v", got)
	}

	inventory, err := store.Inventory(ctx)
	require.NoError(t, err)

	manifest := objectstore.Manifest{
		Version:         2,
		CreatedAt:       time.Now().UTC(),
		Bucket:          "pvc-migrate-unit-tests",
		Name:            "recovery-point-1",
		SourceNamespace: "tenant",
		SourcePVC:       "data",
		SourcePVCUID:    "pvc-uid-test",
		SourcePV:        "pv-test",
		SourcePVUID:     "pv-uid-test",
		Capacity:        "1Gi",
		VolumeMode:      "Filesystem",
		Consistency:     "offline",
		Compression:     "none",
		ObjectCount:     inventory.ObjectCount,
		TotalBytes:      inventory.TotalBytes,
		InventorySHA256: inventory.SHA256,
	}
	require.NoError(t, store.PutManifest(ctx, manifest))

	got, err = store.Manifest(ctx)
	require.NoError(t, err)

	if got.SourcePVC != manifest.SourcePVC || got.SourcePVCUID != manifest.SourcePVCUID {
		t.Fatalf("round-trip manifest mismatch: got %+v", got)
	}

	if got.ObjectCount != manifest.ObjectCount {
		t.Errorf("ObjectCount=%d want %d", got.ObjectCount, manifest.ObjectCount)
	}

	inv, err := store.Inventory(ctx)
	require.NoError(t, err)

	if inv.ObjectCount != manifest.ObjectCount {
		t.Errorf("Inventory.ObjectCount=%d want %d", inv.ObjectCount, manifest.ObjectCount)
	}

	require.NoError(t, store.VerifyInventory(ctx, manifest))
}

func TestStoreVerifyInventoryDetectsDrift(t *testing.T) {
	store := newExternalS3Store(t)
	ctx := context.Background()

	inventory, err := store.Inventory(ctx)
	require.NoError(t, err)

	manifest := objectstore.Manifest{
		Version:         2,
		CreatedAt:       time.Now().UTC(),
		Bucket:          "pvc-migrate-unit-tests",
		Name:            "recovery-point-1",
		SourceNamespace: "tenant",
		SourcePVC:       "data",
		SourcePVCUID:    "pvc-uid-test",
		SourcePV:        "pv-test",
		SourcePVUID:     "pv-uid-test",
		Capacity:        "1Gi",
		VolumeMode:      "Filesystem",
		Consistency:     "offline",
		Compression:     "none",
		ObjectCount:     inventory.ObjectCount,
		TotalBytes:      inventory.TotalBytes,
		InventorySHA256: inventory.SHA256,
	}
	require.NoError(t, store.PutManifest(ctx, manifest))

	tampered := manifest
	tampered.TotalBytes = manifest.TotalBytes + 99

	err = store.VerifyInventory(ctx, tampered)
	if err == nil {
		t.Fatal("VerifyInventory must detect published-object drift")
	}
}

func TestStoreLockLifecycle(t *testing.T) {
	store := newExternalS3Store(t)
	ctx := context.Background()

	etag, err := store.AcquireLock(ctx, "holder-1", 30*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, etag)

	// NOTE: mutual exclusion of a second AcquireLock while held relies on the
	// S3 backend enforcing PutObject If-None-Match:*. Some S3 implementations
	// (MinIO releases before 2023-12 on non-versioned buckets) silently accept
	// the overwrite, so that exclusion cannot be asserted portably here.

	newEtag, err := store.RenewLock(ctx, "holder-1", etag, 30*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, newEtag)

	require.NoError(t, store.ReleaseLock(ctx, newEtag))

	if _, err := store.AcquireLock(ctx, "holder-2", 30*time.Second); err != nil {
		t.Fatalf("AcquireLock after release failed: %v", err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
