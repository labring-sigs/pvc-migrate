package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

type fakeObject struct {
	data []byte
	etag string
}

type fakeS3 struct {
	mu       sync.Mutex
	objects  map[string]fakeObject
	nextETag int
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: map[string]fakeObject{}} }

func (f *fakeS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, missingS3ObjectError{}
	}
	return &s3.HeadObjectOutput{ETag: aws.String(object.etag)}, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, missingS3ObjectError{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(object.data)), ETag: aws.String(object.etag)}, nil
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(in.Key)
	existing, exists := f.objects[key]
	if aws.ToString(in.IfNoneMatch) == "*" && exists {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "object exists"}
	}
	if in.IfMatch != nil && (!exists || aws.ToString(in.IfMatch) != existing.etag) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "etag changed"}
	}
	f.nextETag++
	etag := fmt.Sprintf("etag-%d", f.nextETag)
	f.objects[key] = fakeObject{data: data, etag: etag}
	return &s3.PutObjectOutput{ETag: aws.String(etag)}, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := aws.ToString(in.Key)
	existing, exists := f.objects[key]
	if in.IfMatch != nil && (!exists || aws.ToString(in.IfMatch) != existing.etag) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "etag changed"}
	}
	delete(f.objects, key)
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := aws.ToString(in.Prefix)
	keys := make([]string, 0)
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	contents := make([]types.Object, 0, len(keys))
	for _, key := range keys {
		object := f.objects[key]
		contents = append(contents, types.Object{Key: aws.String(key), Size: aws.Int64(int64(len(object.data))), ETag: aws.String(object.etag)})
	}
	return &s3.ListObjectsV2Output{Contents: contents}, nil
}

type missingS3ObjectError struct{}

func (missingS3ObjectError) Error() string                 { return "missing object" }
func (missingS3ObjectError) ErrorCode() string             { return "NoSuchKey" }
func (missingS3ObjectError) ErrorMessage() string          { return "missing object" }
func (missingS3ObjectError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

type timeoutS3 struct{ API }

func (timeoutS3) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, context.DeadlineExceeded
}

func (timeoutS3) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return nil, context.DeadlineExceeded
}

func (timeoutS3) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, context.DeadlineExceeded
}

func (timeoutS3) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return nil, context.DeadlineExceeded
}

func newTestStore(t *testing.T, client API) *Store {
	t.Helper()
	store, err := NewWithClient(client, Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"}, Credentials{AccessKey: "key", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestManifestMissingAndImmutable(t *testing.T) {
	store := newTestStore(t, newFakeS3())
	manifest, err := store.Manifest(context.Background())
	if err != nil || manifest != nil {
		t.Fatalf("missing manifest = %#v, err=%v", manifest, err)
	}
	want := Manifest{CreatedAt: time.Unix(10, 0).UTC(), SourceNamespace: "default", SourcePVC: "data", SourcePVCUID: "uid", Capacity: "1Gi", VolumeMode: "Filesystem", Consistency: "offline file-consistent copy", Compression: "none", InventorySHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
	if err := store.PutManifest(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := store.Manifest(context.Background())
	if err != nil || got == nil || got.SourcePVCUID != want.SourcePVCUID || got.Version != manifestVersion {
		t.Fatalf("manifest = %#v, err=%v", got, err)
	}
	if err := store.PutManifest(context.Background(), want); err == nil {
		t.Fatal("second manifest publication succeeded")
	}
}

func TestS3OperationsPreserveTimeoutCategory(t *testing.T) {
	store := newTestStore(t, timeoutS3{API: newFakeS3()})
	ctx := context.Background()
	for name, run := range map[string]func() error{
		"manifest": func() error {
			_, err := store.Manifest(ctx)
			return err
		},
		"inventory": func() error {
			_, err := store.Inventory(ctx)
			return err
		},
		"lock": func() error {
			_, err := store.AcquireLock(ctx, "holder", time.Minute)
			return err
		},
		"release": func() error {
			return store.ReleaseLock(ctx, "etag")
		},
	} {
		t.Run(name, func(t *testing.T) {
			if category := domain.CategoryOf(run()); category != domain.ErrorTimeout {
				t.Fatalf("category=%s, want timeout", category)
			}
		})
	}
}

func TestManifestRejectsInvalidIdentity(t *testing.T) {
	client := newFakeS3()
	store := newTestStore(t, client)
	client.objects[store.manifestKey()] = fakeObject{data: []byte(`{"version":1,"bucket":"other","name":"daily"}`), etag: "x"}
	if _, err := store.Manifest(context.Background()); err == nil {
		t.Fatal("invalid manifest identity was accepted")
	}
}

func TestLockExclusionAndExpiredReplacement(t *testing.T) {
	client := newFakeS3()
	store := newTestStore(t, client)
	ctx := context.Background()
	etag, err := store.AcquireLock(ctx, "first", time.Minute)
	if err != nil || etag == "" {
		t.Fatalf("acquire first lock: etag=%q err=%v", etag, err)
	}
	if _, err := store.AcquireLock(ctx, "second", time.Minute); err == nil {
		t.Fatal("concurrent lock acquisition succeeded")
	}
	client.mu.Lock()
	lockObject := client.objects[store.lockKey()]
	lockObject.data = []byte(`{"holder":"first","expiresAt":"2000-01-01T00:00:00Z"}`)
	client.objects[store.lockKey()] = lockObject
	client.mu.Unlock()
	replaced, err := store.AcquireLock(ctx, "second", time.Minute)
	if err != nil || replaced == etag || replaced == "" {
		t.Fatalf("expired lock replacement: etag=%q err=%v", replaced, err)
	}
	if err := store.ReleaseLock(ctx, replaced); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Manifest(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestLockRenewalUsesConditionalOwnership(t *testing.T) {
	client := newFakeS3()
	store := newTestStore(t, client)
	ctx := context.Background()
	etag, err := store.AcquireLock(ctx, "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := store.RenewLock(ctx, "first", etag, time.Minute)
	if err != nil || renewed == "" || renewed == etag {
		t.Fatalf("renewed ETag=%q err=%v", renewed, err)
	}
	if _, err := store.RenewLock(ctx, "second", renewed, time.Minute); err == nil {
		t.Fatal("foreign holder renewed the lock")
	}
	if err := store.ReleaseLock(ctx, ""); err == nil {
		t.Fatal("unconditional lock release succeeded")
	}
	if err := store.ReleaseLock(ctx, renewed); err != nil {
		t.Fatal(err)
	}
}

func TestLockReleaseRejectsStaleETag(t *testing.T) {
	client := newFakeS3()
	store := newTestStore(t, client)
	first, err := store.AcquireLock(context.Background(), "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	lockObject := client.objects[store.lockKey()]
	lockObject.etag = "new-etag"
	client.objects[store.lockKey()] = lockObject
	client.mu.Unlock()
	if category := domain.CategoryOf(store.ReleaseLock(context.Background(), first)); category != domain.ErrorConflict {
		t.Fatalf("stale release category=%s, want conflict", category)
	}
}

func TestValidateConfigSecurityBoundaries(t *testing.T) {
	cases := []Config{
		{Bucket: "", Name: "daily"},
		{Bucket: "backups", Name: "daily", Endpoint: "http://s3.example"},
		{Bucket: "backups", Name: "daily", AccessKey: "key"},
		{Bucket: "backups", Name: "daily", ServerSideEncryption: "invalid"},
	}
	for _, cfg := range cases {
		if err := ValidateConfig(cfg); err == nil {
			t.Fatalf("accepted invalid config: %#v", cfg)
		}
	}
	valid := Config{Bucket: "backups", Prefix: "team/data", Name: "daily", Endpoint: "http://s3.example", AllowInsecureEndpoint: true, ServerSideEncryption: "aws:kms", SSEKMSKeyID: "key-id", AccessKey: "key", SecretKey: "secret"}
	if err := ValidateConfig(valid); err != nil {
		t.Fatal(err)
	}
}

func TestValidatePathRejectsAbsoluteAndTraversalPaths(t *testing.T) {
	for _, path := range []string{"/data", "../data", "data/../other", "data\\child", "data//other"} {
		if err := ValidatePath(path); err == nil {
			t.Fatalf("accepted unsafe path %q", path)
		}
	}
	for _, path := range []string{"", "data", "data/nested-1"} {
		if err := ValidatePath(path); err != nil {
			t.Fatalf("rejected safe path %q: %v", path, err)
		}
	}
}

func TestInventoryDetectsObjectMutationAndIgnoresOtherPrefixes(t *testing.T) {
	client := newFakeS3()
	store := newTestStore(t, client)
	client.objects["pv-migrate/daily/b.txt"] = fakeObject{data: []byte("two"), etag: "etag-b"}
	client.objects["pv-migrate/daily/a.txt"] = fakeObject{data: []byte("one"), etag: "etag-a"}
	client.objects["pv-migrate/other/file.txt"] = fakeObject{data: []byte("ignored"), etag: "etag-other"}

	inventory, err := store.Inventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if inventory.ObjectCount != 2 || inventory.TotalBytes != 6 || len(inventory.SHA256) != 64 {
		t.Fatalf("inventory=%+v", inventory)
	}
	manifest := Manifest{ObjectCount: inventory.ObjectCount, TotalBytes: inventory.TotalBytes, InventorySHA256: inventory.SHA256}
	if err := store.VerifyInventory(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	client.objects["pv-migrate/daily/a.txt"] = fakeObject{data: []byte("ONE"), etag: "etag-a-new"}
	if err := store.VerifyInventory(context.Background(), manifest); err == nil {
		t.Fatal("mutated backup object matched the published inventory")
	}
}

func TestRcloneConfigSelectsProviderForEndpoint(t *testing.T) {
	withoutEndpoint := newTestStore(t, newFakeS3())
	if !strings.Contains(withoutEndpoint.RcloneConfig(), "provider = AWS\n") {
		t.Fatal("managed S3 configuration should use the AWS provider by default")
	}

	withEndpoint, err := NewWithClient(newFakeS3(), Config{
		Bucket:   "backups",
		Name:     "daily",
		Endpoint: "https://minio.example",
	}, Credentials{AccessKey: "key", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(withEndpoint.RcloneConfig(), "provider = Other\n") {
		t.Fatal("custom S3 endpoint should use the generic provider by default")
	}

	explicit, err := NewWithClient(newFakeS3(), Config{
		Bucket:   "backups",
		Name:     "daily",
		Endpoint: "https://minio.example",
		Provider: "Minio",
	}, Credentials{AccessKey: "key", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(explicit.RcloneConfig(), "provider = Minio\n") {
		t.Fatal("explicit S3 provider was overridden")
	}
}
