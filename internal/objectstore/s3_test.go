package objectstore_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	. "github.com/labring-sigs/pvc-migrate/internal/objectstore"
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

func TestConfigOnlyStoreRejectsNetworkOperations(t *testing.T) {
	store, err := NewConfigOnly(Config{Bucket: "backups", Name: "daily"})
	if err != nil {
		t.Fatal(err)
	}

	manifest := Manifest{Version: 2}

	operations := []struct {
		name string
		run  func() error
	}{
		{
			name: "manifest",
			run:  func() error { _, err := store.Manifest(context.Background()); return err },
		},
		{
			name: "put manifest",
			run:  func() error { return store.PutManifest(context.Background(), manifest) },
		},
		{
			name: "inventory",
			run:  func() error { _, err := store.Inventory(context.Background()); return err },
		},
		{
			name: "acquire lock",
			run:  func() error { _, err := store.AcquireLock(context.Background(), "holder", time.Minute); return err },
		},
		{
			name: "release lock",
			run:  func() error { return store.ReleaseLock(context.Background(), "etag") },
		},
		{name: "renew lock", run: func() error {
			_, err := store.RenewLock(context.Background(), "holder", "etag", time.Minute)
			return err
		}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			err := operation.run()
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), "client is not configured") {
				t.Fatalf("category=%q error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func (f *fakeS3) HeadObject(
	_ context.Context,
	in *s3.HeadObjectInput,
	_ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	object, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, missingS3ObjectError{}
	}

	return &s3.HeadObjectOutput{ETag: aws.String(object.etag)}, nil
}

func (f *fakeS3) GetObject(
	_ context.Context,
	in *s3.GetObjectInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	object, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, missingS3ObjectError{}
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(object.data)),
		ETag: aws.String(object.etag),
	}, nil
}

func (f *fakeS3) PutObject(
	_ context.Context,
	in *s3.PutObjectInput,
	_ ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
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

func (f *fakeS3) DeleteObject(
	_ context.Context,
	in *s3.DeleteObjectInput,
	_ ...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
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

func (f *fakeS3) ListObjectsV2(
	_ context.Context,
	in *s3.ListObjectsV2Input,
	_ ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
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
		contents = append(
			contents,
			types.Object{
				Key:  aws.String(key),
				Size: aws.Int64(int64(len(object.data))),
				ETag: aws.String(object.etag),
			},
		)
	}

	return &s3.ListObjectsV2Output{Contents: contents}, nil
}

type missingS3ObjectError struct{}

func (missingS3ObjectError) Error() string                 { return "missing object" }
func (missingS3ObjectError) ErrorCode() string             { return "NoSuchKey" }
func (missingS3ObjectError) ErrorMessage() string          { return "missing object" }
func (missingS3ObjectError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

type missingS3Bucket struct{}

func (missingS3Bucket) HeadObject(
	context.Context,
	*s3.HeadObjectInput,
	...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "missing bucket"}
}

func (missingS3Bucket) GetObject(
	context.Context,
	*s3.GetObjectInput,
	...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "missing bucket"}
}

func (missingS3Bucket) PutObject(
	context.Context,
	*s3.PutObjectInput,
	...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "missing bucket"}
}

func (missingS3Bucket) DeleteObject(
	context.Context,
	*s3.DeleteObjectInput,
	...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "missing bucket"}
}

func (missingS3Bucket) ListObjectsV2(
	context.Context,
	*s3.ListObjectsV2Input,
	...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return nil, &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "missing bucket"}
}

type timeoutS3 struct{ API }

type getObjectErrorS3 struct {
	API
	err error
}

func (s getObjectErrorS3) GetObject(
	context.Context,
	*s3.GetObjectInput,
	...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	return nil, s.err
}

func (timeoutS3) GetObject(
	context.Context,
	*s3.GetObjectInput,
	...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	return nil, context.DeadlineExceeded
}

func (timeoutS3) ListObjectsV2(
	context.Context,
	*s3.ListObjectsV2Input,
	...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return nil, context.DeadlineExceeded
}

func (timeoutS3) PutObject(
	context.Context,
	*s3.PutObjectInput,
	...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	return nil, context.DeadlineExceeded
}

func (timeoutS3) DeleteObject(
	context.Context,
	*s3.DeleteObjectInput,
	...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
	return nil, context.DeadlineExceeded
}

type conditionalWriteUnsupportedS3 struct{ API }

func (c conditionalWriteUnsupportedS3) PutObject(
	ctx context.Context,
	input *s3.PutObjectInput,
	options ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	if input.IfNoneMatch != nil || input.IfMatch != nil {
		return nil, &smithy.GenericAPIError{
			Code:    "NotImplemented",
			Message: "conditional writes are unsupported",
		}
	}

	return c.API.PutObject(ctx, input, options...)
}

func newTestStore(t *testing.T, client API) *Store {
	t.Helper()

	store, err := NewWithClient(
		client,
		Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
		Credentials{AccessKey: "key", SecretKey: "secret"},
	)
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

	want := Manifest{
		CreatedAt:       time.Unix(10, 0).UTC(),
		SessionID:       "backup-session",
		SourceNamespace: "default",
		SourcePVC:       "data",
		SourcePVCUID:    "uid",
		SourcePV:        "pv-data",
		SourcePVUID:     "pv-uid",
		Capacity:        "1Gi",
		VolumeMode:      "Filesystem",
		Consistency:     "offline file-consistent copy",
		Compression:     "none",
		InventorySHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}
	if err := store.PutManifest(context.Background(), want); err != nil {
		t.Fatal(err)
	}

	got, err := store.Manifest(context.Background())
	if err != nil || got == nil || got.SourcePVCUID != want.SourcePVCUID ||
		got.SourcePVUID != want.SourcePVUID || got.SessionID != want.SessionID ||
		got.Version != ManifestVersionForTest {
		t.Fatalf("manifest = %#v, err=%v", got, err)
	}

	if err := store.PutManifest(context.Background(), want); err == nil {
		t.Fatal("second manifest publication succeeded")
	}
}

func TestIsMissingDistinguishesBucketAndObjectErrors(t *testing.T) {
	tests := []struct {
		name string
		code string
		want bool
	}{
		{name: "missing object", code: "NoSuchKey", want: true},
		{name: "generic not found object", code: "NotFound", want: true},
		{name: "missing bucket", code: "NoSuchBucket", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := &smithy.GenericAPIError{Code: test.code, Message: test.name}
			if got := IsMissingForTest(err); got != test.want {
				t.Fatalf("isMissing(%q)=%t, want %t", test.code, got, test.want)
			}
		})
	}
}

func TestMissingBucketErrorsRemainActionable(t *testing.T) {
	store := newTestStore(t, missingS3Bucket{})
	if _, err := store.Manifest(
		context.Background(),
	); err == nil ||
		!strings.Contains(err.Error(), `bucket "backups" does not exist`) {
		t.Fatalf("manifest missing bucket error=%v", err)
	}

	if _, err := store.AcquireLock(
		context.Background(),
		"holder",
		time.Minute,
	); err == nil ||
		!strings.Contains(err.Error(), `bucket "backups" does not exist`) {
		t.Fatalf("lock missing bucket error=%v", err)
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

func TestManifestReportsActionableTransportFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "DNS",
			err: &url.Error{Err: &net.DNSError{
				Err: "no such host", Name: "private.invalid",
			}},
			want: "S3 endpoint DNS resolution failed; verify the endpoint hostname and cluster DNS",
		},
		{
			name: "TLS",
			err: &url.Error{Err: &tls.CertificateVerificationError{
				Err: x509.UnknownAuthorityError{},
			}},
			want: "S3 endpoint TLS verification failed; verify the certificate chain and endpoint hostname",
		},
		{
			name: "connection",
			err: &url.Error{Err: &net.OpError{
				Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED,
			}},
			want: "S3 endpoint connection failed; verify the endpoint, port, network policy, and firewall",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t, getObjectErrorS3{API: newFakeS3(), err: test.err})

			_, err := store.Manifest(context.Background())
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("category=%s error=%v, want %q", domain.CategoryOf(err), err, test.want)
			}

			if strings.Contains(err.Error(), "private.invalid") {
				t.Fatalf("public error exposed endpoint from cause: %v", err)
			}
		})
	}
}

func TestManifestRejectsInvalidIdentity(t *testing.T) {
	client := newFakeS3()
	store := newTestStore(t, client)

	client.objects[store.ManifestKeyForTest()] = fakeObject{
		data: []byte(`{"version":1,"bucket":"other","name":"daily"}`),
		etag: "x",
	}
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
	lockObject := client.objects[store.LockKeyForTest()]
	lockObject.data = []byte(`{"holder":"first","expiresAt":"2000-01-01T00:00:00Z"}`)
	client.objects[store.LockKeyForTest()] = lockObject
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

func TestLockReportsMissingConditionalWriteCapability(t *testing.T) {
	store := newTestStore(t, conditionalWriteUnsupportedS3{API: newFakeS3()})

	_, err := store.AcquireLock(context.Background(), "holder", time.Minute)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(
			err.Error(),
			"verify PutObject If-None-Match support, encryption settings, and write permission",
		) ||
		!strings.Contains(err.Error(), "S3 code NotImplemented") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
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
	lockObject := client.objects[store.LockKeyForTest()]
	lockObject.etag = "new-etag"
	client.objects[store.LockKeyForTest()] = lockObject
	client.mu.Unlock()

	if category := domain.CategoryOf(
		store.ReleaseLock(context.Background(), first),
	); category != domain.ErrorConflict {
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

	valid := Config{
		Bucket:                "backups",
		Prefix:                "team/data",
		Name:                  "daily",
		Endpoint:              "http://s3.example",
		AllowInsecureEndpoint: true,
		ServerSideEncryption:  "aws:kms",
		SSEKMSKeyID:           "key-id",
		AccessKey:             "key",
		SecretKey:             "secret",
	}
	if err := ValidateConfig(valid); err != nil {
		t.Fatal(err)
	}

	for name, cfg := range map[string]Config{
		"bucket length": {Bucket: strings.Repeat("b", 64), Name: "daily"},
		"name length":   {Bucket: "backups", Name: strings.Repeat("n", 254)},
		"prefix length": {Bucket: "backups", Name: "daily", Prefix: strings.Repeat("p", 1025)},
		"complete key length": {
			Bucket: "backups",
			Prefix: strings.Repeat("p", 1010),
			Name:   "daily",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateConfig(cfg); err == nil {
				t.Fatalf("accepted oversized config: %#v", cfg)
			}
		})
	}
}

func TestValidatePathRejectsAbsoluteAndTraversalPaths(t *testing.T) {
	for _, path := range []string{"/data", "../data", "data/../other", "data\\child", "data//other", "data/", "."} {
		if err := ValidatePath(path); err == nil {
			t.Fatalf("accepted unsafe path %q", path)
		}
	}

	for _, path := range []string{"", "data", "data/nested-1", "tenant data/当前's files"} {
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
	client.objects["pv-migrate/other/file.txt"] = fakeObject{
		data: []byte("ignored"),
		etag: "etag-other",
	}

	inventory, err := store.Inventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if inventory.ObjectCount != 2 || inventory.TotalBytes != 6 || len(inventory.SHA256) != 64 {
		t.Fatalf("inventory=%+v", inventory)
	}

	manifest := Manifest{
		ObjectCount:     inventory.ObjectCount,
		TotalBytes:      inventory.TotalBytes,
		InventorySHA256: inventory.SHA256,
	}
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

func TestRcloneConfigUsesAmbientCredentialsWithoutSerializingThem(t *testing.T) {
	store, err := NewWithClient(newFakeS3(), Config{
		Bucket:                "backups",
		Name:                  "daily",
		UseAmbientCredentials: true,
	}, Credentials{AccessKey: "resolved-access", SecretKey: "resolved-secret"})
	if err != nil {
		t.Fatal(err)
	}

	config := store.RcloneConfig()
	if !strings.Contains(config, "env_auth = true\n") {
		t.Fatalf("ambient rclone config=%q", config)
	}

	if strings.Contains(config, "resolved-access") || strings.Contains(config, "resolved-secret") {
		t.Fatalf("ambient credentials were serialized into rclone config: %q", config)
	}
}
