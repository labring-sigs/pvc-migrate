package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	manifestVersion       = 2
	maxBucketLength       = 63
	maxObjectNameLength   = 253
	maxObjectPrefixLength = 1024
	maxObjectKeyLength    = 1024
)

var safeSegment = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

type Config struct {
	Bucket                string
	Prefix                string
	Name                  string
	Provider              string
	Endpoint              string
	Region                string
	AccessKey             string
	SecretKey             string
	SessionToken          string
	AllowInsecureEndpoint bool
	ForcePathStyle        bool
	ServerSideEncryption  string
	SSEKMSKeyID           string
	// UseAmbientCredentials keeps static controller credentials out of the
	// transfer Pod. The controller's S3 client still uses AccessKey/SecretKey,
	// while RcloneConfig emits env_auth for the Pod's bound ServiceAccount.
	UseAmbientCredentials bool
	// ServiceAccountName selects the pre-provisioned transfer Pod identity for
	// workload-identity profiles. It is controller metadata, not S3 wire data.
	ServiceAccountName string
	// ProfileUID and ProfileGeneration are controller-only provenance. They
	// pin a running workflow to the administrator profile without exposing
	// credentials or changing the object-store wire configuration.
	ProfileUID                string
	ProfileGeneration         int64
	CredentialsSecretUID      string
	ServiceAccountUID         string
	ServiceAccountFingerprint string
}

type Credentials struct {
	AccessKey    string
	SecretKey    string
	SessionToken string
}

type Manifest struct {
	Version         int       `json:"version"`
	CreatedAt       time.Time `json:"createdAt"`
	Bucket          string    `json:"bucket"`
	Prefix          string    `json:"prefix,omitempty"`
	Name            string    `json:"name"`
	SessionID       string    `json:"sessionID,omitempty"`
	Path            string    `json:"path,omitempty"`
	SourceNamespace string    `json:"sourceNamespace"`
	SourcePVC       string    `json:"sourcePVC"`
	SourcePVCUID    string    `json:"sourcePVCUID"`
	SourcePV        string    `json:"sourcePV,omitempty"`
	SourcePVUID     string    `json:"sourcePVUID,omitempty"`
	Capacity        string    `json:"capacity"`
	VolumeMode      string    `json:"volumeMode"`
	Consistency     string    `json:"consistency"`
	Compression     string    `json:"compression"`
	ObjectCount     int64     `json:"objectCount"`
	TotalBytes      int64     `json:"totalBytes"`
	InventorySHA256 string    `json:"inventorySHA256"`
}

type Inventory struct {
	ObjectCount int64  `json:"objectCount"`
	TotalBytes  int64  `json:"totalBytes"`
	SHA256      string `json:"sha256"`
}

type inventoryEntry struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	ETag string `json:"etag"`
}

type Lock struct {
	Holder    string    `json:"holder"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type API interface {
	HeadObject(
		ctx context.Context,
		params *s3.HeadObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.HeadObjectOutput, error)
	GetObject(
		ctx context.Context,
		params *s3.GetObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
	PutObject(
		ctx context.Context,
		params *s3.PutObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.PutObjectOutput, error)
	DeleteObject(
		ctx context.Context,
		params *s3.DeleteObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(
		ctx context.Context,
		params *s3.ListObjectsV2Input,
		optFns ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
}

type Store struct {
	client      API
	config      Config
	credentials Credentials
}

func (s *Store) Config() Config { return s.config }

func New(ctx context.Context, cfg Config) (*Store, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	loadOptions := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if cfg.AccessKey != "" {
		loadOptions = append(
			loadOptions,
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					cfg.AccessKey,
					cfg.SecretKey,
					cfg.SessionToken,
				),
			),
		)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorValidation,
			"S3 configuration",
			"load AWS configuration",
			err,
		)
	}

	resolved, err := awsCfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorPrecondition,
			"S3 credentials",
			"resolve credentials",
			err,
		)
	}

	client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.UsePathStyle = cfg.ForcePathStyle || cfg.Endpoint != ""
		if cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})

	return &Store{
		client: client,
		config: cfg,
		credentials: Credentials{
			AccessKey:    resolved.AccessKeyID,
			SecretKey:    resolved.SecretAccessKey,
			SessionToken: resolved.SessionToken,
		},
	}, nil
}

func NewWithClient(client API, cfg Config, resolved Credentials) (*Store, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	if client == nil {
		return nil, domain.NewError(domain.ErrorInternal, "S3 store", "client is required")
	}

	return &Store{client: client, config: cfg, credentials: resolved}, nil
}

// NewConfigOnly validates object-store routing fields without resolving
// credentials or constructing a network client. Controller-mode submission
// uses this form because the controller owns the administrator Secret and the
// submitting tenant must not receive or persist its contents.
func NewConfigOnly(cfg Config) (*Store, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	return &Store{config: cfg}, nil
}

func ValidateConfig(cfg Config) error {
	if len(cfg.Bucket) > maxBucketLength {
		return domain.NewError(
			domain.ErrorValidation,
			"S3 configuration",
			"bucket exceeds the maximum length of 63 characters",
		)
	}
	if !safeSegment.MatchString(cfg.Bucket) {
		return domain.NewError(
			domain.ErrorValidation,
			"S3 configuration",
			"bucket must contain only alphanumeric characters, dots, underscores, and hyphens",
		)
	}

	if len(cfg.Name) > maxObjectNameLength {
		return domain.NewError(
			domain.ErrorValidation,
			"S3 configuration",
			"name exceeds the maximum length of 253 characters",
		)
	}

	if !safeSegment.MatchString(cfg.Name) {
		return domain.NewError(
			domain.ErrorValidation,
			"S3 configuration",
			"name must contain only alphanumeric characters, dots, underscores, and hyphens",
		)
	}

	if len(cfg.Prefix) > maxObjectPrefixLength {
		return domain.NewError(
			domain.ErrorValidation,
			"S3 configuration",
			"prefix exceeds the maximum length of 1024 characters",
		)
	}

	if cfg.Prefix != "" {
		for segment := range strings.SplitSeq(cfg.Prefix, "/") {
			if !safeSegment.MatchString(segment) {
				return domain.NewError(
					domain.ErrorValidation,
					"S3 configuration",
					"prefix must contain slash-separated safe path segments",
				)
			}
		}
	}

	// S3 limits object keys, including the profile prefix, recovery-point
	// name, and controller-owned completion suffix, to 1024 bytes. Validate
	// the complete manifest key here so profile-backed workflows fail before
	// Helm or a transfer Pod is started.
	if len(path.Join(cfg.Prefix, cfg.Name+".complete.json")) > maxObjectKeyLength {
		return domain.NewError(
			domain.ErrorValidation,
			"S3 configuration",
			"prefix and recovery-point name exceed S3's 1024-byte object-key limit",
		)
	}

	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return domain.NewError(
			domain.ErrorValidation,
			"S3 credentials",
			"access key and secret key must be supplied together",
		)
	}

	for name, value := range map[string]string{
		"provider": cfg.Provider, "endpoint": cfg.Endpoint, "region": cfg.Region,
		"access key": cfg.AccessKey, "secret key": cfg.SecretKey, "session token": cfg.SessionToken,
		"SSE KMS key ID": cfg.SSEKMSKeyID,
	} {
		if strings.ContainsAny(value, "\r\n\x00") || strings.TrimSpace(value) != value {
			return domain.NewError(
				domain.ErrorValidation,
				"S3 configuration",
				name+" contains unsafe whitespace",
			)
		}
	}

	if cfg.Endpoint != "" {
		parsed, err := url.Parse(cfg.Endpoint)
		if err != nil || parsed.Host == "" ||
			(parsed.Scheme != "https" && parsed.Scheme != "http") {
			return domain.NewError(
				domain.ErrorValidation,
				"S3 configuration",
				"endpoint must be an absolute HTTP or HTTPS URL",
			)
		}
		if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" {
			return domain.NewError(
				domain.ErrorValidation,
				"S3 configuration",
				"endpoint must not contain credentials, query parameters, fragments, or a path",
			)
		}

		if parsed.Scheme == "http" && !cfg.AllowInsecureEndpoint {
			return domain.NewError(
				domain.ErrorPrecondition,
				"S3 configuration",
				"HTTP endpoint requires --allow-insecure-endpoint",
			)
		}
	}

	if cfg.SSEKMSKeyID != "" && cfg.ServerSideEncryption != "aws:kms" {
		return domain.NewError(
			domain.ErrorValidation,
			"S3 encryption",
			"SSE KMS key ID requires --s3-server-side-encryption=aws:kms",
		)
	}

	switch cfg.ServerSideEncryption {
	case "", "AES256", "aws:kms":
	default:
		return domain.NewError(
			domain.ErrorValidation,
			"S3 encryption",
			"server-side encryption must be AES256 or aws:kms",
		)
	}

	return nil
}

// ValidatePath accepts a canonical relative slash-separated PVC subdirectory.
// The empty string is the canonical full-volume root used by backup manifests.
func ValidatePath(value string) error {
	if value == "" {
		return nil
	}

	normalized, err := domain.NormalizeTransferPath(value)
	if err != nil {
		return domain.WrapError(
			domain.ErrorValidation,
			"PVC path",
			fmt.Sprintf("path %q is invalid", value),
			err,
		)
	}

	if normalized == domain.VolumeRootPath || normalized != value {
		return domain.NewError(
			domain.ErrorValidation,
			"PVC path",
			"path must be stored in normalized form; use an empty path for the PVC root",
		)
	}

	return nil
}

func (s *Store) Credentials() Credentials { return s.credentials }

func (s *Store) RemotePath() string {
	return "remote:" + path.Join(s.config.Bucket, s.config.Prefix, s.config.Name) + "/"
}

func (s *Store) Destination() string {
	segments := []string{s.config.Bucket}
	if s.config.Prefix != "" {
		segments = append(segments, s.config.Prefix)
	}

	segments = append(segments, s.config.Name)

	return "s3://" + strings.Join(segments, "/") + "/"
}

func (s *Store) RcloneConfig() string {
	provider := s.config.Provider
	if provider == "" {
		provider = "AWS"
		if s.config.Endpoint != "" {
			// Rclone's AWS provider applies AWS-specific behavior that can
			// reject compatible services. Generic endpoint URLs use the
			// provider-neutral S3 mode unless the caller selects a dialect.
			provider = "Other"
		}
	}

	var builder strings.Builder
	builder.WriteString("[remote]\n")
	builder.WriteString("type = s3\n")
	fmt.Fprintf(&builder, "provider = %s\n", provider)
	if s.config.UseAmbientCredentials {
		builder.WriteString("env_auth = true\n")
	} else {
		fmt.Fprintf(&builder, "access_key_id = %s\n", s.credentials.AccessKey)
		fmt.Fprintf(&builder, "secret_access_key = %s\n", s.credentials.SecretKey)

		if s.credentials.SessionToken != "" {
			fmt.Fprintf(&builder, "session_token = %s\n", s.credentials.SessionToken)
		}
	}

	if s.config.Endpoint != "" {
		fmt.Fprintf(&builder, "endpoint = %s\n", s.config.Endpoint)
		builder.WriteString("force_path_style = true\n")
	}

	if s.config.Region != "" {
		fmt.Fprintf(&builder, "region = %s\n", s.config.Region)
	}

	if s.config.ServerSideEncryption != "" {
		fmt.Fprintf(&builder, "server_side_encryption = %s\n", s.config.ServerSideEncryption)
	}

	if s.config.SSEKMSKeyID != "" {
		fmt.Fprintf(&builder, "sse_kms_key_id = %s\n", s.config.SSEKMSKeyID)
	}

	builder.WriteString("no_check_bucket = true\n")

	return builder.String()
}

func (s *Store) Manifest(ctx context.Context) (*Manifest, error) {
	if s == nil || s.client == nil {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"object-store client is not configured",
		)
	}

	output, err := s.client.GetObject(
		ctx,
		&s3.GetObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(s.manifestKey())},
	)
	if err != nil {
		if isNoSuchBucket(err) {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"S3 manifest",
				fmt.Sprintf("bucket %q does not exist", s.config.Bucket),
			)
		}

		if isMissing(err) {
			return nil, nil
		}

		return nil, wrapS3Error(
			ctx,
			domain.ErrorPrecondition,
			"S3 manifest",
			"read completion manifest",
			err,
		)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(io.LimitReader(output.Body, 1<<20))
	if err != nil {
		return nil, wrapS3Error(
			ctx,
			domain.ErrorPrecondition,
			"S3 manifest",
			"read completion manifest body",
			err,
		)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, domain.WrapError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"decode completion manifest",
			err,
		)
	}

	if manifest.Version != manifestVersion || manifest.Bucket != s.config.Bucket ||
		manifest.Prefix != s.config.Prefix ||
		manifest.Name != s.config.Name {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"S3 manifest",
			"completion manifest identity is invalid",
		)
	}

	if err := validateManifest(manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

func (s *Store) PutManifest(ctx context.Context, manifest Manifest) error {
	manifest.Version = manifestVersion
	manifest.Bucket = s.config.Bucket
	manifest.Prefix = s.config.Prefix

	manifest.Name = s.config.Name
	if err := validateManifest(manifest); err != nil {
		return err
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		return domain.WrapError(
			domain.ErrorInternal,
			"S3 manifest",
			"encode completion manifest",
			err,
		)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.config.Bucket),
		Key:         aws.String(s.manifestKey()),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
		IfNoneMatch: aws.String("*"),
	}
	s.applyEncryption(input)

	if _, err := s.client.PutObject(ctx, input); err != nil {
		return wrapS3Error(
			ctx,
			domain.ErrorConflict,
			"S3 manifest",
			"publish immutable completion manifest",
			err,
		)
	}

	return nil
}

func (s *Store) Inventory(ctx context.Context) (Inventory, error) {
	prefix := path.Join(s.config.Prefix, s.config.Name) + "/"
	entries := make([]inventoryEntry, 0)

	var continuationToken *string
	for {
		output, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.config.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return Inventory{}, wrapS3Error(
				ctx,
				domain.ErrorPrecondition,
				"S3 inventory",
				"list backup objects",
				err,
			)
		}

		for _, object := range output.Contents {
			key := aws.ToString(object.Key)
			if !strings.HasPrefix(key, prefix) || key == prefix {
				continue
			}

			etag := strings.TrimSpace(aws.ToString(object.ETag))
			if etag == "" {
				return Inventory{}, domain.NewError(
					domain.ErrorPrecondition,
					"S3 inventory",
					fmt.Sprintf("object %s has no ETag", key),
				)
			}

			size := aws.ToInt64(object.Size)
			if size < 0 {
				return Inventory{}, domain.NewError(
					domain.ErrorPrecondition,
					"S3 inventory",
					fmt.Sprintf("object %s has a negative size", key),
				)
			}

			entries = append(
				entries,
				inventoryEntry{Key: strings.TrimPrefix(key, prefix), Size: size, ETag: etag},
			)
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}

		if output.NextContinuationToken == nil || aws.ToString(output.NextContinuationToken) == "" {
			return Inventory{}, domain.NewError(
				domain.ErrorPrecondition,
				"S3 inventory",
				"truncated object listing has no continuation token",
			)
		}

		continuationToken = output.NextContinuationToken
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	hasher := sha256.New()
	encoder := json.NewEncoder(hasher)

	var totalBytes int64
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			return Inventory{}, domain.WrapError(
				domain.ErrorInternal,
				"S3 inventory",
				"encode inventory entry",
				err,
			)
		}

		totalBytes += entry.Size
		if totalBytes < 0 {
			return Inventory{}, domain.NewError(
				domain.ErrorPrecondition,
				"S3 inventory",
				"backup object size total overflowed",
			)
		}
	}

	return Inventory{
		ObjectCount: int64(len(entries)),
		TotalBytes:  totalBytes,
		SHA256:      hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func (s *Store) VerifyInventory(ctx context.Context, manifest Manifest) error {
	current, err := s.Inventory(ctx)
	if err != nil {
		return err
	}

	if current.ObjectCount != manifest.ObjectCount || current.TotalBytes != manifest.TotalBytes ||
		current.SHA256 != manifest.InventorySHA256 {
		return domain.NewError(
			domain.ErrorConflict,
			"S3 inventory",
			fmt.Sprintf(
				"published backup objects changed: count=%d/%d bytes=%d/%d digest=%s/%s",
				current.ObjectCount,
				manifest.ObjectCount,
				current.TotalBytes,
				manifest.TotalBytes,
				current.SHA256,
				manifest.InventorySHA256,
			),
		)
	}

	return nil
}

func (s *Store) AcquireLock(ctx context.Context, holder string, ttl time.Duration) (string, error) {
	if holder == "" || ttl <= 0 {
		return "", domain.NewError(
			domain.ErrorValidation,
			"S3 lock",
			"holder and positive TTL are required",
		)
	}

	lock := Lock{Holder: holder, ExpiresAt: time.Now().UTC().Add(ttl)}

	data, err := json.Marshal(lock)
	if err != nil {
		return "", domain.WrapError(
			domain.ErrorInternal,
			"S3 lock",
			"encode lock before acquisition",
			err,
		)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.config.Bucket),
		Key:         aws.String(s.lockKey()),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
		IfNoneMatch: aws.String("*"),
	}
	s.applyEncryption(input)

	output, err := s.client.PutObject(ctx, input)
	if err == nil {
		etag := aws.ToString(output.ETag)
		if etag == "" {
			return "", domain.NewError(
				domain.ErrorConflict,
				"S3 lock",
				"S3 did not return an ETag for the newly acquired lock",
			)
		}

		return etag, nil
	}

	current, etag, getErr := s.readLock(ctx)
	if getErr != nil {
		if isNoSuchBucket(getErr) {
			return "", domain.NewError(
				domain.ErrorPrecondition,
				"S3 lock",
				fmt.Sprintf("bucket %q does not exist", s.config.Bucket),
			)
		}

		return "", wrapS3Error(ctx, domain.ErrorConflict, "S3 lock", "acquire backup lock", getErr)
	}

	if current == nil {
		return "", wrapS3Error(
			ctx,
			domain.ErrorPrecondition,
			"S3 lock",
			"backend rejected atomic lock creation; verify PutObject If-None-Match support, encryption settings, and write permission",
			err,
		)
	}

	if time.Now().UTC().Before(current.ExpiresAt) {
		return "", domain.NewError(
			domain.ErrorConflict,
			"S3 lock",
			"backup recovery point is locked by "+current.Holder,
		)
	}

	input.Body = bytes.NewReader(data)
	input.IfNoneMatch = nil
	input.IfMatch = aws.String(etag)

	output, err = s.client.PutObject(ctx, input)
	if err != nil {
		return "", wrapS3Error(
			ctx,
			domain.ErrorConflict,
			"S3 lock",
			"replace expired backup lock",
			err,
		)
	}

	newETag := aws.ToString(output.ETag)
	if newETag == "" {
		return "", domain.NewError(
			domain.ErrorConflict,
			"S3 lock",
			"S3 did not return an ETag for the replaced lock",
		)
	}

	return newETag, nil
}

func (s *Store) ReleaseLock(ctx context.Context, etag string) error {
	if etag == "" {
		return domain.NewError(
			domain.ErrorConflict,
			"S3 lock",
			"lock ETag is required for conditional release",
		)
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(s.lockKey()),
	}

	input.IfMatch = aws.String(etag)
	if _, err := s.client.DeleteObject(ctx, input); err != nil && !isMissing(err) {
		return wrapS3Error(ctx, domain.ErrorConflict, "S3 lock", "release backup lock", err)
	}

	return nil
}

// RenewLock extends a lock only when the caller still owns the exact object
// version returned by AcquireLock or a previous RenewLock call.
func (s *Store) RenewLock(
	ctx context.Context,
	holder, etag string,
	ttl time.Duration,
) (string, error) {
	if holder == "" || etag == "" || ttl <= 0 {
		return "", domain.NewError(
			domain.ErrorValidation,
			"S3 lock",
			"holder, ETag, and positive TTL are required",
		)
	}

	current, currentETag, err := s.readLock(ctx)
	if err != nil {
		return "", wrapS3Error(
			ctx,
			domain.ErrorConflict,
			"S3 lock",
			"read lock before renewal",
			err,
		)
	}

	if current == nil || current.Holder != holder || currentETag != etag {
		return "", domain.NewError(
			domain.ErrorConflict,
			"S3 lock",
			"lock ownership changed before renewal",
		)
	}

	data, err := json.Marshal(Lock{Holder: holder, ExpiresAt: time.Now().UTC().Add(ttl)})
	if err != nil {
		return "", domain.WrapError(
			domain.ErrorInternal,
			"S3 lock",
			"encode lock before renewal",
			err,
		)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.config.Bucket),
		Key:         aws.String(s.lockKey()),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
		IfMatch:     aws.String(currentETag),
	}
	s.applyEncryption(input)

	output, err := s.client.PutObject(ctx, input)
	if err != nil {
		return "", wrapS3Error(ctx, domain.ErrorConflict, "S3 lock", "renew backup lock", err)
	}

	newETag := aws.ToString(output.ETag)
	if newETag == "" {
		return "", domain.NewError(
			domain.ErrorConflict,
			"S3 lock",
			"S3 did not return an ETag for the renewed lock",
		)
	}

	return newETag, nil
}

func validateManifest(manifest Manifest) error {
	if manifest.Version != manifestVersion {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"unsupported completion manifest version",
		)
	}

	if manifest.CreatedAt.IsZero() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"completion manifest createdAt is required",
		)
	}

	if manifest.SourceNamespace == "" || manifest.SourcePVC == "" || manifest.SourcePVCUID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"completion manifest source identity is incomplete",
		)
	}

	if (manifest.SourcePV == "") != (manifest.SourcePVUID == "") {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"completion manifest source PV identity is incomplete",
		)
	}

	if manifest.Capacity == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"completion manifest capacity is required",
		)
	}

	capacity, err := resource.ParseQuantity(manifest.Capacity)
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"parse completion manifest capacity",
			err,
		)
	}

	if capacity.Sign() <= 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"completion manifest capacity must be positive",
		)
	}

	if manifest.VolumeMode == "" || manifest.Consistency == "" || manifest.Compression == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"completion manifest storage and consistency fields are required",
		)
	}

	if manifest.VolumeMode != "Filesystem" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"S3 file backup manifest must use Filesystem VolumeMode",
		)
	}

	if manifest.ObjectCount < 0 || manifest.TotalBytes < 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"completion manifest inventory counts must be non-negative",
		)
	}

	if len(manifest.InventorySHA256) != sha256.Size*2 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"completion manifest inventory SHA-256 is required",
		)
	}

	if _, err := hex.DecodeString(manifest.InventorySHA256); err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"decode completion manifest inventory SHA-256",
			err,
		)
	}

	if err := ValidatePath(manifest.Path); err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"S3 manifest",
			"validate completion manifest path",
			err,
		)
	}

	return nil
}

func (s *Store) manifestKey() string {
	return path.Join(s.config.Prefix, s.config.Name+".complete.json")
}
func (s *Store) lockKey() string { return path.Join(s.config.Prefix, s.config.Name+".lock.json") }

func (s *Store) readLock(ctx context.Context) (*Lock, string, error) {
	output, err := s.client.GetObject(
		ctx,
		&s3.GetObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(s.lockKey())},
	)
	if err != nil {
		if isMissing(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	defer output.Body.Close()

	data, err := io.ReadAll(io.LimitReader(output.Body, 64<<10))
	if err != nil {
		return nil, "", err
	}

	var lock Lock
	if err := json.Unmarshal(data, &lock); err != nil {
		return nil, "", err
	}

	return &lock, aws.ToString(output.ETag), nil
}

func (s *Store) applyEncryption(input *s3.PutObjectInput) {
	if s.config.ServerSideEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(s.config.ServerSideEncryption)
	}

	if s.config.SSEKMSKeyID != "" {
		input.SSEKMSKeyId = aws.String(s.config.SSEKMSKeyID)
	}
}

func wrapS3Error(
	ctx context.Context,
	category domain.ErrorCategory,
	operation, message string,
	err error,
) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(ctx.Err(), context.Canceled) {
		return domain.WrapError(
			domain.ErrorTimeout,
			operation,
			message+": operation timed out",
			err,
		)
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() != "" {
		message += fmt.Sprintf(" (S3 code %s)", apiErr.ErrorCode())
	}

	return domain.WrapError(category, operation, message, err)
}

func isMissing(err error) bool {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchObject":
			return true
		case "NoSuchBucket":
			// A missing bucket is a configuration or provisioning error that callers
			// need to distinguish from an absent object.
			return false
		}
	}

	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}

	return false
}

func isNoSuchBucket(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchBucket"
}

func LockHolder(operationID string) string {
	digest := sha256.Sum256([]byte(operationID))
	return "pvc-migrate-" + hex.EncodeToString(digest[:8])
}
