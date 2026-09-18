package cli

import (
	"context"
	"os"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type s3CredentialFlags struct {
	accessKey, secretKey, sessionToken                         string
	secretName, accessKeyKey, secretKeyKey, sessionTokenKey    string
	accessKeyExplicit, secretKeyExplicit, sessionTokenExplicit bool
}

type s3RepositoryFlags struct {
	spec        v1alpha1.S3BackupRepositorySpec
	credentials s3CredentialFlags
	backend     string
}

func bindRepositoryFlags(
	command *cobra.Command,
	flags *s3RepositoryFlags,
	reference *string,
	submit bool,
) {
	f := command.Flags()
	f.StringVar(
		reference,
		"backup-repository",
		"",
		"BackupRepository in the workflow namespace supplying the S3 location and credentials",
	)
	// Both backends validate the object-storage backend; only s3 is supported.
	f.StringVar(&flags.backend, "backend", "s3", "S3-compatible object backend")

	if submit {
		if err := command.MarkFlagRequired("backup-repository"); err != nil {
			panic(err)
		}

		// Controller submission takes location and credentials from the
		// BackupRepository object; inline S3 parameters do not apply.
		for _, name := range []string{
			"bucket", "prefix", "s3-provider", "endpoint", "region",
			"access-key", "secret-key", "session-token", "credentials-secret",
			"access-key-key", "secret-key-key", "session-token-key",
			"allow-insecure-endpoint", "s3-server-side-encryption", "s3-sse-kms-key-id",
		} {
			_ = command.Flags().MarkHidden(name)
		}

		return
	}

	// Session runs build the repository from inline parameters; the
	// BackupRepository reference does not apply.
	_ = command.Flags().MarkHidden("backup-repository")

	f.StringVar(&flags.spec.Bucket, "bucket", "", "Backup bucket")
	f.StringVar(
		&flags.spec.Prefix,
		"prefix",
		"pv-migrate",
		"Object-store prefix holding the recovery points",
	)
	f.StringVar(&flags.spec.Provider, "s3-provider", "", "rclone S3 provider")
	f.StringVar(&flags.spec.Endpoint, "endpoint", "", "S3 endpoint")
	f.StringVar(&flags.spec.Region, "region", "", "S3 region")
	f.BoolVar(
		&flags.spec.AllowInsecureEndpoint,
		"allow-insecure-endpoint",
		false,
		"Allow an HTTP S3 endpoint",
	)
	f.StringVar(
		&flags.spec.ServerSideEncryption,
		"s3-server-side-encryption",
		"",
		"S3 server-side encryption: AES256 or aws:kms",
	)
	f.StringVar(&flags.spec.SSEKMSKeyID, "s3-sse-kms-key-id", "", "S3 KMS key ID")
	c := &flags.credentials
	f.StringVar(
		&c.accessKey,
		"access-key",
		os.Getenv("AWS_ACCESS_KEY_ID"),
		"S3 access key; defaults to AWS_ACCESS_KEY_ID",
	)
	f.StringVar(
		&c.secretKey,
		"secret-key",
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"S3 secret key; defaults to AWS_SECRET_ACCESS_KEY",
	)
	f.StringVar(
		&c.sessionToken,
		"session-token",
		os.Getenv("AWS_SESSION_TOKEN"),
		"S3 session token; defaults to AWS_SESSION_TOKEN",
	)
	f.StringVar(
		&c.secretName,
		"credentials-secret",
		"",
		"Secret whose accessKey/secretKey/sessionToken fields supply the S3 credentials for this workflow",
	)
	f.StringVar(
		&c.accessKeyKey,
		"access-key-key",
		"accessKey",
		"Access key field in --credentials-secret",
	)
	f.StringVar(
		&c.secretKeyKey,
		"secret-key-key",
		"secretKey",
		"Secret key field in --credentials-secret",
	)
	f.StringVar(
		&c.sessionTokenKey,
		"session-token-key",
		"sessionToken",
		"Session token field in --credentials-secret",
	)
}

func validateRepositoryFlags(
	command *cobra.Command,
	flags *s3RepositoryFlags,
	namespace, reference string,
	requireReference bool,
) error {
	if flags.backend != "s3" {
		return domain.NewError(
			domain.ErrorValidation,
			"repository",
			"unsupported backend: only s3 is supported",
		)
	}

	if reference != "" {
		// Controller submission (backup create / restore create) takes the
		// S3 location and credentials from a BackupRepository object.
		for _, name := range []string{"bucket", "prefix", "s3-provider", "endpoint", "region", "access-key", "secret-key", "session-token", "credentials-secret", "allow-insecure-endpoint", "s3-server-side-encryption", "s3-sse-kms-key-id"} {
			if command.Flags().Changed(name) {
				return domain.NewError(
					domain.ErrorValidation,
					"repository",
					"controller workflows take provider, endpoint, region, and credentials from BackupRepository",
				)
			}
		}

		return nil
	}

	if requireReference {
		return domain.NewError(
			domain.ErrorValidation,
			"repository",
			"controller workflows require --backup-repository",
		)
	}

	// Session workflows build their repository from inline S3 flags; they
	// never reference (or create) BackupRepository objects.
	return nil
}

func loadS3Credentials(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	flags *s3CredentialFlags,
) error {
	if flags.secretName == "" {
		return nil
	}

	secret, err := client.CoreV1().
		Secrets(namespace).
		Get(ctx, flags.secretName, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"S3 credentials",
			"read credentials Secret",
			err,
		)
	}

	read := func(key string) string {
		if value := secret.Data[key]; len(value) != 0 {
			return string(value)
		}
		return secret.StringData[key]
	}
	if !flags.accessKeyExplicit {
		if value := read(flags.accessKeyKey); value != "" {
			flags.accessKey = value
		}
	}

	if !flags.secretKeyExplicit {
		if value := read(flags.secretKeyKey); value != "" {
			flags.secretKey = value
		}
	}

	if !flags.sessionTokenExplicit {
		if value := read(flags.sessionTokenKey); value != "" {
			flags.sessionToken = value
		}
	}

	return nil
}

func prepareInlineRepository(
	ctx context.Context,
	cmd *cobra.Command,
	client kubernetes.Interface,
	namespace, id string,
	flags *s3RepositoryFlags,
) (*v1alpha1.BackupRepository, map[string][]byte, error) {
	c := &flags.credentials

	c.accessKeyExplicit, c.secretKeyExplicit, c.sessionTokenExplicit = cmd.Flags().
		Changed("access-key"),
		cmd.Flags().
			Changed("secret-key"),
		cmd.Flags().
			Changed("session-token")
	if err := loadS3Credentials(ctx, client, namespace, c); err != nil {
		return nil, nil, err
	}

	data := map[string][]byte{
		kube.BackupAccessKeyDataKey:    []byte(c.accessKey),
		kube.BackupSecretKeyDataKey:    []byte(c.secretKey),
		kube.BackupSessionTokenDataKey: []byte(c.sessionToken),
	}
	if err := kube.ValidateS3CredentialsData(data); err != nil {
		return nil, nil, err
	}

	spec := flags.spec.DeepCopy()
	spec.ForcePathStyle = spec.Endpoint != ""
	spec.CredentialsSecret.Name = kube.BackupCredentialsSecretName(id)

	return &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "session-" + id},
		Spec:       v1alpha1.BackupRepositorySpec{Type: v1alpha1.BackupRepositoryTypeS3, S3: spec},
	}, data, nil
}

// loadRestoreRepositoryConnection rebuilds the object-store connection for a
// persisted restore session from its recorded spec and credentials Secret.
func (r *rootState) loadRestoreRepositoryConnection(
	ctx context.Context,
	runtime *commandRuntime,
	object *v1alpha1.Restore,
) (*objectstore.Store, error) {
	secretName := kube.BackupCredentialsSecretName(object.Name)

	secret, err := runtime.clients.Kubernetes.CoreV1().Secrets(object.Namespace).
		Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"restore session",
			"read session credentials Secret "+secretName,
			err,
		)
	}

	// The session persisted its inline BackupRepository alongside its
	// credentials; reload it to rebuild the connection.
	store := kube.NewConfigMapRepositoryStore(runtime.clients.Kubernetes)

	repository, err := store.Load(ctx, crclient.ObjectKey{
		Namespace: object.Namespace,
		Name:      object.Spec.RepositoryRef.Name,
	})
	if err != nil {
		return nil, err
	}

	config, err := backup.S3RepositoryLocation(
		repository,
		object.Spec.Name,
	)
	if err != nil {
		return nil, err
	}

	config.AccessKey = string(secret.Data[kube.BackupAccessKeyDataKey])
	config.SecretKey = string(secret.Data[kube.BackupSecretKeyDataKey])
	config.SessionToken = string(secret.Data[kube.BackupSessionTokenDataKey])

	if r.options.objectStoreFactory != nil {
		return r.options.objectStoreFactory(ctx, config)
	}

	return objectstore.New(ctx, config)
}

func (r *rootState) inlineRepositoryConnection(
	ctx context.Context,
	runtime *commandRuntime,
	repository *v1alpha1.BackupRepository,
	name string,
	data map[string][]byte,
) (*objectstore.Store, error) {
	config, err := backup.S3RepositoryLocation(repository, name)
	if err != nil {
		return nil, err
	}

	config.AccessKey, config.SecretKey, config.SessionToken = string(
		data[kube.BackupAccessKeyDataKey],
	), string(
		data[kube.BackupSecretKeyDataKey],
	), string(
		data[kube.BackupSessionTokenDataKey],
	)
	if r.options.objectStoreFactory != nil {
		return r.options.objectStoreFactory(ctx, config)
	}

	return objectstore.New(ctx, config)
}

func (r *rootState) newControllerRepositoryStore(
	ctx context.Context,
	runtime *commandRuntime,
	namespace, repositoryName, name string,
) (*objectstore.Store, error) {
	repository := &v1alpha1.BackupRepository{}
	if err := runtime.clients.Runtime.Get(
		ctx,
		crclient.ObjectKey{Namespace: namespace, Name: repositoryName},
		repository,
	); err != nil {
		return nil, err
	}

	config, err := backup.S3RepositoryLocation(repository, name)
	if err != nil {
		return nil, err
	}

	return objectstore.NewConfigOnly(config)
}
