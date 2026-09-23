package backup

import (
	"context"
	"fmt"
	"path"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// ResolveS3Repository reads the referenced API repository and its credentials.
// Connection details stay in the data plane; resource identities are returned
// separately for the operation to checkpoint before running a transfer.
func ResolveS3Repository(
	ctx context.Context,
	reader crclient.Reader,
	client kubernetes.Interface,
	key crclient.ObjectKey,
	name string,
) (objectstore.Config, *v1alpha1.BackupRepositoryBindingStatus, error) {
	if reader == nil {
		return objectstore.Config{}, nil, domain.NewError(
			domain.ErrorKubernetes, "backup repository", "repository reader is not configured",
		)
	}

	return resolveStoredS3Repository(
		ctx,
		func(ctx context.Context, key crclient.ObjectKey) (*v1alpha1.BackupRepository, error) {
			repository := &v1alpha1.BackupRepository{}
			err := reader.Get(ctx, key, repository)
			return repository, err
		},
		client,
		key,
		name,
	)
}

type RepositoryLoader func(context.Context, crclient.ObjectKey) (*v1alpha1.BackupRepository, error)

func resolveStoredS3Repository(
	ctx context.Context,
	load RepositoryLoader,
	client kubernetes.Interface,
	key crclient.ObjectKey,
	name string,
) (objectstore.Config, *v1alpha1.BackupRepositoryBindingStatus, error) {
	if load == nil {
		return objectstore.Config{}, nil, domain.NewError(
			domain.ErrorInternal,
			"backup repository",
			"repository loader is required",
		)
	}

	if key.Namespace == "" || key.Name == "" {
		return objectstore.Config{}, nil, domain.NewError(
			domain.ErrorValidation,
			"backup repository",
			"repository namespace and name are required",
		)
	}

	repository, err := load(ctx, key)
	if err != nil {
		category := domain.ErrorKubernetes
		if apierrors.IsNotFound(err) {
			category = domain.ErrorPrecondition
		}

		return objectstore.Config{}, nil, domain.WrapError(
			category, "backup repository", "read BackupRepository "+key.String(), err,
		)
	}

	if repository == nil || repository.Name != key.Name || repository.Namespace != key.Namespace {
		return objectstore.Config{}, nil, domain.NewError(
			domain.ErrorConflict,
			"backup repository",
			"loaded repository differs from the requested identity",
		)
	}

	config, err := S3RepositoryLocation(repository, name)
	if err != nil {
		return objectstore.Config{}, nil, err
	}

	if repository.UID == "" || repository.Generation < 1 {
		return objectstore.Config{}, nil, domain.NewError(
			domain.ErrorPrecondition,
			"backup repository",
			"repository UID and generation are required",
		)
	}

	if client == nil {
		return objectstore.Config{}, nil, domain.NewError(
			domain.ErrorKubernetes, "backup repository", "Kubernetes client is not configured",
		)
	}

	secretName := repository.Spec.S3.CredentialsSecret.Name
	if secretName == "" {
		return objectstore.Config{}, nil, domain.NewError(
			domain.ErrorPrecondition,
			"backup repository",
			"BackupRepository credentialsSecret is required",
		)
	}

	secret, err := client.CoreV1().Secrets(key.Namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		category := domain.ErrorKubernetes
		if apierrors.IsNotFound(err) {
			category = domain.ErrorPrecondition
		}

		return objectstore.Config{}, nil, domain.WrapError(
			category, "backup repository", "read BackupRepository credentials Secret", err,
		)
	}

	if secret.DeletionTimestamp != nil {
		return objectstore.Config{}, nil, domain.NewError(
			domain.ErrorPrecondition,
			"backup repository",
			"BackupRepository credentials Secret is being deleted",
		)
	}

	if err := kube.ValidateS3CredentialsData(secret.Data); err != nil {
		return objectstore.Config{}, nil, domain.WrapError(
			domain.ErrorPrecondition,
			"backup repository",
			"BackupRepository credentials Secret is invalid",
			err,
		)
	}

	binding := &v1alpha1.BackupRepositoryBindingStatus{
		Type:       v1alpha1.BackupRepositoryTypeS3,
		UID:        repository.UID,
		Generation: repository.Generation,
		S3:         &v1alpha1.S3BackupRepositoryBindingStatus{CredentialsSecretUID: secret.UID},
	}
	if err := validateRepositoryBinding(binding); err != nil {
		return objectstore.Config{}, nil, err
	}

	config.AccessKey = string(secret.Data[kube.BackupAccessKeyDataKey])
	config.SecretKey = string(secret.Data[kube.BackupSecretKeyDataKey])
	config.SessionToken = string(secret.Data[kube.BackupSessionTokenDataKey])

	return config, binding, nil
}

// S3RepositoryLocation resolves routing without reading credentials, allowing
// submission previews to use the same namespace isolation as execution.
func S3RepositoryLocation(
	repository *v1alpha1.BackupRepository,
	name string,
) (objectstore.Config, error) {
	if repository == nil {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorValidation, "backup repository", "BackupRepository is required",
		)
	}

	if repository.DeletionTimestamp != nil {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorPrecondition, "backup repository", "BackupRepository is being deleted",
		)
	}

	if repository.Spec.Type != v1alpha1.BackupRepositoryTypeS3 || repository.Spec.S3 == nil {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorValidation,
			"backup repository",
			"BackupRepository requires type s3 with only an s3 configuration",
		)
	}

	spec := repository.Spec.S3

	config := objectstore.Config{
		Name:                  name,
		Bucket:                spec.Bucket,
		Prefix:                spec.Prefix,
		Provider:              spec.Provider,
		Endpoint:              spec.Endpoint,
		Region:                spec.Region,
		AllowInsecureEndpoint: spec.AllowInsecureEndpoint,
		ForcePathStyle:        spec.ForcePathStyle || spec.Endpoint != "",
		ServerSideEncryption:  spec.ServerSideEncryption,
		SSEKMSKeyID:           spec.SSEKMSKeyID,
	}
	if err := objectstore.ValidateConfig(config); err != nil {
		return objectstore.Config{}, domain.WrapError(
			domain.ErrorValidation, "backup repository", "validate BackupRepository location", err,
		)
	}

	// Recovery points live directly under the configured prefix; the
	// recovery-point name (config.Name) is the only per-workflow segment
	// appended by the data plane.
	config.Prefix = path.Clean("/" + spec.Prefix)[1:]

	if err := objectstore.ValidateConfig(config); err != nil {
		return objectstore.Config{}, domain.WrapError(
			domain.ErrorValidation,
			"backup repository",
			"validate scoped BackupRepository location",
			err,
		)
	}

	return config, nil
}

// PinRepository records the repository object identity for the lifetime
// of a running workflow. Location changes take effect only for a new workflow;
// Secret data may rotate in place; replacing the referenced Secret changes its
// identity and requires a new workflow.
func PinRepository(
	ctx context.Context,
	requested *v1alpha1.BackupRepositoryBindingStatus,
	pinned **v1alpha1.BackupRepositoryBindingStatus,
	persist func(context.Context) error,
) error {
	if pinned == nil || persist == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"backup repository",
			"binding checkpoint destination and writer are required",
		)
	}

	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := validateRepositoryMatch(requested, *pinned); err != nil {
		return err
	}

	if *pinned == nil {
		*pinned = requested.DeepCopy()
		if err := persist(ctx); err != nil {
			*pinned = nil
			return err
		}

		return kube.LeaseFenceError(ctx)
	}

	return nil
}

func validateRepositoryMatch(requested, current *v1alpha1.BackupRepositoryBindingStatus) error {
	if requested == nil || requested.Type == "" || requested.UID == "" ||
		requested.Generation <= 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup repository",
			"resolved BackupRepository type, UID, and generation are required",
		)
	}

	if err := validateRepositoryBinding(requested); err != nil {
		return err
	}

	if current == nil {
		return nil
	}

	if current.Type != requested.Type {
		return repositoryBindingConflict(
			"BackupRepository backend changed while the workflow was running",
		)
	}

	if current.UID != requested.UID {
		return repositoryBindingConflict(
			"BackupRepository was replaced while the workflow was running",
		)
	}

	if current.Generation != requested.Generation {
		return repositoryBindingConflict("BackupRepository changed while the workflow was running")
	}

	switch requested.Type {
	case v1alpha1.BackupRepositoryTypeS3:
		if current.S3 == nil ||
			current.S3.CredentialsSecretUID != requested.S3.CredentialsSecretUID {
			return repositoryBindingConflict(
				"BackupRepository credentials Secret was replaced while the workflow was running",
			)
		}
	default:
		return repositoryBindingConflict(
			fmt.Sprintf("resolved repository backend %q is unsupported", requested.Type),
		)
	}

	return nil
}

func validateRepositoryBinding(binding *v1alpha1.BackupRepositoryBindingStatus) error {
	if binding == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup repository",
			"resolved repository binding is required",
		)
	}

	switch binding.Type {
	case v1alpha1.BackupRepositoryTypeS3:
		if binding.S3 == nil || binding.S3.CredentialsSecretUID == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"backup repository",
				"resolved S3 repository requires a credentials Secret UID",
			)
		}
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup repository",
			fmt.Sprintf("resolved repository backend %q is unsupported", binding.Type),
		)
	}

	return nil
}

func repositoryBindingConflict(message string) error {
	return domain.NewError(
		domain.ErrorConflict,
		"backup repository",
		message+"; create a new workflow",
	)
}
