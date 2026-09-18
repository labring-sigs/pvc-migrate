package backup

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// S3RepositoryResolver provides a repository connection and the identities that
// must be checkpointed before transfer. It never receives workflow state.
type S3RepositoryResolver interface {
	Resolve(
		ctx context.Context,
		key crclient.ObjectKey,
		name string,
	) (S3RepositoryStore, *v1alpha1.BackupRepositoryBindingStatus, error)
}

// RepositoryResources manages only resources created for a session backend.
// Controllers leave this unset because referenced repositories are user-owned.
type RepositoryResources interface {
	ValidateOwnedCleanup(
		ctx context.Context,
		key crclient.ObjectKey,
		owner v1alpha1.ObjectReference,
	) error
	CleanupOwned(ctx context.Context, key crclient.ObjectKey, owner v1alpha1.ObjectReference) error
}

type repositoryAccess struct {
	load    RepositoryLoader
	client  kubernetes.Interface
	factory func(context.Context, objectstore.Config) (*objectstore.Store, error)
}

func NewS3RepositoryResolver(
	load RepositoryLoader,
	client kubernetes.Interface,
	factory func(context.Context, objectstore.Config) (*objectstore.Store, error),
) S3RepositoryResolver {
	if factory == nil {
		factory = objectstore.New
	}

	return &repositoryAccess{
		load:    load,
		client:  client,
		factory: factory,
	}
}

func (r *repositoryAccess) Resolve(
	ctx context.Context,
	key crclient.ObjectKey,
	name string,
) (S3RepositoryStore, *v1alpha1.BackupRepositoryBindingStatus, error) {
	config, binding, err := resolveStoredS3Repository(
		ctx,
		r.load,
		r.client,
		key,
		name,
	)
	if err != nil {
		return nil, nil, err
	}

	store, err := r.factory(ctx, config)
	if err != nil {
		return nil, nil, err
	}

	return store, binding, nil
}

func resolveTransferRepository(
	ctx context.Context,
	resolver S3RepositoryResolver,
	namespace, repository, name string,
) (S3RepositoryStore, *v1alpha1.BackupRepositoryBindingStatus, error) {
	if resolver == nil {
		return nil, nil, domain.NewError(
			domain.ErrorInternal,
			"repository transfer",
			"repository resolver is required",
		)
	}

	store, binding, err := resolver.Resolve(
		ctx,
		crclient.ObjectKey{Namespace: namespace, Name: repository},
		name,
	)
	if err != nil {
		return nil, nil, err
	}

	if store == nil {
		return nil, nil, domain.NewError(
			domain.ErrorInternal,
			"repository transfer",
			"repository resolver returned no store",
		)
	}

	if err := requireS3RepositoryBackend(store); err != nil {
		return nil, nil, err
	}

	return store, binding, nil
}
