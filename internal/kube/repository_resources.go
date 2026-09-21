package kube

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (s *ConfigMapRepositoryStore) CreateCredentials(
	ctx context.Context,
	namespace string,
	owner v1alpha1.ObjectReference,
	data map[string][]byte,
) error {
	if err := s.validateOwner(ctx, owner); err != nil {
		return err
	}

	if err := ValidateS3CredentialsData(data); err != nil {
		return err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      BackupCredentialsSecretName(owner.Name),
			Labels:    map[string]string{ManagedByLabel: ManagedByValue, SessionKey: owner.Name},
			Annotations: map[string]string{
				repositoryOwnerUID:       string(owner.UID),
				repositoryOwnerNamespace: owner.Namespace,
				repositoryOwnerName:      owner.Name,
			},
		},
		Type: corev1.SecretTypeOpaque, Immutable: new(true), Data: data,
	}
	if namespace == owner.Namespace {
		secret.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Name:       SessionConfigMapName(owner.Name),
				UID:        owner.UID,
			},
		}
	}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	_, err := s.client.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})

	return errors.Join(err, ctx.Err(), LeaseFenceError(ctx))
}

func (s *ConfigMapRepositoryStore) ValidateOwnedCleanup(
	ctx context.Context,
	key crclient.ObjectKey,
	owner v1alpha1.ObjectReference,
) error {
	if _, err := s.ownedRepository(ctx, key, owner); err != nil {
		return err
	}

	_, err := s.ownedCredentials(ctx, key.Namespace, owner)

	return err
}

func (s *ConfigMapRepositoryStore) CleanupOwned(
	ctx context.Context,
	key crclient.ObjectKey,
	owner v1alpha1.ObjectReference,
) error {
	repository, err := s.ownedRepository(ctx, key, owner)
	if err != nil {
		return err
	}

	secret, err := s.ownedCredentials(ctx, key.Namespace, owner)
	if err != nil {
		return err
	}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	if secret != nil {
		if err := s.client.CoreV1().
			Secrets(secret.Namespace).
			Delete(ctx, secret.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &secret.UID, ResourceVersion: &secret.ResourceVersion}}); err != nil &&
			!apierrors.IsNotFound(err) {
			return err
		}
		if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
			return err
		}
	}

	if repository != nil {
		return s.Delete(ctx, repository, owner)
	}

	return nil
}

func (s *ConfigMapRepositoryStore) ownedRepository(
	ctx context.Context,
	key crclient.ObjectKey,
	owner v1alpha1.ObjectReference,
) (*v1alpha1.BackupRepository, error) {
	if owner.Name == "" || owner.Namespace == "" || owner.UID == "" || key.Namespace == "" ||
		key.Name == "" {
		return nil, errors.New(
			"repository cleanup requires repository and owning workflow identities",
		)
	}

	cm, err := s.client.CoreV1().
		ConfigMaps(key.Namespace).
		Get(ctx, repositoryConfigMapName(key.Name), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	if cm.Annotations[repositoryOwnerUID] != string(owner.UID) ||
		cm.Annotations[repositoryOwnerNamespace] != owner.Namespace ||
		cm.Annotations[repositoryOwnerName] != owner.Name {
		return nil, workflowStoreConflict(
			"cleanup repository",
			"repository belongs to another workflow",
		)
	}

	return decodeRepositoryConfigMap(cm, key)
}

func (s *ConfigMapRepositoryStore) ownedCredentials(
	ctx context.Context,
	namespace string,
	owner v1alpha1.ObjectReference,
) (*corev1.Secret, error) {
	secret, err := s.client.CoreV1().
		Secrets(namespace).
		Get(ctx, BackupCredentialsSecretName(owner.Name), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	if secret.UID == "" || secret.ResourceVersion == "" ||
		secret.Labels[ManagedByLabel] != ManagedByValue ||
		secret.Labels[SessionKey] != owner.Name ||
		secret.Annotations[repositoryOwnerUID] != string(owner.UID) ||
		secret.Annotations[repositoryOwnerNamespace] != owner.Namespace ||
		secret.Annotations[repositoryOwnerName] != owner.Name {
		return nil, workflowStoreConflict(
			"cleanup credentials",
			"credentials belong to another workflow",
		)
	}

	return secret, nil
}
