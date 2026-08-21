package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// #nosec G101 -- This is a Kubernetes object-name prefix, not credential material.
	BackupCredentialsSecretPrefix = "pvc-migrate-backup-credentials-"
	BackupAccessKeyDataKey        = "accessKey"
	BackupSecretKeyDataKey        = "secretKey"
	BackupSessionTokenDataKey     = "sessionToken"
)

func BackupCredentialsSecretName(sessionID string) string {
	digest := sha256.Sum256([]byte(sessionID))
	return BackupCredentialsSecretPrefix + hex.EncodeToString(digest[:])[:32]
}

func CreateBackupCredentialsSecret(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, sessionID string,
	data map[string][]byte,
) (*corev1.Secret, error) {
	if client == nil || namespace == "" || sessionID == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"backup credentials",
			"Kubernetes client, namespace, and session ID are required",
		)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BackupCredentialsSecretName(sessionID),
			Namespace: namespace,
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     sessionID,
			},
		},
		Type:      corev1.SecretTypeOpaque,
		Immutable: new(true),
		Data:      data,
	}

	created, err := client.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil, domain.WrapError(
			domain.ErrorConflict,
			"backup credentials",
			"credentials Secret already exists",
			err,
		)
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"backup credentials",
			"create credentials Secret",
			err,
		)
	}

	return created, nil
}

func DeleteBackupCredentialsSecret(
	ctx context.Context,
	client kubernetes.Interface,
	ref domain.ObjectReference,
	sessionID string,
) error {
	secret, err := backupCredentialsSecretForCleanup(ctx, client, ref, sessionID)
	if err != nil || secret == nil {
		return err
	}

	options := metav1.DeleteOptions{}
	if ref.UID != "" {
		options.Preconditions = &metav1.Preconditions{UID: &ref.UID}
	}

	if err := client.CoreV1().
		Secrets(ref.Namespace).
		Delete(ctx, ref.Name, options); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"backup credentials cleanup",
			fmt.Sprintf("delete Secret %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	return nil
}

// ValidateBackupCredentialsSecretCleanup verifies cleanup ownership without
// mutating the Secret. A missing Secret is already clean.
func ValidateBackupCredentialsSecretCleanup(
	ctx context.Context,
	client kubernetes.Interface,
	ref domain.ObjectReference,
	sessionID string,
) error {
	_, err := backupCredentialsSecretForCleanup(ctx, client, ref, sessionID)
	return err
}

func backupCredentialsSecretForCleanup(
	ctx context.Context,
	client kubernetes.Interface,
	ref domain.ObjectReference,
	sessionID string,
) (*corev1.Secret, error) {
	if client == nil || ref.Namespace == "" || ref.Name == "" {
		return nil, nil
	}

	if ref.Name != BackupCredentialsSecretName(sessionID) {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"backup credentials cleanup",
			"credentials Secret name does not match the backup session",
		)
	}

	secret, err := client.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"backup credentials cleanup",
			fmt.Sprintf("read Secret %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	if secret.Labels[ManagedByLabel] != ManagedByValue || secret.Labels[SessionKey] != sessionID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"backup credentials cleanup",
			fmt.Sprintf("Secret %s/%s is not owned by the backup session", ref.Namespace, ref.Name),
		)
	}

	if ref.UID != "" && secret.UID != ref.UID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"backup credentials cleanup",
			"credentials Secret identity changed",
		)
	}

	return secret, nil
}

func GetBackupCredentialsSecret(
	ctx context.Context,
	client kubernetes.Interface,
	ref domain.ObjectReference,
	sessionID string,
) (*corev1.Secret, error) {
	if client == nil || ref.Namespace == "" || ref.Name == "" || sessionID == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"backup credentials",
			"Kubernetes client, Secret reference, and session ID are required",
		)
	}

	if ref.Name != BackupCredentialsSecretName(sessionID) {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"backup credentials",
			"credentials Secret name does not match the backup session",
		)
	}

	secret, err := client.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, domain.WrapError(
			domain.ErrorPrecondition,
			"backup credentials",
			fmt.Sprintf(
				"credentials Secret %s/%s no longer exists; start a new backup with credentials",
				ref.Namespace,
				ref.Name,
			),
			err,
		)
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"backup credentials",
			fmt.Sprintf("read Secret %s/%s", ref.Namespace, ref.Name),
			err,
		)
	}

	if ref.UID != "" && secret.UID != ref.UID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"backup credentials",
			"credentials Secret identity changed",
		)
	}

	if secret.Labels[ManagedByLabel] != ManagedByValue || secret.Labels[SessionKey] != sessionID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"backup credentials",
			"credentials Secret ownership does not match the backup session",
		)
	}

	return secret, nil
}
