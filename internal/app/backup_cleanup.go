package app

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

// cleanupBackupCredentials owns backup-only credential lifecycle. Migration
// cleanup has no Secret to delete and never enters this file.
func (s *Service) cleanupBackupCredentials(ctx context.Context, session *domain.Session) error {
	ref := backupCredentialsCleanupReference(session)
	if ref.Name == "" {
		return nil
	}

	if err := kube.DeleteBackupCredentialsSecret(ctx, s.client, ref, session.ID); err != nil {
		return err
	}

	session.Spec.Backup.CredentialsSecret = domain.ObjectReference{}
	if session.ResourceVersion != "" {
		return s.persist(ctx, session)
	}

	return nil
}

func backupCredentialsCleanupReference(session *domain.Session) domain.ObjectReference {
	if session == nil || session.Spec.Backup == nil {
		return domain.ObjectReference{}
	}

	if session.Spec.Backup.CredentialsSecret.Name != "" {
		return session.Spec.Backup.CredentialsSecret
	}

	if session.ID == "" || session.Spec.SessionNamespace == "" {
		return domain.ObjectReference{}
	}

	return domain.ObjectReference{
		APIVersion: "v1",
		Kind:       "Secret",
		Namespace:  session.Spec.SessionNamespace,
		Name:       kube.BackupCredentialsSecretName(session.ID),
	}
}
