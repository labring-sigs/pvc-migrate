package kube

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
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

// ValidateS3CredentialsData rejects incomplete credential material before a
// controller can silently fall back to ambient credentials. Session tokens are
// optional for long-lived access keys.
func ValidateS3CredentialsData(data map[string][]byte) error {
	if len(data[BackupAccessKeyDataKey]) == 0 || len(data[BackupSecretKeyDataKey]) == 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup credentials",
			"Secret must contain non-empty accessKey and secretKey data",
		)
	}

	for key, value := range data {
		if key != BackupAccessKeyDataKey && key != BackupSecretKeyDataKey &&
			key != BackupSessionTokenDataKey {
			continue
		}

		for _, b := range value {
			if b == '\r' || b == '\n' || b == 0 {
				return domain.NewError(
					domain.ErrorValidation,
					"backup credentials",
					"Secret data contains unsafe control characters",
				)
			}
		}
	}

	return nil
}
