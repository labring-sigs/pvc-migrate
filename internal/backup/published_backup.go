package backup

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
)

func validatePublishedBackup(
	ctx context.Context,
	store S3RepositoryStore,
	id, namespace string,
	plan v1alpha1.BackupPlan,
	manifest *objectstore.Manifest,
) error {
	if manifest == nil || id == "" {
		return domain.NewError(
			domain.ErrorValidation,
			backupResumePhase,
			"published manifest and backup session are required",
		)
	}

	if manifest.SessionID != id ||
		manifest.SourceNamespace != namespace ||
		manifest.SourcePVC != plan.SourcePVC.Name ||
		manifest.SourcePVCUID != string(plan.SourcePVC.UID) ||
		manifest.SourcePV != plan.SourcePV.Name ||
		manifest.SourcePVUID != string(plan.SourcePV.UID) ||
		manifest.Path != plan.Path ||
		manifest.Consistency != backupConsistency(plan.Online) {
		return domain.NewError(
			domain.ErrorConflict,
			backupResumePhase,
			"published completion manifest does not belong to this backup session",
		)
	}

	if err := store.VerifyInventory(ctx, *manifest); err != nil {
		return wrapBackupError(
			domain.ErrorConflict,
			backupResumePhase,
			"verify published backup inventory",
			err,
		)
	}

	return nil
}
