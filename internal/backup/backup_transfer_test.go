package backup

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

func backupToolRequestFixture(
	namespace, id string,
	plan v1alpha1.BackupPlan,
	writableMount bool,
	store S3RepositoryStore,
	config string,
	overrides *kube.HelmOverrides,
) (pvmigrate.Backup, error) {
	transfer := backupTransfer{store: store}

	return transfer.toolRequest(namespace, id, plan, writableMount, config, overrides)
}
