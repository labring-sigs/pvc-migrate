package backup

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
)

// A backup/restore round trip drops POSIX mode bits unless rclone carries
// object metadata (--metadata), and symbolic links require --links. Both tool
// requests must keep carrying both flags (#30).
func TestBackupAndRestoreToolRequestsPreserveLinksAndMetadata(t *testing.T) {
	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
		objectstore.Credentials{AccessKey: "access", SecretKey: "secret"},
	)
	if err != nil {
		t.Fatal(err)
	}

	backup, err := backupToolRequestFixture(
		"default",
		"daily",
		v1alpha1.BackupPlan{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
		false,
		store,
		store.RcloneConfig(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, flag := range []string{"--links", "--metadata"} {
		if !strings.Contains(backup.RcloneExtraArgs, flag) {
			t.Errorf("backup RcloneExtraArgs %q misses %s", backup.RcloneExtraArgs, flag)
		}
	}

	transfer := restoreTransfer{store: store}

	restore, err := transfer.toolRequest(
		"default",
		"daily",
		v1alpha1.RestorePlan{
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "restored"},
			Path:           ".",
		},
		store.RcloneConfig(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, flag := range []string{"--links", "--metadata"} {
		if !strings.Contains(restore.RcloneExtraArgs, flag) {
			t.Errorf("restore RcloneExtraArgs %q misses %s", restore.RcloneExtraArgs, flag)
		}
	}
}

// Keep the Helm overrides plumbing honest while touching this file's fixture.
func TestBackupToolRequestKeepsHelmOverrides(t *testing.T) {
	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Name: "daily"},
		objectstore.Credentials{AccessKey: "a", SecretKey: "b"},
	)
	if err != nil {
		t.Fatal(err)
	}

	overrides := &kube.HelmOverrides{Values: []string{"rclone.pvcMounts[0].readOnly=false"}}

	got, err := backupToolRequestFixture(
		"default",
		"daily",
		v1alpha1.BackupPlan{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
		true,
		store,
		store.RcloneConfig(),
		overrides,
	)
	if err != nil {
		t.Fatal(err)
	}

	joined := strings.Join(got.HelmValues, "\n")
	if !strings.Contains(joined, "readOnly=false") {
		t.Fatalf("writable-mount override lost: %v", got.HelmValues)
	}
}
