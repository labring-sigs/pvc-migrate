package backup

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"helm.sh/helm/v4/pkg/strvals"
)

func TestRcloneConfigHelmValuePreservesStrvalsDelimiters(t *testing.T) {
	want := "[remote]\n" +
		"type = s3\n" +
		"access_key_id = access,key\\with\\slashes\n" +
		"secret_access_key = secret=with,delimiters\n"

	value, err := rcloneConfigHelmValue(want)
	if err != nil {
		t.Fatal(err)
	}

	parsed, err := strvals.ParseString(value)
	if err != nil {
		t.Fatalf("parse Helm string value: %v", err)
	}

	rclone, ok := parsed["rclone"].(map[string]any)
	if !ok {
		t.Fatalf("parsed rclone value has type %T: %#v", parsed["rclone"], parsed)
	}

	if got, ok := rclone["config"].(string); !ok || got != want {
		t.Fatalf("rclone config=%q, want %q", got, want)
	}
}

func TestRcloneConfigHelmValueRejectsEmptyConfig(t *testing.T) {
	if _, err := rcloneConfigHelmValue("\n  \t"); err == nil {
		t.Fatal("empty rclone config was accepted")
	}
}

func TestPVMigrateBackupRequestKeepsCredentialsOutOfUpstreamFields(t *testing.T) {
	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
		objectstore.Credentials{
			AccessKey:    "access,key\\with\\slashes",
			SecretKey:    "secret=with,delimiters",
			SessionToken: "token,with\\delimiters",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := backupToolRequestFixture(
		"default",
		"daily",
		v1alpha1.BackupPlan{
			SourcePVC: v1alpha1.LocalResourceReference{Name: "data"},
		},
		false,
		store,
		store.RcloneConfig(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	if got.AccessKey != "" || got.SecretKey != "" || got.S3Provider != "" ||
		got.Endpoint != "" || got.Region != "" {
		t.Fatalf("upstream request carries unnecessary credentials/configuration: %#v", got)
	}

	configValue, err := rcloneConfigHelmValue(store.RcloneConfig())
	if err != nil {
		t.Fatal(err)
	}

	if !containsString(got.HelmStringValues, configValue) {
		t.Fatalf(
			"upstream request did not carry generated config in Helm values: %v",
			got.HelmStringValues,
		)
	}

	if got.RcloneConfigFile == "" || got.Remote != store.RemotePath() {
		t.Fatalf("upstream raw-config mode=%q remote=%q", got.RcloneConfigFile, got.Remote)
	}
}

func TestRequireSecretReleaseStorage(t *testing.T) {
	t.Run("unset driver allows the transfer", func(t *testing.T) {
		if err := requireSecretReleaseStorage(); err != nil {
			t.Fatalf("unset HELM_DRIVER rejected the transfer: %v", err)
		}
	})

	t.Run("secret driver allows the transfer", func(t *testing.T) {
		t.Setenv("HELM_DRIVER", "secret")

		if err := requireSecretReleaseStorage(); err != nil {
			t.Fatalf("HELM_DRIVER=secret rejected the transfer: %v", err)
		}
	})

	for _, driver := range []string{"configmap", "configmaps", "memory", "sql", "CONFIGMAP", "postgres"} {
		t.Run("driver "+driver+" rejects the transfer", func(t *testing.T) {
			t.Setenv("HELM_DRIVER", driver)

			err := requireSecretReleaseStorage()
			if err == nil {
				t.Fatalf("HELM_DRIVER=%q was accepted", driver)
			}

			if category := domain.CategoryOf(err); category != domain.ErrorValidation {
				t.Fatalf("HELM_DRIVER=%q category = %v, want validation", driver, category)
			}

			if !strings.Contains(err.Error(), "HELM_DRIVER") {
				t.Fatalf("rejection does not name the setting: %v", err)
			}
		})
	}
}

// TestBackupToolRequestRejectsNonSecretReleaseStorage pins the guard at the
// request boundary: the credential-bearing rclone config value must never be
// handed to a transfer whose release storage is not the Secret driver.
func TestBackupToolRequestRejectsNonSecretReleaseStorage(t *testing.T) {
	store, err := objectstore.NewWithClient(
		&preflightObjectStore{},
		objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
		objectstore.Credentials{AccessKey: "access", SecretKey: "secret"},
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("HELM_DRIVER", "configmap")

	if _, err := backupToolRequestFixture(
		"tenant-a",
		"nightly",
		v1alpha1.BackupPlan{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
		false,
		store,
		store.RcloneConfig(),
		nil,
	); err == nil || domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("configmap release storage was accepted: %v", err)
	}
}
