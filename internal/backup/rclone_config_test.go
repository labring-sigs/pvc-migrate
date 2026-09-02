package backup

import (
	"testing"

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

	request := Request{Namespace: "default", PVCName: "data", Store: store}

	got, err := pvmigrateBackupRequest(
		request,
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
