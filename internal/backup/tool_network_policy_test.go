package backup

import (
	"slices"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
)

func TestToolRequestResolvesToolNetworkPolicy(t *testing.T) {
	tests := []struct {
		name        string
		planValue   string
		toolsValue  string
		wantEnabled string // "", "true", or "false"
	}{
		{
			name:        "plan overrides the process default",
			planValue:   "off",
			toolsValue:  "auto",
			wantEnabled: "false",
		},
		{
			name:        "plan on wins over process off",
			planValue:   "on",
			toolsValue:  "off",
			wantEnabled: "true",
		},
		{
			name:        "empty plan keeps the process default",
			planValue:   "",
			toolsValue:  "off",
			wantEnabled: "false",
		},
		{
			name:        "auto everywhere keeps the upstream default",
			planValue:   "",
			toolsValue:  "",
			wantEnabled: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := objectstore.NewWithClient(
				&preflightObjectStore{},
				objectstore.Config{Bucket: "backups", Prefix: "pv-migrate", Name: "daily"},
				objectstore.Credentials{AccessKey: "access", SecretKey: "secret"},
			)
			if err != nil {
				t.Fatal(err)
			}

			transfer := backupTransfer{
				store: store,
				tools: ToolRuntime{ToolNetworkPolicy: test.toolsValue},
			}

			request, err := transfer.toolRequest(
				"app",
				"backup-1",
				v1alpha1.BackupPlan{
					SourcePVC:         v1alpha1.LocalResourceReference{Name: "data"},
					Name:              "backup-1",
					ToolNetworkPolicy: test.planValue,
				},
				false,
				store.RcloneConfig(),
				nil,
			)
			if err != nil {
				t.Fatalf("toolRequest: %v", err)
			}

			var network []string
			for _, value := range request.HelmValues {
				if strings.Contains(value, "networkPolicy.enabled") {
					network = append(network, value)
				}
			}

			switch test.wantEnabled {
			case "":
				if len(network) != 0 {
					t.Fatalf("network policy values %v, want none", network)
				}
			default:
				want := []string{
					"rsync.networkPolicy.enabled=" + test.wantEnabled,
					"sshd.networkPolicy.enabled=" + test.wantEnabled,
					"rclone.networkPolicy.enabled=" + test.wantEnabled,
				}
				if !slices.Equal(network, want) {
					t.Fatalf("network policy values %v, want %v", network, want)
				}
			}
		})
	}
}
