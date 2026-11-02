package copyengine_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

func TestCopyExpandsToolNetworkPolicy(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want []string
	}{
		{
			name: "on forces every component",
			mode: kube.ToolNetworkPolicyOn,
			want: []string{
				"rsync.networkPolicy.enabled=true",
				"sshd.networkPolicy.enabled=true",
				"rclone.networkPolicy.enabled=true",
			},
		},
		{
			name: "off disables every component",
			mode: kube.ToolNetworkPolicyOff,
			want: []string{
				"rsync.networkPolicy.enabled=false",
				"sshd.networkPolicy.enabled=false",
				"rclone.networkPolicy.enabled=false",
			},
		},
		{name: "auto keeps upstream default", mode: kube.ToolNetworkPolicyAuto},
		{name: "empty keeps upstream default", mode: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var captured []string

			engine := copyengine.NewPVMigrateForTest(
				func(_ context.Context, migration pvmigrate.Migration) error {
					captured = slices.Clone(migration.HelmValues)
					return nil
				},
			)

			err := engine.Copy(
				context.Background(),
				copyengine.CopyRequest{
					AttemptIdentity: copyengine.AttemptIdentity{
						Mode:      copyengine.ModeWarm,
						SessionID: "np-test",
						Source: v1alpha1.ObjectReference{
							Kind: "PersistentVolumeClaim", Namespace: "source", Name: "a", UID: "a",
						},
					},
					Destination: copyengine.CopyDestination{
						Reference: v1alpha1.ObjectReference{
							Kind: "PersistentVolumeClaim", Namespace: "source", Name: "b", UID: "b",
						},
					},
					Policy: copyengine.CopyPolicy{
						Strategies:        []string{"clusterip"},
						ToolNetworkPolicy: test.mode,
					},
				},
				nil,
			)
			if err != nil {
				t.Fatalf("copy: %v", err)
			}

			var network []string
			for _, value := range captured {
				if strings.Contains(value, "networkPolicy.enabled") {
					network = append(network, value)
				}
			}

			if !slices.Equal(network, test.want) {
				t.Fatalf("network policy values %v, want %v", network, test.want)
			}
		})
	}
}
