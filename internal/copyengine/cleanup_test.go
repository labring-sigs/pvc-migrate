package copyengine //nolint:testpackage // Exercise internal Helm cleanup with an in-memory release store.

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"helm.sh/helm/v4/pkg/action"
	helmkube "helm.sh/helm/v4/pkg/kube/fake"
	"helm.sh/helm/v4/pkg/release/common"
	release "helm.sh/helm/v4/pkg/release/v1"
	"helm.sh/helm/v4/pkg/storage"
	"helm.sh/helm/v4/pkg/storage/driver"
)

func TestCleanupRemovesOnlyInterruptedAttemptReleases(t *testing.T) {
	request := Request{
		SessionID: "recovery",
		Source:    domain.ObjectReference{Namespace: "tenant", Name: "data"},
		Mode:      ModeFinal,
		Attempt:   2,
	}
	config := &action.Configuration{
		Releases:   storage.Init(driver.NewMemory()),
		KubeClient: &helmkube.PrintingKubeClient{Out: io.Discard},
	}

	name := "pv-migrate-" + OperationID(request) + "-clusterip"
	for _, releaseName := range []string{name, "other-workload"} {
		if err := config.Releases.Create(&release.Release{
			Name: releaseName, Namespace: "tenant", Version: 1,
			Info:     &release.Info{Status: common.StatusPendingInstall},
			Manifest: "apiVersion: v1\nkind: Secret\nmetadata:\n  name: transfer-key\n",
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := cleanupReleases(context.Background(), config, request); err != nil {
		t.Fatal(err)
	}

	if _, err := config.Releases.Last(name); !errors.Is(err, driver.ErrReleaseNotFound) {
		t.Fatalf("interrupted release remains: %v", err)
	}

	if _, err := config.Releases.Last("other-workload"); err != nil {
		t.Fatalf("unrelated release changed: %v", err)
	}

	if err := cleanupReleases(context.Background(), config, request); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
}

func TestCleanupStopsBeforeMutationOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := cleanupReleases(ctx, nil, Request{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}
