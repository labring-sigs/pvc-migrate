package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

// toolCleanupTimeout bounds the detached best-effort removal of an
// interrupted tool release. The uninstall's worst case is the 30s delete
// wait plus roughly seven individually bounded API calls (history read,
// reachability, per-resource DELETEs, purge) under a slow API server, so the
// budget stays above that sum; the operation lock TTL it protects is minutes
// larger still.
const toolCleanupTimeout = 150 * time.Second

// ToolRunner runs the data-plane tool and can remove its helm release. The
// interface keeps the transfer paths testable and gives the error paths one
// authoritative way to converge "the tool is no longer running".
type ToolRunner interface {
	RunBackup(ctx context.Context, request pvmigrate.Backup) error
	RunRestore(ctx context.Context, request pvmigrate.Restore) error
	UninstallTool(
		ctx context.Context,
		kubeconfigPath, kubeContext, namespace, releaseName string,
	) error
}

type upstreamToolRunner struct{}

func (upstreamToolRunner) RunBackup(ctx context.Context, request pvmigrate.Backup) error {
	return pvmigrate.RunBackup(ctx, request)
}

func (upstreamToolRunner) RunRestore(ctx context.Context, request pvmigrate.Restore) error {
	return pvmigrate.RunRestore(ctx, request)
}

func (upstreamToolRunner) UninstallTool(
	ctx context.Context,
	kubeconfigPath, kubeContext, namespace, releaseName string,
) error {
	return copyengine.UninstallNamedRelease(
		ctx,
		kubeconfigPath,
		kubeContext,
		namespace,
		releaseName,
	)
}

func resolveToolRunner(runner ToolRunner) ToolRunner {
	if runner != nil {
		return runner
	}

	return upstreamToolRunner{}
}

func backupToolReleaseName(toolID string) string {
	return fmt.Sprintf("pv-migrate-%s-backup", toolID)
}

func restoreToolReleaseName(toolID string) string {
	return fmt.Sprintf("pv-migrate-%s-restore", toolID)
}

// operationLockReleaseBlocked reports whether the operation lock must be left
// to expire instead of released: an error path whose tool could not be
// confirmed stopped may still be writing, and releasing would let a retry
// run a second writer against the same recovery point or destination.
func operationLockReleaseBlocked(retErr, cleanupErr error) bool {
	return retErr != nil && cleanupErr != nil
}

// cleanupInterruptedTool removes the tool release an error path may have
// left running: upstream helm-wait timeouts and cancellations install the
// release but never run their completion cleanup, so an orphaned rclone job
// can keep writing after the operator process has given up. The call runs
// on a detached, bounded context and returns nil only when the tool is
// confirmed gone (a release that never existed counts as gone).
func cleanupInterruptedTool(
	ctx context.Context,
	runner ToolRunner,
	tools ToolRuntime,
	operation, namespace, releaseName string,
) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), toolCleanupTimeout)
	defer cancel()

	err := resolveToolRunner(runner).UninstallTool(
		cleanupCtx,
		tools.KubeconfigPath,
		tools.KubeContext,
		namespace,
		releaseName,
	)
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			operation+" tool cleanup",
			"uninstall the interrupted tool release "+releaseName,
			err,
		)
	}

	return nil
}
