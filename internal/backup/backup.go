package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/client-go/kubernetes"
)

func backupToolProbeTarget(
	source v1alpha1.ObjectReference,
	path string,
	nodeName string,
	online bool,
	writableMount bool,
) kube.ToolProbeTarget {
	pvcName := ""
	if nodeName == "" || path != "" || online || writableMount {
		pvcName = source.Name
	}

	return kube.ToolProbeTarget{
		Namespace: source.Namespace, NodeName: nodeName, PVCName: pvcName,
		RequiredPath: path, WritablePVCMount: writableMount,
		Components: []string{kube.ToolComponentRclone},
	}
}

type repositoryLocation interface {
	Backend() string
	Destination() string
}

func acquireBackupTargetLock(
	ctx context.Context,
	locker kube.SessionLocker,
	namespace string,
	location repositoryLocation,
	logger *slog.Logger,
) (context.Context, kube.SessionLock, context.CancelFunc, error) {
	if locker == nil {
		return ctx, nil, func() {}, nil
	}

	if location == nil || strings.TrimSpace(namespace) == "" {
		return nil, nil, nil, domain.NewError(
			domain.ErrorValidation,
			"backup target lock",
			"object store and session namespace are required",
		)
	}

	lockID := backupTargetLockID(location)
	logOperation(
		logger,
		"acquiring Kubernetes backup target Lease",
		"destination",
		location.Destination(),
	)

	lock, err := kube.AcquireRequiredSessionLock(
		ctx,
		locker,
		namespace,
		lockID,
	)
	if err != nil {
		return nil, nil, nil, wrapBackupTargetLockError(
			location.Destination(),
			"another backup is already changing this recovery point",
			err,
		)
	}

	boundCtx, cancel := lock.Bind(ctx)
	boundCtx = kube.WithLeaseFence(boundCtx, lock)

	logOperation(
		logger,
		"Kubernetes backup target Lease acquired",
		"destination",
		location.Destination(),
	)

	return boundCtx, lock, cancel, nil
}

func backupTargetLockID(store repositoryLocation) string {
	digest := sha256.Sum256([]byte(store.Backend() + "\x00" + store.Destination()))
	return "backup-target-" + hex.EncodeToString(digest[:])[:32]
}

func wrapBackupTargetLockError(destination, message string, err error) error {
	category := domain.CategoryOf(err)
	if category == domain.ErrorInternal {
		category = domain.ErrorConflict
	}

	return domain.WrapError(
		category,
		"backup target lock",
		fmt.Sprintf("%s (%s)", message, destination),
		err,
	)
}

func validateOfflineBackupToolStart(
	ctx context.Context,
	client kubernetes.Interface,
	source v1alpha1.ObjectReference,
) error {
	// A consumer can appear after Preflight while the operation waits for the
	// object-store lock and tool setup. Recheck immediately before the tool
	// mounts the offline source PVC.
	_, err := inspectBackupPVC(ctx, client, source.Namespace, source.Name, false)

	return err
}

type lockLease struct {
	mu   sync.RWMutex
	etag string
}

func (l *lockLease) current() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.etag
}

func (l *lockLease) renewNow(
	ctx context.Context,
	store RepositoryStore,
	holder string,
	ttl time.Duration,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	etag, err := store.RenewLock(ctx, holder, l.etag, ttl)
	if err != nil {
		return err
	}

	l.etag = etag

	return nil
}

func checkObjectStoreLease(ctx context.Context, leaseErrors <-chan error) error {
	select {
	case err := <-leaseErrors:
		var typed *domain.Error
		if errors.As(err, &typed) && typed.Category == domain.ErrorTimeout {
			return err
		}

		return classifyLeaseError(
			ctx,
			domain.WrapError(domain.ErrorConflict, "S3 lock", "S3 lock ownership was lost", err),
		)
	default:
	}

	if err := ctx.Err(); err != nil {
		return classifyLeaseError(ctx, err)
	}

	return nil
}

func classifyLeaseError(ctx context.Context, err error) error {
	if domain.CategoryOf(err) == domain.ErrorTimeout || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(ctx.Err(), context.Canceled) {
		return domain.WrapError(
			domain.ErrorTimeout,
			"backup",
			"S3 lock lease ended before the backup was published",
			err,
		)
	}

	if domain.CategoryOf(err) == domain.ErrorConflict {
		return err
	}

	return err
}

// lockRenewalAborts decides whether a failed renewal must abandon the
// transfer: definite ownership loss always aborts, and transient failures
// abort only once half the TTL has passed without a successful renewal —
// past that point a successor may legally hold the lock.
func lockRenewalAborts(err error, lastRenewed time.Time, ttl time.Duration) bool {
	return domain.CategoryOf(err) == domain.ErrorConflict || time.Since(lastRenewed) > ttl/2
}

func renewObjectStoreLock(
	ctx context.Context,
	cancel context.CancelFunc,
	store RepositoryStore,
	holder string,
	ttl time.Duration,
	lease *lockLease,
	leaseErrors chan<- error,
	done chan<- struct{},
) {
	defer close(done)

	interval := max(ttl/3, 30*time.Second)
	interval = min(interval, time.Minute)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Definite ownership loss (a foreign holder or a fenced ETag) aborts
	// immediately; transient failures retry inside the TTL budget the holder
	// already owns, so one S3 blip cannot abandon a healthy multi-hour
	// transfer. Half the TTL is the abort line: past it another holder may
	// legally take over, so writing on would be unsafe.
	lastRenewed := time.Now()

	const retryInterval = 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := lease.renewNow(ctx, store, holder, ttl)
			if err == nil {
				lastRenewed = time.Now()

				ticker.Reset(interval)

				continue
			}

			if lockRenewalAborts(err, lastRenewed, ttl) {
				select {
				case leaseErrors <- err:
				default:
				}

				cancel()

				return
			}

			ticker.Reset(retryInterval)
		}
	}
}

func onlineBackupToolNode(
	ctx context.Context,
	client kubernetes.Interface,
	source v1alpha1.ObjectReference,
) (string, error) {
	info, err := inspectBackupPVC(ctx, client, source.Namespace, source.Name, true)
	if err != nil {
		return "", err
	}

	node, err := rwoConsumerNode(info, "online backup scheduling")
	if err != nil {
		return "", err
	}

	return node, nil
}

func backupConsistency(online bool) string {
	if online {
		return "best-effort crash-consistent file copy"
	}
	return "offline file-consistent copy"
}
