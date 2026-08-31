package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

func runBackupWithSession(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	expectedPVCUID, expectedPVUID string,
) (retErr error) {
	if req.SessionStore == nil {
		return runBackup(ctx, client, req, expectedPVCUID, expectedPVUID)
	}

	session, err := prepareBackupSession(ctx, client, req, expectedPVCUID, expectedPVUID)
	if err != nil {
		return err
	}

	return withBackupSessionLock(ctx, req, session, func(lockedCtx context.Context) error {
		if err := req.SessionStore.Create(lockedCtx, session); err != nil {
			return err
		}

		if err := persistBackupCredentials(lockedCtx, client, req, session); err != nil {
			return err
		}

		req.BackupSession = session

		return runBackupSession(lockedCtx, client, req, session)
	})
}

func runBackupSession(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	session *domain.Session,
) (retErr error) {
	if session == nil {
		return domain.NewError(
			domain.ErrorValidation,
			backupResumePhase,
			"backup session is required",
		)
	}

	if session.Spec.Backup == nil {
		return domain.NewError(
			domain.ErrorValidation,
			backupResumePhase,
			"backup session payload is required",
		)
	}

	req.ID = session.ID
	expectedPVCUID := string(session.Spec.Backup.SourcePVC.UID)
	expectedPVUID := string(session.Spec.Backup.SourcePV.UID)
	sharedRestored := false

	defer func() {
		if fenceErr := backupSessionFenceError(ctx); fenceErr != nil {
			retErr = errors.Join(retErr, fenceErr)
			return
		}

		if !sharedRestored {
			cleanupErr := runWithPreservedCleanupTimeout(
				ctx,
				lockReleaseTimeout,
				func(cleanupCtx context.Context) error {
					return restoreBackupSharedMounts(cleanupCtx, req, session)
				},
			)
			if cleanupErr != nil {
				retErr = errors.Join(retErr, cleanupErr)
			}
		}

		if fenceErr := backupSessionFenceError(ctx); fenceErr != nil {
			retErr = errors.Join(retErr, fenceErr)
			return
		}

		if retErr != nil {
			failureCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				lockReleaseTimeout,
			)
			failureErr := failBackupSession(failureCtx, req, session, retErr)

			cancel()

			retErr = errors.Join(retErr, failureErr)
		}
	}()

	if session.Status.Phase == domain.PhaseCompleted {
		return nil
	}

	if err := updateBackupSession(
		ctx,
		req,
		session,
		domain.PhaseWarmCopying,
		"backup preparing source PVC",
	); err != nil {
		return err
	}

	info, err := inspectPVC(
		ctx,
		client,
		req.Namespace,
		req.PVCName,
		req.Online,
		req.AllowMounted,
		false,
	)
	if err != nil {
		return err
	}

	if info.PV.Spec.CSI != nil && info.PV.Spec.CSI.Driver == kube.OpenEBSLVMCSIDriver &&
		len(info.Consumers) > 0 {
		req.WritablePVCMount = true
	}

	if err := backupSessionOpenEBSState(ctx, req, session, info); err != nil {
		return err
	}

	if err := updateBackupSession(
		ctx,
		req,
		session,
		domain.PhaseWarmCopied,
		"backup source PVC prepared",
	); err != nil {
		return err
	}

	if err := runBackup(ctx, client, req, expectedPVCUID, expectedPVUID); err != nil {
		return err
	}

	if err := runWithPreservedCleanupTimeout(
		ctx,
		lockReleaseTimeout,
		func(cleanupCtx context.Context) error {
			return restoreBackupSharedMounts(cleanupCtx, req, session)
		},
	); err != nil {
		return err
	}

	sharedRestored = true

	if err := updateBackupSession(
		ctx,
		req,
		session,
		domain.PhaseCompleted,
		"backup completed",
	); err != nil {
		return err
	}

	return nil
}

func pvmigrateBackupRequest(
	req Request,
	configPath string,
	helmValues []string,
) (pvmigrate.Backup, error) {
	imageValues, err := kube.ToolImageHelmValues(req.ToolImage)
	if err != nil {
		return pvmigrate.Backup{}, err
	}

	if req.WritablePVCMount {
		// The upstream bucket-storage chart forces Backup mounts read-only. An
		// active OpenEBS LVM volume with shared=yes must use the same writable
		// mount contract as online migration, otherwise the CSI driver rejects
		// the second mount even though shared mode is enabled. Helm's map merge
		// replaces arrays as a whole, so preserve the base mount fields in one
		// override instead of replacing the array with only readOnly.
		helmValues = append(
			helmValues,
			"rclone.pvcMounts[0].name="+req.PVCName+
				",rclone.pvcMounts[0].mountPath=/data,rclone.pvcMounts[0].readOnly=false",
		)
	}

	return pvmigrate.Backup{
		ID: req.ID,
		PVC: pvmigrate.PVC{
			KubeconfigPath: req.KubeconfigPath,
			Context:        req.KubeContext,
			Namespace:      req.Namespace,
			Name:           req.PVCName,
		},
		Backend:          string(domain.ObjectStoreBackendS3),
		Bucket:           req.Store.Config().Bucket,
		Name:             req.Store.Config().Name,
		Path:             req.Path,
		Prefix:           req.Store.Config().Prefix,
		RcloneConfigFile: configPath,
		Remote:           req.Store.RemotePath(),
		RcloneExtraArgs:  rclonePreserveLinksArgs,
		// Consumer policy is enforced by inspectPVC immediately before launch.
		// Upstream also counts terminal Pods, so its broader mounted check must
		// not override the phase-aware result.
		IgnoreMounted: true,
		HelmValues:    kube.ToolSecurityContextHelmValues(),
		HelmStringValues: append(
			append(kube.ZeroResourceHelmValues(), imageValues...),
			helmValues...,
		),
		HelmTimeout:    toolHelmTimeout(req.HelmTimeout),
		Writer:         req.Writer,
		Logger:         req.Logger,
		StructuredLogs: true,
	}, nil
}

func runBackup(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	expectedPVCUID, expectedPVUID string,
) (retErr error) {
	targetCtx, targetLock, cancelTarget, err := acquireBackupTargetLock(ctx, req)
	if err != nil {
		return err
	}

	if targetLock != nil {
		ctx = targetCtx

		defer func() {
			cancelTarget()

			if lockErr := targetLock.Err(); lockErr != nil {
				retErr = errors.Join(
					retErr,
					wrapBackupTargetLockError(
						req,
						"backup target lock ownership was lost",
						lockErr,
					),
				)
			}

			if deleteErr := runWithCleanupTimeout(
				lockReleaseTimeout,
				targetLock.Delete,
			); deleteErr != nil {
				retErr = errors.Join(
					retErr,
					wrapBackupTargetLockError(req, "delete backup target Lease", deleteErr),
				)
			}
		}()
	}

	holder, err := operationLockHolder(req.ID)
	if err != nil {
		return err
	}

	logOperation(
		req,
		"acquiring backup operation lock",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
	)

	lockETag, err := req.Store.AcquireLock(ctx, holder, operationLockTTL(ctx, req.HelmTimeout))
	if err != nil {
		return err
	}

	logOperation(
		req,
		"backup operation lock acquired",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
	)

	lease := &lockLease{etag: lockETag}
	leaseCtx, cancelLease := context.WithCancel(ctx)
	leaseErrors := make(chan error, 1)
	leaseDone := make(chan struct{})

	go renewObjectStoreLock(
		leaseCtx,
		cancelLease,
		req.Store,
		holder,
		operationLockTTL(ctx, req.HelmTimeout),
		lease,
		leaseErrors,
		leaseDone,
	)
	defer func() {
		cancelLease()
		<-leaseDone

		select {
		case leaseErr := <-leaseErrors:
			retErr = errors.Join(retErr, leaseErr)
		default:
		}

		if releaseErr := runWithCleanupTimeout(
			lockReleaseTimeout,
			func(releaseCtx context.Context) error {
				return req.Store.ReleaseLock(releaseCtx, lease.current())
			},
		); releaseErr != nil {
			retErr = errors.Join(retErr, releaseErr)
		}
	}()

	// A concurrent backup may pass the initial preflight while this operation
	// waits for the distributed lock, so check the immutable recovery point again.
	manifest, err := req.Store.Manifest(leaseCtx)
	if err != nil {
		return err
	}

	if manifest != nil {
		if req.BackupSession != nil {
			return validatePublishedBackupSession(leaseCtx, req, manifest)
		}

		return domain.NewError(
			domain.ErrorConflict,
			"backup",
			"S3 completion manifest already exists; use a new backup name to preserve the published recovery point",
		)
	}

	_, currentPV, err := verifyPVCIdentity(
		leaseCtx,
		client,
		req.Namespace,
		req.PVCName,
		expectedPVCUID,
		expectedPVUID,
	)
	if err != nil {
		return err
	}

	probeResult, helmValues, err := prepareBackupTransferTool(
		leaseCtx,
		client,
		req,
		currentPV,
	)
	if err != nil {
		return err
	}

	configFile, err := os.CreateTemp("", "pvc-migrate-s3-*.conf")
	if err != nil {
		return domain.WrapError(domain.ErrorInternal, "backup", "create temporary S3 config", err)
	}

	configPath := configFile.Name()
	defer os.Remove(configPath)

	if err := configFile.Chmod(0o600); err != nil {
		configFile.Close()
		return domain.WrapError(domain.ErrorInternal, "backup", "protect temporary S3 config", err)
	}

	if _, err := configFile.WriteString(req.Store.RcloneConfig()); err != nil {
		configFile.Close()
		return domain.WrapError(domain.ErrorInternal, "backup", "write temporary S3 config", err)
	}

	if err := configFile.Close(); err != nil {
		return domain.WrapError(domain.ErrorInternal, "backup", "close temporary S3 config", err)
	}

	toolRequest := req
	toolRequest.ID = toolOperationID(holder)

	backupRequest, err := pvmigrateBackupRequest(toolRequest, configPath, helmValues)
	if err != nil {
		return err
	}

	if err := validateTransferToolLaunch(
		leaseCtx,
		client,
		req,
		expectedPVCUID,
		expectedPVUID,
		probeResult,
		false,
	); err != nil {
		return err
	}

	logOperation(
		req,
		"starting backup data synchronization",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
		"toolNode",
		probeResult.NodeName,
	)

	toolLogs := startToolLogs(leaseCtx, client, toolRequest)
	toolErr := pvmigrate.RunBackup(leaseCtx, backupRequest)

	toolLogs.Stop()

	toolErr = errors.Join(toolErr, toolLogs.ObservedError())
	if toolErr != nil {
		return classifyToolAndLeaseError(leaseCtx, "backup", toolErr, leaseErrors)
	}

	if err := checkObjectStoreLease(leaseCtx, leaseErrors); err != nil {
		return err
	}

	if err := lease.renewNow(
		leaseCtx,
		req.Store,
		holder,
		operationLockTTL(ctx, req.HelmTimeout),
	); err != nil {
		return classifyLeaseError(leaseCtx, err)
	}

	pvc, pv, err := verifyPVCIdentity(
		leaseCtx,
		client,
		req.Namespace,
		req.PVCName,
		expectedPVCUID,
		expectedPVUID,
	)
	if err != nil {
		return err
	}

	capacity := pv.Spec.Capacity[corev1.ResourceStorage]

	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}

	logOperation(req, "building backup inventory", "namespace", req.Namespace, "pvc", req.PVCName)

	inventory, err := req.Store.Inventory(leaseCtx)
	if err != nil {
		return err
	}

	if err := checkObjectStoreLease(leaseCtx, leaseErrors); err != nil {
		return err
	}

	if err := lease.renewNow(
		leaseCtx,
		req.Store,
		holder,
		operationLockTTL(ctx, req.HelmTimeout),
	); err != nil {
		return classifyLeaseError(leaseCtx, err)
	}

	logOperation(
		req,
		"publishing backup completion manifest",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
	)

	manifestSessionID := ""
	if req.BackupSession != nil {
		manifestSessionID = req.BackupSession.ID
	}

	return req.Store.PutManifest(leaseCtx, objectstore.Manifest{
		CreatedAt:       time.Now().UTC(),
		SessionID:       manifestSessionID,
		SourceNamespace: req.Namespace,
		SourcePVC:       req.PVCName,
		SourcePVCUID:    string(pvc.UID),
		SourcePV:        pv.Name,
		SourcePVUID:     string(pv.UID),
		Path:            req.Path,
		Capacity:        capacity.String(),
		VolumeMode:      string(mode),
		Consistency:     backupConsistency(req.Online),
		Compression:     "none",
		ObjectCount:     inventory.ObjectCount,
		TotalBytes:      inventory.TotalBytes,
		InventorySHA256: inventory.SHA256,
	})
}

func prepareBackupTransferTool(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	pv *corev1.PersistentVolume,
) (kube.ToolImageProbeResult, []string, error) {
	toolNode, err := onlineBackupToolNode(ctx, client, req)
	if err != nil {
		return kube.ToolImageProbeResult{}, nil, err
	}

	if toolNode == "" {
		toolNode, err = uniquePVToolNode(ctx, client, pv, backupPreflightPhase)
		if err != nil {
			return kube.ToolImageProbeResult{}, nil, err
		}
	}

	if err := validateBackupToolStart(ctx, client, req); err != nil {
		return kube.ToolImageProbeResult{}, nil, err
	}

	probeResult, err := probeTransferToolImage(ctx, req, toolNode, false)
	if err != nil {
		return kube.ToolImageProbeResult{}, nil, err
	}

	helmValues, err := transferToolHelmValues(ctx, client, probeResult)
	if err != nil {
		return kube.ToolImageProbeResult{}, nil, err
	}

	return probeResult, helmValues, nil
}

func acquireBackupTargetLock(
	ctx context.Context,
	req Request,
) (context.Context, kube.SessionLock, context.CancelFunc, error) {
	locker, ok := req.SessionStore.(kube.SessionLocker)
	if !ok {
		return ctx, nil, func() {}, nil
	}

	if req.Store == nil || strings.TrimSpace(req.SessionNamespace) == "" {
		return nil, nil, nil, domain.NewError(
			domain.ErrorValidation,
			"backup target lock",
			"object store and session namespace are required",
		)
	}

	lockID := backupTargetLockID(req.Store)
	logOperation(
		req,
		"acquiring Kubernetes backup target Lease",
		"destination",
		req.Store.Destination(),
	)

	lock, err := locker.AcquireSessionLock(ctx, req.SessionNamespace, lockID)
	if err != nil {
		return nil, nil, nil, wrapBackupTargetLockError(
			req,
			"another backup is already changing this recovery point",
			err,
		)
	}

	boundCtx, cancel := lock.Bind(ctx)

	logOperation(
		req,
		"Kubernetes backup target Lease acquired",
		"destination",
		req.Store.Destination(),
	)

	return boundCtx, lock, cancel, nil
}

func backupTargetLockID(store *objectstore.Store) string {
	config := store.Config()
	digest := sha256.Sum256([]byte(config.Bucket + "\x00" + config.Prefix + "\x00" + config.Name))
	return "backup-target-" + hex.EncodeToString(digest[:])[:32]
}

func wrapBackupTargetLockError(req Request, message string, err error) error {
	category := domain.CategoryOf(err)
	if category == domain.ErrorInternal {
		category = domain.ErrorConflict
	}

	destination := "unknown"
	if req.Store != nil {
		destination = req.Store.Destination()
	}

	return domain.WrapError(
		category,
		"backup target lock",
		fmt.Sprintf("%s (%s)", message, destination),
		err,
	)
}

func validateBackupToolStart(ctx context.Context, client kubernetes.Interface, req Request) error {
	if req.Online {
		return nil
	}
	// A consumer can appear after Preflight while the operation waits for the
	// object-store lock and tool setup. Recheck immediately before the tool
	// mounts the offline source PVC.
	_, err := inspectPVC(ctx, client, req.Namespace, req.PVCName, false, false, false)

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
	store *objectstore.Store,
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

func renewObjectStoreLock(
	ctx context.Context,
	cancel context.CancelFunc,
	store *objectstore.Store,
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

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := lease.renewNow(ctx, store, holder, ttl)
			if err != nil {
				select {
				case leaseErrors <- err:
				default:
				}

				cancel()

				return
			}
		}
	}
}

func onlineBackupToolNode(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
) (string, error) {
	if !req.Online {
		return "", nil
	}

	info, err := inspectPVC(ctx, client, req.Namespace, req.PVCName, true, true, false)
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
