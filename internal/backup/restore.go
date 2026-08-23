package backup

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	restoreLockAnnotation       = "pvc-migrate.io/backup-restore-lock"
	restoreLockExpiryAnnotation = "pvc-migrate.io/backup-restore-lock-expires-at"
)

func pvmigrateRestoreRequest(
	req Request,
	configPath string,
	helmValues []string,
) (pvmigrate.Restore, error) {
	imageValues, err := kube.ToolImageHelmValues(req.ToolImage)
	if err != nil {
		return pvmigrate.Restore{}, err
	}

	return pvmigrate.Restore{
		ID: req.ID,
		PVC: pvmigrate.PVC{
			KubeconfigPath: req.KubeconfigPath,
			Context:        req.KubeContext,
			Namespace:      req.Namespace,
			Name:           req.PVCName,
		},
		Backend:          "s3",
		Bucket:           req.Store.Config().Bucket,
		Name:             req.Store.Config().Name,
		Path:             req.Path,
		Prefix:           req.Store.Config().Prefix,
		RcloneConfigFile: configPath,
		Remote:           req.Store.RemotePath(),
		RcloneExtraArgs:  rclonePreserveLinksArgs,
		// inspectPVC enforces the requested mounted policy immediately before
		// launch and excludes terminal Pods that upstream still counts.
		IgnoreMounted:         true,
		DeleteExtraneousFiles: req.DeleteExtraneousFiles,
		HelmValues:            kube.ToolSecurityContextHelmValues(),
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

func runRestore(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	expectedPVCUID, expectedPVUID string,
	expectedObjectCount, expectedTotalBytes int64,
	expectedInventorySHA256 string,
) (retErr error) {
	holder, err := operationLockHolder(req.ID)
	if err != nil {
		return err
	}

	ttl := operationLockTTL(ctx, req.HelmTimeout)
	logOperation(
		req,
		"acquiring restore operation lock",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
	)

	unlock, lockedPVCUID, err := acquireRestoreLock(
		ctx,
		client,
		req.Namespace,
		req.PVCName,
		holder,
		ttl,
		expectedPVCUID,
	)
	if err != nil {
		return err
	}

	logOperation(
		req,
		"restore operation lock acquired",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
	)

	leaseCtx, cancelLease := context.WithCancel(ctx)
	leaseErrors := make(chan error, 1)

	leaseDone := make(chan struct{})

	go renewRestoreLock(
		leaseCtx,
		cancelLease,
		client,
		req.Namespace,
		req.PVCName,
		holder,
		lockedPVCUID,
		ttl,
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

		if releaseErr := runWithCleanupTimeout(lockReleaseTimeout, unlock); releaseErr != nil {
			retErr = errors.Join(retErr, releaseErr)
		}
	}()

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
	// A controller can recreate a consumer after the initial preflight. Recheck
	// immediately before the tool mounts the destination so restore never
	// silently writes into a newly active workload unless explicitly allowed.
	currentInfo, err := inspectPVC(
		leaseCtx,
		client,
		req.Namespace,
		req.PVCName,
		false,
		req.AllowMounted,
		true,
	)
	if err != nil {
		return err
	}

	expectedInventory := objectstore.Manifest{
		ObjectCount:     expectedObjectCount,
		TotalBytes:      expectedTotalBytes,
		InventorySHA256: expectedInventorySHA256,
	}

	logOperation(
		req,
		"verifying backup inventory before restore synchronization",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
	)

	if err := req.Store.VerifyInventory(leaseCtx, expectedInventory); err != nil {
		return wrapBackupError(
			domain.ErrorPrecondition,
			"restore",
			"verify S3 backup inventory before synchronization",
			err,
		)
	}

	consumerNode, err := rwoConsumerNode(currentInfo, "restore scheduling")
	if err != nil {
		return err
	}

	if consumerNode != "" && req.TargetNode != "" {
		if _, err := selectRestoreToolNode(req.TargetNode, consumerNode, ""); err != nil {
			return err
		}
	}

	pvNode := ""
	if consumerNode == "" || req.TargetNode != "" {
		pvNode, err = uniquePVToolNode(leaseCtx, client, currentPV, "restore preflight")
		if err != nil {
			return err
		}
	}

	toolNode, err := selectRestoreToolNode(req.TargetNode, consumerNode, pvNode)
	if err != nil {
		return err
	}

	probeResult, err := probeTransferToolImage(leaseCtx, req, toolNode, true)
	if err != nil {
		return err
	}

	helmValues, err := transferToolHelmValues(leaseCtx, client, probeResult)
	if err != nil {
		return err
	}

	configFile, err := os.CreateTemp("", "pvc-migrate-s3-*.conf")
	if err != nil {
		return domain.WrapError(domain.ErrorInternal, "restore", "create temporary S3 config", err)
	}

	configPath := configFile.Name()
	defer os.Remove(configPath)

	if err := configFile.Chmod(0o600); err != nil {
		configFile.Close()
		return domain.WrapError(domain.ErrorInternal, "restore", "protect temporary S3 config", err)
	}

	if _, err := configFile.WriteString(req.Store.RcloneConfig()); err != nil {
		configFile.Close()
		return domain.WrapError(domain.ErrorInternal, "restore", "write temporary S3 config", err)
	}

	if err := configFile.Close(); err != nil {
		return domain.WrapError(domain.ErrorInternal, "restore", "close temporary S3 config", err)
	}

	toolRequest := req
	toolRequest.ID = toolOperationID(holder)

	restoreRequest, err := pvmigrateRestoreRequest(toolRequest, configPath, helmValues)
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
		true,
	); err != nil {
		return err
	}

	logOperation(
		req,
		"starting restore data synchronization",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
		"toolNode",
		probeResult.NodeName,
	)

	toolLogs := startToolLogs(leaseCtx, client, toolRequest)
	toolErr := pvmigrate.RunRestore(leaseCtx, restoreRequest)

	toolLogs.Stop()

	toolErr = errors.Join(toolErr, toolLogs.ObservedError())
	if toolErr != nil {
		return classifyToolAndLeaseError(leaseCtx, "restore", toolErr, leaseErrors)
	}

	if _, _, err := verifyPVCIdentity(
		ctx,
		client,
		req.Namespace,
		req.PVCName,
		expectedPVCUID,
		expectedPVUID,
	); err != nil {
		return err
	}

	logOperation(
		req,
		"verifying backup inventory after restore synchronization",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
	)

	if err := req.Store.VerifyInventory(ctx, expectedInventory); err != nil {
		return wrapBackupError(
			domain.ErrorConflict,
			"restore",
			"S3 backup inventory changed during synchronization",
			err,
		)
	}

	return nil
}

func acquireRestoreLock(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name, holder string,
	ttl time.Duration,
	expectedPVCUID string,
) (func(context.Context) error, string, error) {
	if expectedPVCUID == "" {
		return nil, "", domain.NewError(
			domain.ErrorPrecondition,
			"restore lock",
			"expected PVC identity is required",
		)
	}

	annotation := restoreLockAnnotation

	var originalUID string

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		originalUID = string(pvc.UID)
		if originalUID != expectedPVCUID {
			return domain.NewError(
				domain.ErrorConflict,
				"restore lock",
				"destination PVC identity changed since preflight",
			)
		}

		if owner := pvc.Annotations[annotation]; owner != "" && owner != holder {
			expiresAt, parseErr := time.Parse(
				time.RFC3339Nano,
				pvc.Annotations[restoreLockExpiryAnnotation],
			)
			if parseErr != nil || time.Now().UTC().Before(expiresAt) {
				return domain.NewError(
					domain.ErrorConflict,
					"restore lock",
					"PVC is locked by "+owner,
				)
			}
		}

		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}

		pvc.Annotations[annotation] = holder
		pvc.Annotations[restoreLockExpiryAnnotation] = time.Now().
			UTC().
			Add(ttl).
			Format(time.RFC3339Nano)
		_, err = client.CoreV1().
			PersistentVolumeClaims(namespace).
			Update(ctx, pvc, metav1.UpdateOptions{})

		return err
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
			errors.Is(ctx.Err(), context.DeadlineExceeded) ||
			errors.Is(ctx.Err(), context.Canceled) {
			return nil, "", domain.WrapError(
				domain.ErrorTimeout,
				"restore lock",
				"acquire PVC restore lock timed out",
				err,
			)
		}

		if _, ok := errors.AsType[*domain.Error](err); ok {
			return nil, "", err
		}

		if apierrors.IsConflict(err) {
			return nil, "", domain.WrapError(
				domain.ErrorConflict,
				"restore lock",
				"PVC changed while acquiring lock",
				err,
			)
		}

		return nil, "", domain.WrapError(
			domain.ErrorKubernetes,
			"restore lock",
			"acquire PVC lock",
			err,
		)
	}

	return func(releaseCtx context.Context) error {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			pvc, err := client.CoreV1().
				PersistentVolumeClaims(namespace).
				Get(releaseCtx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return nil
			}

			if err != nil {
				return err
			}

			if string(pvc.UID) != originalUID || pvc.Annotations[annotation] != holder {
				return nil
			}

			delete(pvc.Annotations, annotation)
			delete(pvc.Annotations, restoreLockExpiryAnnotation)
			_, err = client.CoreV1().
				PersistentVolumeClaims(namespace).
				Update(releaseCtx, pvc, metav1.UpdateOptions{})

			return err
		})
	}, originalUID, nil
}

func renewRestoreLock(
	ctx context.Context,
	cancel context.CancelFunc,
	client kubernetes.Interface,
	namespace, name, holder, pvcUID string,
	ttl time.Duration,
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
			err := renewRestoreLockOnce(ctx, client, namespace, name, holder, pvcUID, ttl)
			if err != nil {
				select {
				case leaseErrors <- classifyRestoreLockError(ctx, err):
				default:
				}

				cancel()

				return
			}
		}
	}
}

func renewRestoreLockOnce(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name, holder, pvcUID string,
	ttl time.Duration,
) error {
	if pvcUID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"restore lock",
			"PVC identity is required for lock renewal",
		)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if string(pvc.UID) != pvcUID || pvc.Annotations[restoreLockAnnotation] != holder {
			return domain.NewError(
				domain.ErrorConflict,
				"restore lock",
				"PVC lock ownership changed during renewal",
			)
		}

		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}

		pvc.Annotations[restoreLockExpiryAnnotation] = time.Now().
			UTC().
			Add(ttl).
			Format(time.RFC3339Nano)
		_, err = client.CoreV1().
			PersistentVolumeClaims(namespace).
			Update(ctx, pvc, metav1.UpdateOptions{})

		return err
	})
}

func classifyRestoreLockError(ctx context.Context, err error) error {
	if domain.CategoryOf(err) == domain.ErrorTimeout || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(ctx.Err(), context.Canceled) {
		return domain.WrapError(
			domain.ErrorTimeout,
			"restore lock",
			"renew PVC restore lock timed out",
			err,
		)
	}

	return domain.WrapError(domain.ErrorConflict, "restore lock", "renew PVC restore lock", err)
}
