package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	restoreLockAnnotation       = "pvc-migrate.io/backup-restore-lock"
	restoreLockExpiryAnnotation = "pvc-migrate.io/backup-restore-lock-expires-at"
)

type Request struct {
	ID                    string
	Namespace             string
	PVCName               string
	Path                  string
	Online                bool
	AllowMounted          bool
	DeleteExtraneousFiles bool
	HelmTimeout           time.Duration
	KubeconfigPath        string
	KubeContext           string
	Store                 *objectstore.Store
	Writer                io.Writer
	Logger                *slog.Logger
}

type Plan struct {
	Operation        string   `json:"operation" yaml:"operation"`
	Namespace        string   `json:"namespace" yaml:"namespace"`
	PVC              string   `json:"pvc" yaml:"pvc"`
	Mode             string   `json:"mode" yaml:"mode"`
	Consistency      string   `json:"consistency" yaml:"consistency"`
	Destination      string   `json:"destination" yaml:"destination"`
	ManifestPresent  bool     `json:"manifestPresent" yaml:"manifestPresent"`
	MountedPods      []string `json:"mountedPods,omitempty" yaml:"mountedPods,omitempty"`
	Capacity         string   `json:"capacity" yaml:"capacity"`
	VolumeMode       string   `json:"volumeMode" yaml:"volumeMode"`
	PVCUID           string   `json:"pvcUID,omitempty" yaml:"pvcUID,omitempty"`
	PVUID            string   `json:"pvUID,omitempty" yaml:"pvUID,omitempty"`
	ObjectCount      int64    `json:"objectCount,omitempty" yaml:"objectCount,omitempty"`
	TotalBytes       int64    `json:"totalBytes,omitempty" yaml:"totalBytes,omitempty"`
	InventorySHA256  string   `json:"inventorySHA256,omitempty" yaml:"inventorySHA256,omitempty"`
	DeleteExtraneous bool     `json:"deleteExtraneous,omitempty" yaml:"deleteExtraneous,omitempty"`
	Compression      string   `json:"compression" yaml:"compression"`
	Warnings         []string `json:"warnings,omitempty" yaml:"warnings,omitempty"`
}

type Result struct {
	Operation   string `json:"operation" yaml:"operation"`
	Namespace   string `json:"namespace" yaml:"namespace"`
	PVC         string `json:"pvc" yaml:"pvc"`
	Name        string `json:"name" yaml:"name"`
	Destination string `json:"destination" yaml:"destination"`
	Mode        string `json:"mode" yaml:"mode"`
	Status      string `json:"status" yaml:"status"`
}

type PVCInfo struct {
	PVC       *corev1.PersistentVolumeClaim
	PV        *corev1.PersistentVolume
	Capacity  resource.Quantity
	Mode      corev1.PersistentVolumeMode
	Consumers []string
	Nodes     []string
}

func Preflight(ctx context.Context, client kubernetes.Interface, req Request, restore bool) (*Plan, error) {
	if client == nil || req.Store == nil {
		return nil, domain.NewError(domain.ErrorInternal, "backup preflight", "Kubernetes client and S3 store are required")
	}
	if err := objectstore.ValidatePath(req.Path); err != nil {
		return nil, err
	}
	info, err := inspectPVC(ctx, client, req.Namespace, req.PVCName, req.Online, req.AllowMounted, restore)
	if err != nil {
		return nil, err
	}
	if err := checkHelperQuota(ctx, client, req.Namespace); err != nil {
		return nil, err
	}
	manifest, err := req.Store.Manifest(ctx)
	if err != nil {
		return nil, err
	}
	plan := &Plan{
		Operation:        "backup",
		Namespace:        req.Namespace,
		PVC:              req.PVCName,
		Mode:             "offline",
		Consistency:      backupConsistency(req.Online),
		Destination:      req.Store.Destination(),
		ManifestPresent:  manifest != nil,
		Capacity:         info.Capacity.String(),
		VolumeMode:       string(info.Mode),
		PVCUID:           string(info.PVC.UID),
		PVUID:            string(info.PV.UID),
		MountedPods:      append([]string(nil), info.Consumers...),
		DeleteExtraneous: restore && req.DeleteExtraneousFiles,
		Compression:      "none",
	}
	if req.Online {
		plan.Mode = "online"
		plan.Consistency = "best-effort crash-consistent file copy"
		if node, nodeErr := onlineRWOConsumerNode(info); nodeErr != nil {
			return nil, nodeErr
		} else if node != "" {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("RWO online helper will be pinned to consumer node %s", node))
		}
	}
	if restore {
		plan.Operation = "restore"
		plan.Mode = "restore"
		plan.Consistency = "destination PVC write; application must be quiesced"
		if manifest == nil {
			return nil, domain.NewError(domain.ErrorPrecondition, "restore preflight", "S3 completion manifest is missing; the backup is not a published recovery point")
		}
		plan.ObjectCount = manifest.ObjectCount
		plan.TotalBytes = manifest.TotalBytes
		plan.InventorySHA256 = manifest.InventorySHA256
		if err := req.Store.VerifyInventory(ctx, *manifest); err != nil {
			return nil, wrapBackupError(domain.ErrorPrecondition, "restore preflight", "verify S3 backup inventory", err)
		}
		if manifest.Capacity != "" {
			backupCapacity, parseErr := resource.ParseQuantity(manifest.Capacity)
			if parseErr != nil {
				return nil, domain.WrapError(domain.ErrorPrecondition, "restore preflight", "parse backup capacity", parseErr)
			}
			if info.Capacity.Cmp(backupCapacity) < 0 {
				return nil, domain.NewError(domain.ErrorPrecondition, "restore preflight", fmt.Sprintf("destination PVC capacity %s is below backup capacity %s", info.Capacity.String(), backupCapacity.String()))
			}
		}
		if manifest.VolumeMode != "" && manifest.VolumeMode != string(info.Mode) {
			return nil, domain.NewError(domain.ErrorPrecondition, "restore preflight", fmt.Sprintf("destination PVC volume mode %s differs from backup volume mode %s", info.Mode, manifest.VolumeMode))
		}
		if manifest.Path != req.Path {
			return nil, domain.NewError(domain.ErrorPrecondition, "restore preflight", fmt.Sprintf("restore path %q differs from backup path %q", req.Path, manifest.Path))
		}
		if req.AllowMounted && len(info.Consumers) > 0 {
			plan.Warnings = append(plan.Warnings, "restore is explicitly allowed while the destination PVC has consumers")
		}
		if req.DeleteExtraneousFiles {
			plan.Warnings = append(plan.Warnings, "restore will delete destination files absent from the published backup")
		}
		return plan, nil
	}
	if manifest != nil {
		return nil, domain.NewError(domain.ErrorConflict, "backup preflight", "S3 completion manifest already exists; use a new backup name to preserve the published recovery point")
	}
	return plan, nil
}

func Run(ctx context.Context, client kubernetes.Interface, req Request, restore bool) error {
	plan, err := Preflight(ctx, client, req, restore)
	if err != nil {
		return err
	}
	if restore {
		return runRestore(ctx, client, req, plan.PVCUID, plan.PVUID, plan.ObjectCount, plan.TotalBytes, plan.InventorySHA256)
	}
	return runBackup(ctx, client, req, plan.PVCUID, plan.PVUID)
}

func pvmigrateBackupRequest(req Request, configPath string, helmValues []string) pvmigrate.Backup {
	return pvmigrate.Backup{
		ID:               req.ID,
		ImageTag:         kube.PVMigrateImageTag,
		PVC:              pvmigrate.PVC{KubeconfigPath: req.KubeconfigPath, Context: req.KubeContext, Namespace: req.Namespace, Name: req.PVCName},
		Backend:          "s3",
		Bucket:           req.Store.Config().Bucket,
		Name:             req.Store.Config().Name,
		Path:             req.Path,
		Prefix:           req.Store.Config().Prefix,
		RcloneConfigFile: configPath,
		Remote:           req.Store.RemotePath(),
		IgnoreMounted:    req.Online,
		HelmStringValues: append(kube.ZeroResourceHelmValues(), helmValues...),
		HelmTimeout:      req.HelmTimeout,
		Writer:           req.Writer,
		Logger:           req.Logger,
		StructuredLogs:   true,
	}
}

func pvmigrateRestoreRequest(req Request, configPath string) pvmigrate.Restore {
	return pvmigrate.Restore{
		ID:                    req.ID,
		ImageTag:              kube.PVMigrateImageTag,
		PVC:                   pvmigrate.PVC{KubeconfigPath: req.KubeconfigPath, Context: req.KubeContext, Namespace: req.Namespace, Name: req.PVCName},
		Backend:               "s3",
		Bucket:                req.Store.Config().Bucket,
		Name:                  req.Store.Config().Name,
		Path:                  req.Path,
		Prefix:                req.Store.Config().Prefix,
		RcloneConfigFile:      configPath,
		Remote:                req.Store.RemotePath(),
		IgnoreMounted:         req.AllowMounted,
		DeleteExtraneousFiles: req.DeleteExtraneousFiles,
		HelmStringValues:      kube.ZeroResourceHelmValues(),
		HelmTimeout:           req.HelmTimeout,
		Writer:                req.Writer,
		Logger:                req.Logger,
		StructuredLogs:        true,
	}
}

func runBackup(ctx context.Context, client kubernetes.Interface, req Request, expectedPVCUID, expectedPVUID string) (retErr error) {
	holder, err := operationLockHolder(req.ID)
	if err != nil {
		return err
	}
	lockETag, err := req.Store.AcquireLock(ctx, holder, operationLockTTL(ctx, req.HelmTimeout))
	if err != nil {
		return err
	}
	lease := &lockLease{etag: lockETag}
	leaseCtx, cancelLease := context.WithCancel(ctx)
	leaseErrors := make(chan error, 1)
	leaseDone := make(chan struct{})
	go renewObjectStoreLock(leaseCtx, cancelLease, req.Store, holder, operationLockTTL(ctx, req.HelmTimeout), lease, leaseErrors, leaseDone)
	defer func() {
		cancelLease()
		<-leaseDone
		if retErr == nil {
			select {
			case leaseErr := <-leaseErrors:
				retErr = leaseErr
			default:
			}
		}
		if releaseErr := req.Store.ReleaseLock(context.Background(), lease.current()); retErr == nil && releaseErr != nil {
			retErr = releaseErr
		}
	}()
	// A concurrent backup may pass the initial preflight while this operation
	// waits for the distributed lock, so check the immutable recovery point again.
	manifest, err := req.Store.Manifest(ctx)
	if err != nil {
		return err
	}
	if manifest != nil {
		return domain.NewError(domain.ErrorConflict, "backup", "S3 completion manifest already exists; use a new backup name to preserve the published recovery point")
	}
	if _, _, err := verifyPVCIdentity(ctx, client, req.Namespace, req.PVCName, expectedPVCUID, expectedPVUID); err != nil {
		return err
	}
	helmValues, err := onlineBackupHelmValues(ctx, client, req)
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
	if err := validateBackupHelperStart(ctx, client, req); err != nil {
		return err
	}
	helperRequest := req
	helperRequest.ID = helperOperationID(holder)
	if err := pvmigrate.RunBackup(leaseCtx, pvmigrateBackupRequest(helperRequest, configPath, helmValues)); err != nil {
		select {
		case leaseErr := <-leaseErrors:
			return leaseErr
		default:
		}
		return classifySyncError(leaseCtx, "backup", err)
	}
	if err := checkObjectStoreLease(leaseCtx, leaseErrors, "backup"); err != nil {
		return err
	}
	if err := lease.renewNow(leaseCtx, req.Store, holder, operationLockTTL(ctx, req.HelmTimeout)); err != nil {
		return classifyLeaseError(leaseCtx, "backup", err)
	}
	pvc, pv, err := verifyPVCIdentity(leaseCtx, client, req.Namespace, req.PVCName, expectedPVCUID, expectedPVUID)
	if err != nil {
		return err
	}
	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}
	inventory, err := req.Store.Inventory(leaseCtx)
	if err != nil {
		return err
	}
	if err := checkObjectStoreLease(leaseCtx, leaseErrors, "backup"); err != nil {
		return err
	}
	if err := lease.renewNow(leaseCtx, req.Store, holder, operationLockTTL(ctx, req.HelmTimeout)); err != nil {
		return classifyLeaseError(leaseCtx, "backup", err)
	}
	return req.Store.PutManifest(leaseCtx, objectstore.Manifest{
		CreatedAt:       time.Now().UTC(),
		SourceNamespace: req.Namespace,
		SourcePVC:       req.PVCName,
		SourcePVCUID:    string(pvc.UID),
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

func validateBackupHelperStart(ctx context.Context, client kubernetes.Interface, req Request) error {
	if req.Online {
		return nil
	}
	// A consumer can appear after Preflight while the operation waits for the
	// object-store lock and helper setup. Recheck immediately before the helper
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

func (l *lockLease) renewNow(ctx context.Context, store *objectstore.Store, holder string, ttl time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	etag, err := store.RenewLock(ctx, holder, l.etag, ttl)
	if err != nil {
		return err
	}
	l.etag = etag
	return nil
}

func checkObjectStoreLease(ctx context.Context, leaseErrors <-chan error, operation string) error {
	select {
	case err := <-leaseErrors:
		var typed *domain.Error
		if errors.As(err, &typed) && typed.Category == domain.ErrorTimeout {
			return err
		}
		return classifyLeaseError(ctx, operation, domain.WrapError(domain.ErrorConflict, "S3 lock", "S3 lock ownership was lost", err))
	default:
	}
	if err := ctx.Err(); err != nil {
		return classifyLeaseError(ctx, operation, err)
	}
	return nil
}

func classifyLeaseError(ctx context.Context, operation string, err error) error {
	if domain.CategoryOf(err) == domain.ErrorTimeout || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
		return domain.WrapError(domain.ErrorTimeout, operation, "S3 lock lease ended before the backup was published", err)
	}
	if domain.CategoryOf(err) == domain.ErrorConflict {
		return err
	}
	return err
}

func wrapBackupError(fallback domain.ErrorCategory, operation, message string, err error) error {
	var typed *domain.Error
	if errors.As(err, &typed) {
		fallback = typed.Category
	}
	return domain.WrapError(fallback, operation, message, err)
}

func operationLockHolder(operationID string) (string, error) {
	attempt, err := domain.NewSessionID(time.Now())
	if err != nil {
		return "", err
	}
	// The request ID identifies the logical recovery point and may be reused
	// for a retry. The lock holder identifies this process attempt, so every
	// invocation gets a distinct holder even when --id is the same.
	if operationID != "" {
		attempt = operationID + "/" + attempt
	}
	return objectstore.LockHolder(attempt), nil
}

func helperOperationID(holder string) string {
	digest := sha256.Sum256([]byte(holder))
	// pv-migrate embeds this value in Helm and Kubernetes names. Keep it
	// lowercase, DNS-safe, and below its 24-character identifier limit.
	return "pm-" + hex.EncodeToString(digest[:8])
}

func operationLockTTL(ctx context.Context, helmTimeout time.Duration) time.Duration {
	if helmTimeout <= 0 {
		helmTimeout = 30 * time.Minute
	}
	ttl := helmTimeout + 10*time.Minute
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline) + 10*time.Minute
		if remaining > ttl {
			ttl = remaining
		}
	}
	return ttl
}

func renewObjectStoreLock(ctx context.Context, cancel context.CancelFunc, store *objectstore.Store, holder string, ttl time.Duration, lease *lockLease, leaseErrors chan<- error, done chan<- struct{}) {
	defer close(done)
	interval := ttl / 3
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	if interval > time.Minute {
		interval = time.Minute
	}
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

func onlineBackupHelmValues(ctx context.Context, client kubernetes.Interface, req Request) ([]string, error) {
	if !req.Online {
		return nil, nil
	}
	info, err := inspectPVC(ctx, client, req.Namespace, req.PVCName, true, true, false)
	if err != nil {
		return nil, err
	}
	node, err := onlineRWOConsumerNode(info)
	if err != nil {
		return nil, err
	}
	if node == "" {
		return nil, nil
	}
	return []string{"rclone.nodeName=" + node}, nil
}

func onlineRWOConsumerNode(info *PVCInfo) (string, error) {
	if len(info.Consumers) == 0 || !hasRWO(info.PVC) {
		return "", nil
	}
	if len(info.Nodes) != len(info.Consumers) {
		return "", domain.NewError(domain.ErrorPrecondition, "online backup scheduling", "every mounted consumer must be scheduled before an RWO online backup")
	}
	node := info.Nodes[0]
	for _, candidate := range info.Nodes[1:] {
		if candidate != node {
			return "", domain.NewError(domain.ErrorPrecondition, "online backup scheduling", fmt.Sprintf("RWO PVC consumers span nodes %s and %s", node, candidate))
		}
	}
	return node, nil
}

func backupConsistency(online bool) string {
	if online {
		return "best-effort crash-consistent file copy"
	}
	return "offline file-consistent copy"
}

func runRestore(ctx context.Context, client kubernetes.Interface, req Request, expectedPVCUID, expectedPVUID string, expectedObjectCount, expectedTotalBytes int64, expectedInventorySHA256 string) (retErr error) {
	holder, err := operationLockHolder(req.ID)
	if err != nil {
		return err
	}
	ttl := operationLockTTL(ctx, req.HelmTimeout)
	unlock, err := acquireRestoreLock(ctx, client, req.Namespace, req.PVCName, holder, ttl, expectedPVCUID)
	if err != nil {
		return err
	}
	leaseCtx, cancelLease := context.WithCancel(ctx)
	leaseErrors := make(chan error, 1)
	leaseDone := make(chan struct{})
	go renewRestoreLock(leaseCtx, cancelLease, client, req.Namespace, req.PVCName, holder, ttl, leaseErrors, leaseDone)
	defer func() {
		cancelLease()
		<-leaseDone
		if retErr == nil {
			select {
			case leaseErr := <-leaseErrors:
				retErr = leaseErr
			default:
			}
		}
		if releaseErr := unlock(context.Background()); retErr == nil && releaseErr != nil {
			retErr = releaseErr
		}
	}()
	if _, _, err := verifyPVCIdentity(ctx, client, req.Namespace, req.PVCName, expectedPVCUID, expectedPVUID); err != nil {
		return err
	}
	// A controller can recreate a consumer after the initial preflight. Recheck
	// immediately before the helper mounts the destination so restore never
	// silently writes into a newly active workload unless explicitly allowed.
	if _, err := inspectPVC(ctx, client, req.Namespace, req.PVCName, false, req.AllowMounted, true); err != nil {
		return err
	}
	expectedInventory := objectstore.Manifest{ObjectCount: expectedObjectCount, TotalBytes: expectedTotalBytes, InventorySHA256: expectedInventorySHA256}
	if err := req.Store.VerifyInventory(ctx, expectedInventory); err != nil {
		return wrapBackupError(domain.ErrorPrecondition, "restore", "verify S3 backup inventory before synchronization", err)
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
	helperRequest := req
	helperRequest.ID = helperOperationID(holder)
	if err := pvmigrate.RunRestore(leaseCtx, pvmigrateRestoreRequest(helperRequest, configPath)); err != nil {
		select {
		case leaseErr := <-leaseErrors:
			return leaseErr
		default:
		}
		return classifySyncError(leaseCtx, "restore", err)
	}
	if _, _, err := verifyPVCIdentity(ctx, client, req.Namespace, req.PVCName, expectedPVCUID, expectedPVUID); err != nil {
		return err
	}
	if err := req.Store.VerifyInventory(ctx, expectedInventory); err != nil {
		return wrapBackupError(domain.ErrorConflict, "restore", "S3 backup inventory changed during synchronization", err)
	}
	return nil
}

func classifySyncError(ctx context.Context, operation string, err error) error {
	contextErr := ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(contextErr, context.DeadlineExceeded) {
		return domain.WrapError(domain.ErrorTimeout, operation, "S3 data synchronization deadline exceeded", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(contextErr, context.Canceled) {
		return domain.WrapError(domain.ErrorTimeout, operation, "S3 data synchronization canceled", err)
	}
	return domain.WrapError(domain.ErrorCopy, operation, "S3 data synchronization failed", err)
}

func inspectPVC(ctx context.Context, client kubernetes.Interface, namespace, name string, online, allowMounted, restore bool) (*PVCInfo, error) {
	pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "backup preflight", "read PVC", err)
	}
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return nil, domain.NewError(domain.ErrorPrecondition, "backup preflight", fmt.Sprintf("PVC %s/%s must be Bound", namespace, name))
	}
	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}
	if mode != corev1.PersistentVolumeFilesystem {
		return nil, domain.NewError(domain.ErrorPrecondition, "backup preflight", "S3 backup and restore require a Filesystem PVC")
	}
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "backup preflight", "read PV", err)
	}
	capacity, ok := pv.Spec.Capacity[corev1.ResourceStorage]
	if !ok || capacity.Sign() <= 0 {
		return nil, domain.NewError(domain.ErrorPrecondition, "backup preflight", "PV has no positive storage capacity")
	}
	consumerNames, consumerNodes, err := pvcConsumerDetails(ctx, client, namespace, name)
	if err != nil {
		return nil, err
	}
	switch {
	case restore:
		if !kube.HasWritableAccessMode(pvc.Spec.AccessModes) {
			return nil, domain.NewError(domain.ErrorPrecondition, "restore preflight", "destination PVC has no writable access mode")
		}
		if len(consumerNames) > 0 && !allowMounted {
			return nil, domain.NewError(domain.ErrorPrecondition, "restore preflight", fmt.Sprintf("destination PVC is referenced by Pod(s) %s", strings.Join(consumerNames, ",")))
		}
		if len(consumerNames) > 0 && hasRWOP(pvc) {
			return nil, domain.NewError(domain.ErrorPrecondition, "restore preflight", "restore cannot mount an active ReadWriteOncePod PVC")
		}
	case !online && len(consumerNames) > 0:
		return nil, domain.NewError(domain.ErrorPrecondition, "backup preflight", fmt.Sprintf("source PVC is referenced by Pod(s) %s", strings.Join(consumerNames, ",")))
	case online && hasRWOP(pvc) && len(consumerNames) > 0:
		return nil, domain.NewError(domain.ErrorPrecondition, "backup preflight", "online backup cannot mount an active ReadWriteOncePod PVC")
	}
	return &PVCInfo{PVC: pvc, PV: pv, Capacity: capacity, Mode: mode, Consumers: consumerNames, Nodes: consumerNodes}, nil
}

func checkHelperQuota(ctx context.Context, client kubernetes.Interface, namespace string) error {
	quotas, err := client.CoreV1().ResourceQuotas(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "backup preflight", "list helper ResourceQuotas", err)
	}
	demand := helperQuotaDemand()
	violations := make([]string, 0)
	for _, quota := range quotas.Items {
		// The zero-resource policy deliberately omits an ephemeral-storage
		// limit. Kubernetes requires an explicit limit when a namespace quota
		// tracks limits.ephemeral-storage, so this helper Pod would be rejected
		// by admission even though its requested quantity is zero.
		if _, bounded := quota.Spec.Hard[corev1.ResourceLimitsEphemeralStorage]; bounded {
			violations = append(violations, fmt.Sprintf("%s/%s %s: helper omits an ephemeral-storage limit required by this quota", namespace, quota.Name, corev1.ResourceLimitsEphemeralStorage))
		}
		for name, requested := range demand {
			hard, bounded := quota.Spec.Hard[name]
			if !bounded {
				continue
			}
			used := quota.Status.Used[name]
			total := used.DeepCopy()
			total.Add(requested)
			if total.Cmp(hard) > 0 {
				violations = append(violations, fmt.Sprintf("%s/%s %s: used %s + helper demand %s exceeds hard %s", namespace, quota.Name, name, used.String(), requested.String(), hard.String()))
			}
		}
	}
	if len(violations) > 0 {
		sort.Strings(violations)
		return domain.NewError(domain.ErrorPrecondition, "backup preflight", "helper resources exceed namespace quota: "+strings.Join(violations, "; "))
	}
	limitRanges, err := client.CoreV1().LimitRanges(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "backup preflight", "list helper LimitRanges", err)
	}
	for _, limitRange := range limitRanges.Items {
		for _, item := range limitRange.Spec.Limits {
			if item.Type != corev1.LimitTypeContainer && item.Type != corev1.LimitTypePod {
				continue
			}
			for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
				if minimum, ok := item.Min[name]; ok && minimum.Sign() > 0 {
					return domain.NewError(domain.ErrorPrecondition, "backup preflight", fmt.Sprintf("helper resource %s=0 is below LimitRange %s minimum %s", name, limitRange.Name, minimum.String()))
				}
			}
		}
	}
	return nil
}

// helperQuotaDemand describes the objects created by the embedded pv-migrate
// bucket-storage chart. Every run creates one ServiceAccount, Job, and Job Pod
// plus one chart Secret. Helm's default Secret storage driver creates a second
// Secret for the release record; configmap storage creates a release ConfigMap.
// Memory and SQL drivers keep release state outside the namespace.
func helperQuotaDemand() map[corev1.ResourceName]resource.Quantity {
	demand := map[corev1.ResourceName]resource.Quantity{}
	add := func(name corev1.ResourceName, count int64) {
		demand[name] = *resource.NewQuantity(count, resource.DecimalSI)
	}
	add(corev1.ResourcePods, 1)
	add(corev1.ResourceName("count/pods"), 1)
	add(corev1.ResourceName("count/jobs.batch"), 1)
	add(corev1.ResourceName("jobs.batch"), 1)
	add(corev1.ResourceName("count/serviceaccounts"), 1)
	add(corev1.ResourceName("serviceaccounts"), 1)
	add(corev1.ResourceName("count/secrets"), 1)
	add(corev1.ResourceName("secrets"), 1)

	switch strings.ToLower(strings.TrimSpace(os.Getenv("HELM_DRIVER"))) {
	case "configmap", "configmaps":
		add(corev1.ResourceConfigMaps, 1)
		add(corev1.ResourceName("count/configmaps"), 1)
	case "memory", "sql":
		// The release record is kept outside the namespace.
	default:
		// Helm defaults to the Secret driver when HELM_DRIVER is empty. Treat
		// unknown values conservatively; Helm will either use a Secret driver
		// alias or fail before creating helper resources.
		add(corev1.ResourceSecrets, 2)
		add(corev1.ResourceName("count/secrets"), 2)
	}
	return demand
}

func pvcConsumerDetails(ctx context.Context, client kubernetes.Interface, namespace, claim string) ([]string, []string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, domain.WrapError(domain.ErrorKubernetes, "backup preflight", "list PVC consumers", err)
	}
	consumers := make([]string, 0)
	nodes := make([]string, 0)
	for i := range pods.Items {
		pod := &pods.Items[i]
		// Succeeded and Failed Pods have no running containers that can write to
		// their volumes. Keeping them out of the consumer set prevents stale
		// terminal objects from blocking an offline operation while active and
		// pending Pods remain protected.
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim {
				consumers = append(consumers, pod.Name)
				if pod.Spec.NodeName != "" {
					nodes = append(nodes, pod.Spec.NodeName)
				}
				break
			}
		}
	}
	return consumers, nodes, nil
}

func hasRWOP(pvc *corev1.PersistentVolumeClaim) bool {
	for _, mode := range pvc.Spec.AccessModes {
		if mode == corev1.ReadWriteOncePod {
			return true
		}
	}
	return false
}

func hasRWO(pvc *corev1.PersistentVolumeClaim) bool {
	for _, mode := range pvc.Spec.AccessModes {
		if mode == corev1.ReadWriteOnce || mode == corev1.ReadWriteOncePod {
			return true
		}
	}
	return false
}

func acquireRestoreLock(ctx context.Context, client kubernetes.Interface, namespace, name, holder string, ttl time.Duration, expectedPVCUID string) (func(context.Context) error, error) {
	annotation := restoreLockAnnotation
	var originalUID string
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		originalUID = string(pvc.UID)
		if expectedPVCUID != "" && originalUID != expectedPVCUID {
			return domain.NewError(domain.ErrorConflict, "restore lock", "destination PVC identity changed since preflight")
		}
		if owner := pvc.Annotations[annotation]; owner != "" && owner != holder {
			expiresAt, parseErr := time.Parse(time.RFC3339Nano, pvc.Annotations[restoreLockExpiryAnnotation])
			if parseErr != nil || time.Now().UTC().Before(expiresAt) {
				return domain.NewError(domain.ErrorConflict, "restore lock", fmt.Sprintf("PVC is locked by %s", owner))
			}
		}
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		pvc.Annotations[annotation] = holder
		pvc.Annotations[restoreLockExpiryAnnotation] = time.Now().UTC().Add(ttl).Format(time.RFC3339Nano)
		_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
			return nil, domain.WrapError(domain.ErrorTimeout, "restore lock", "acquire PVC restore lock timed out", err)
		}
		if apierrors.IsConflict(err) {
			return nil, domain.WrapError(domain.ErrorConflict, "restore lock", "PVC changed while acquiring lock", err)
		}
		return nil, domain.WrapError(domain.ErrorKubernetes, "restore lock", "acquire PVC lock", err)
	}
	return func(releaseCtx context.Context) error {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(releaseCtx, name, metav1.GetOptions{})
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
			_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(releaseCtx, pvc, metav1.UpdateOptions{})
			return err
		})
	}, nil
}

func verifyPVCIdentity(ctx context.Context, client kubernetes.Interface, namespace, name, expectedPVCUID, expectedPVUID string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, error) {
	pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, domain.WrapError(domain.ErrorKubernetes, "backup identity", "read PVC", err)
	}
	if expectedPVCUID != "" && string(pvc.UID) != expectedPVCUID {
		return nil, nil, domain.NewError(domain.ErrorConflict, "backup identity", "PVC identity changed since preflight")
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		return nil, nil, domain.NewError(domain.ErrorPrecondition, "backup identity", "PVC is no longer Bound")
	}
	if pvc.Spec.VolumeName == "" {
		return nil, nil, domain.NewError(domain.ErrorPrecondition, "backup identity", "PVC is no longer bound to a PV")
	}
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, domain.WrapError(domain.ErrorKubernetes, "backup identity", "read PV", err)
	}
	if expectedPVUID != "" && string(pv.UID) != expectedPVUID {
		return nil, nil, domain.NewError(domain.ErrorConflict, "backup identity", "PV identity changed since preflight")
	}
	if pv.Status.Phase != corev1.VolumeBound {
		return nil, nil, domain.NewError(domain.ErrorPrecondition, "backup identity", "PV is no longer Bound")
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != namespace || pv.Spec.ClaimRef.Name != name || (pv.Spec.ClaimRef.UID != "" && pv.Spec.ClaimRef.UID != pvc.UID) {
		return nil, nil, domain.NewError(domain.ErrorConflict, "backup identity", "PVC and PV claimRef no longer identify the same binding")
	}
	return pvc, pv, nil
}

func renewRestoreLock(ctx context.Context, cancel context.CancelFunc, client kubernetes.Interface, namespace, name, holder string, ttl time.Duration, leaseErrors chan<- error, done chan<- struct{}) {
	defer close(done)
	interval := ttl / 3
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				if pvc.Annotations[restoreLockAnnotation] != holder {
					return domain.NewError(domain.ErrorConflict, "restore lock", "PVC lock ownership changed during renewal")
				}
				if pvc.Annotations == nil {
					pvc.Annotations = map[string]string{}
				}
				pvc.Annotations[restoreLockExpiryAnnotation] = time.Now().UTC().Add(ttl).Format(time.RFC3339Nano)
				_, err = client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
				return err
			})
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

func classifyRestoreLockError(ctx context.Context, err error) error {
	if domain.CategoryOf(err) == domain.ErrorTimeout || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
		return domain.WrapError(domain.ErrorTimeout, "restore lock", "renew PVC restore lock timed out", err)
	}
	return domain.WrapError(domain.ErrorConflict, "restore lock", "renew PVC restore lock", err)
}
