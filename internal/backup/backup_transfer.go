package backup

import (
	"context"
	"errors"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// backupTransfer owns S3 locking and tool execution. Its input is the resolved
// Backup CRD plan; it has no access to workflow storage or another operation.
type backupTransfer struct {
	client kubernetes.Interface
	locker kube.SessionLocker
	store  S3RepositoryStore
	tools  ToolRuntime
}

func (b *backupTransfer) toolRequest(
	namespace, id string, plan v1alpha1.BackupPlan, writableMount bool, rcloneConfig string,
	helmValues *kube.HelmOverrides,
) (pvmigrate.Backup, error) {
	if err := requireS3RepositoryBackend(b.store); err != nil {
		return pvmigrate.Backup{}, err
	}

	configValue, err := rcloneConfigHelmValue(rcloneConfig)
	if err != nil {
		return pvmigrate.Backup{}, err
	}

	overrides := kube.HelmOverrides{}
	if helmValues != nil {
		overrides.Values = append([]string(nil), helmValues.Values...)
		overrides.StringValues = append([]string(nil), helmValues.StringValues...)
	}

	storeConfig := b.store.Config()

	imageValues, err := kube.ToolImageHelmValues(plan.ToolImage)
	if err != nil {
		return pvmigrate.Backup{}, err
	}

	if writableMount {
		// The upstream bucket-storage chart forces Backup mounts read-only. An
		// active OpenEBS LVM volume with shared=yes must use the same writable
		// mount contract as online migration, otherwise the CSI driver rejects
		// the second mount even though shared mode is enabled. Helm's map merge
		// replaces arrays as a whole, so preserve the base mount fields in one
		// override instead of replacing the array with only readOnly.
		overrides.StringValues = append(
			overrides.StringValues,
			"rclone.pvcMounts[0].name="+plan.SourcePVC.Name+
				",rclone.pvcMounts[0].mountPath=/data,rclone.pvcMounts[0].readOnly=false",
		)
	}

	return pvmigrate.Backup{
		ID: id,
		PVC: pvmigrate.PVC{
			KubeconfigPath: b.tools.KubeconfigPath,
			Context:        b.tools.KubeContext,
			Namespace:      namespace,
			Name:           plan.SourcePVC.Name,
		},
		Backend:          string(domain.BackupBackendS3),
		Bucket:           storeConfig.Bucket,
		Name:             storeConfig.Name,
		Path:             plan.Path,
		Prefix:           storeConfig.Prefix,
		RcloneConfigFile: upstreamRawConfigSentinel(),
		Remote:           b.store.RemotePath(),
		RcloneExtraArgs:  rclonePreserveLinksArgs,
		// Consumer policy is enforced by inspectPVC immediately before launch.
		// Upstream also counts terminal Pods, so its broader mounted check must
		// not override the phase-aware result.
		IgnoreMounted: true,
		HelmValues: append(
			kube.ToolSecurityContextHelmValues(),
			overrides.Values...,
		),
		// Keep the generated credential-bearing config last so generic scheduling
		// overrides cannot replace the store selected for this operation.
		HelmStringValues: append(
			append(
				append(kube.ZeroResourceHelmValues(), imageValues...),
				overrides.StringValues...,
			),
			configValue,
		),
		HelmTimeout:    toolHelmTimeout(b.tools.HelmTimeout),
		Writer:         b.tools.Writer,
		Logger:         b.tools.Logger,
		StructuredLogs: true,
	}, nil
}

func (b *backupTransfer) run(
	ctx context.Context,
	namespace, id, lockNamespace, manifestID string,
	plan v1alpha1.BackupPlan,
	writableMount bool,
) (retErr error) {
	targetCtx, targetLock, cancelTarget, err := acquireBackupTargetLock(
		ctx, b.locker, lockNamespace, b.store, b.tools.Logger,
	)
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
						b.store.Destination(),
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
					wrapBackupTargetLockError(
						b.store.Destination(), "delete backup target Lease", deleteErr,
					),
				)
			}
		}()
	}

	holder, err := operationLockHolder(id)
	if err != nil {
		return err
	}

	logOperation(
		b.tools.Logger,
		"acquiring backup operation lock",
		"namespace",
		namespace,
		"pvc",
		plan.SourcePVC.Name,
	)

	lockETag, err := b.store.AcquireLock(
		ctx,
		holder,
		operationLockTTL(ctx, b.tools.HelmTimeout),
	)
	if err != nil {
		return err
	}

	logOperation(
		b.tools.Logger,
		"backup operation lock acquired",
		"namespace",
		namespace,
		"pvc",
		plan.SourcePVC.Name,
	)

	lease := &lockLease{etag: lockETag}
	leaseCtx, cancelLease := context.WithCancel(ctx)
	leaseErrors := make(chan error, 1)
	leaseDone := make(chan struct{})

	go renewObjectStoreLock(
		leaseCtx,
		cancelLease,
		b.store,
		holder,
		operationLockTTL(ctx, b.tools.HelmTimeout),
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
				return b.store.ReleaseLock(releaseCtx, lease.current())
			},
		); releaseErr != nil {
			// The renewal goroutine can race the deferred release: its Put
			// may land server-side after the cancel already discarded the
			// fresh ETag, so the conditional delete reports a conflict even
			// though this holder published successfully. A conflict on
			// release after a completed run means the lock object is stale,
			// not that the backup failed; the lock then ages out by TTL.
			if retErr == nil && domain.CategoryOf(releaseErr) == domain.ErrorConflict {
				logOperation(
					b.tools.Logger,
					"backup operation lock left to expire after release conflict",
					"namespace",
					namespace,
					"pvc",
					plan.SourcePVC.Name,
				)

				return
			}

			retErr = errors.Join(retErr, releaseErr)
		}
	}()

	// A concurrent backup may pass the initial preflight while this operation
	// waits for the distributed lock, so check the immutable recovery point again.
	manifest, err := b.store.Manifest(leaseCtx)
	if err != nil {
		return err
	}

	if manifest != nil {
		if manifestID != "" {
			return validatePublishedBackup(leaseCtx, b.store, manifestID, namespace, plan, manifest)
		}

		return domain.NewError(
			domain.ErrorConflict,
			"backup",
			"S3 completion manifest already exists; use a new backup name to preserve the published recovery point",
		)
	}

	_, currentPV, err := verifyPVCIdentity(
		leaseCtx,
		b.client,
		namespace,
		plan.SourcePVC.Name,
		string(plan.SourcePVC.UID),
		string(plan.SourcePV.UID),
	)
	if err != nil {
		return err
	}

	probeResult, helmValues, err := b.prepareTool(
		leaseCtx,
		namespace,
		id,
		plan,
		writableMount,
		currentPV,
	)
	if err != nil {
		return err
	}

	toolID := toolOperationID(holder)

	backupRequest, err := b.toolRequest(
		namespace, toolID, plan, writableMount,
		b.store.RcloneConfig(),
		&helmValues,
	)
	if err != nil {
		return err
	}

	if err := validateBackupToolLaunch(
		leaseCtx,
		b.client,
		v1alpha1.ObjectReference{
			Namespace: namespace,
			Name:      plan.SourcePVC.Name,
			UID:       types.UID(string(plan.SourcePVC.UID)),
		},
		types.UID(string(plan.SourcePV.UID)),
		plan.Online,
		probeResult.NodeName,
	); err != nil {
		return err
	}

	logOperation(
		b.tools.Logger,
		"starting backup data synchronization",
		"namespace",
		namespace,
		"pvc",
		plan.SourcePVC.Name,
		"toolNode",
		probeResult.NodeName,
	)

	var toolLogs *kube.ToolLogStream
	if b.tools.StreamToolLogs {
		toolLogs = kube.StartPVMigrateToolLogs(leaseCtx, b.client, kube.ToolLogOptions{
			Namespaces: []string{namespace}, OperationID: toolID,
			Writer: b.tools.Writer, Logger: b.tools.Logger,
			Structured: b.tools.StructuredLogs,
		})
	}

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
		b.store,
		holder,
		operationLockTTL(ctx, b.tools.HelmTimeout),
	); err != nil {
		return classifyLeaseError(leaseCtx, err)
	}

	pvc, pv, err := verifyPVCIdentity(
		leaseCtx,
		b.client,
		namespace,
		plan.SourcePVC.Name,
		string(plan.SourcePVC.UID),
		string(plan.SourcePV.UID),
	)
	if err != nil {
		return err
	}

	capacity := pv.Spec.Capacity[corev1.ResourceStorage]

	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}

	logOperation(
		b.tools.Logger,
		"building backup inventory",
		"namespace",
		namespace,
		"pvc",
		plan.SourcePVC.Name,
	)

	inventory, err := b.store.Inventory(leaseCtx)
	if err != nil {
		return err
	}

	if err := checkObjectStoreLease(leaseCtx, leaseErrors); err != nil {
		return err
	}

	if err := lease.renewNow(
		leaseCtx,
		b.store,
		holder,
		operationLockTTL(ctx, b.tools.HelmTimeout),
	); err != nil {
		return classifyLeaseError(leaseCtx, err)
	}

	logOperation(
		b.tools.Logger,
		"publishing backup completion manifest",
		"namespace",
		namespace,
		"pvc",
		plan.SourcePVC.Name,
	)

	return b.store.PutManifest(leaseCtx, objectstore.Manifest{
		CreatedAt:       time.Now().UTC(),
		SessionID:       manifestID,
		SourceNamespace: namespace,
		SourcePVC:       plan.SourcePVC.Name,
		SourcePVCUID:    string(pvc.UID),
		SourcePV:        pv.Name,
		SourcePVUID:     string(pv.UID),
		Path:            plan.Path,
		Capacity:        capacity.String(),
		VolumeMode:      string(mode),
		Consistency:     backupConsistency(plan.Online),
		Compression:     "none",
		ObjectCount:     inventory.ObjectCount,
		TotalBytes:      inventory.TotalBytes,
		InventorySHA256: inventory.SHA256,
	})
}

func (b *backupTransfer) prepareTool(
	ctx context.Context,
	namespace, id string,
	plan v1alpha1.BackupPlan,
	writableMount bool,
	pv *corev1.PersistentVolume,
) (kube.ToolImageProbeResult, kube.HelmOverrides, error) {
	source := v1alpha1.ObjectReference{Namespace: namespace, Name: plan.SourcePVC.Name}

	var (
		toolNode string
		err      error
	)
	if plan.Online {
		toolNode, err = onlineBackupToolNode(ctx, b.client, source)
		if err != nil {
			return kube.ToolImageProbeResult{}, kube.HelmOverrides{}, err
		}
	}

	if toolNode == "" {
		toolNode, err = uniquePVToolNode(ctx, b.client, pv, backupPreflightPhase)
		if err != nil {
			return kube.ToolImageProbeResult{}, kube.HelmOverrides{}, err
		}
	}

	if !plan.Online {
		if err := validateOfflineBackupToolStart(ctx, b.client, source); err != nil {
			return kube.ToolImageProbeResult{}, kube.HelmOverrides{}, err
		}
	}

	probeResult, err := probeRcloneToolImage(
		ctx,
		b.tools.ToolImageProber,
		kube.ToolImageProbeOptions{
			OperationID: id,
			Image:       plan.ToolImage,
			Targets: []kube.ToolProbeTarget{backupToolProbeTarget(
				v1alpha1.ObjectReference{Namespace: namespace, Name: plan.SourcePVC.Name},
				plan.Path, toolNode, plan.Online, writableMount,
			)},
			Timeout: toolHelmTimeout(b.tools.HelmTimeout),
			Writer:  b.tools.Writer,
			Logger:  b.tools.Logger,
		},
	)
	if err != nil {
		return kube.ToolImageProbeResult{}, kube.HelmOverrides{}, err
	}

	helmValues, err := transferToolHelmValues(
		ctx,
		b.client,
		probeResult,
		namespace,
		b.tools.ToolServiceAccountName,
	)
	if err != nil {
		return kube.ToolImageProbeResult{}, kube.HelmOverrides{}, err
	}

	return probeResult, helmValues, nil
}
