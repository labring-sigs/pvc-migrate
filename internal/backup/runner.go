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
	"slices"
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
	rclonePreserveLinksArgs     = "--links"
	lockReleaseTimeout          = 10 * time.Second
)

type Mode string

const (
	ModeOffline Mode = "offline"
	ModeOnline  Mode = "online"
	ModeRestore Mode = "restore"
)

type Request struct {
	ID                     string
	ToolImage              string
	Namespace              string
	PVCName                string
	Path                   string
	Online                 bool
	AllowMounted           bool
	DeleteExtraneousFiles  bool
	HelmTimeout            time.Duration
	KubeconfigPath         string
	KubeContext            string
	StreamToolLogs         bool
	StructuredLogs         bool
	Store                  *objectstore.Store
	Writer                 io.Writer
	Logger                 *slog.Logger
	ToolImageProber        kube.ToolImageProber
	SessionStore           kube.SessionStore
	SessionNamespace       string
	OpenEBSLVMEnableShared bool
	OpenEBSLVMManager      kube.OpenEBSLVMSharedVolumeManager
	WritablePVCMount       bool
	BackupSession          *domain.Session
	ObjectStoreFactory     func(context.Context, objectstore.Config) (*objectstore.Store, error)
}

type Plan struct {
	Operation        string   `json:"operation"                  yaml:"operation"`
	ToolImage        string   `json:"toolImage"                  yaml:"toolImage"`
	Namespace        string   `json:"namespace"                  yaml:"namespace"`
	PVC              string   `json:"pvc"                        yaml:"pvc"`
	Path             string   `json:"path"                       yaml:"path"`
	Mode             Mode     `json:"mode"                       yaml:"mode"`
	Consistency      string   `json:"consistency"                yaml:"consistency"`
	Destination      string   `json:"destination"                yaml:"destination"`
	ManifestPresent  bool     `json:"manifestPresent"            yaml:"manifestPresent"`
	MountedPods      []string `json:"mountedPods,omitempty"      yaml:"mountedPods,omitempty"`
	Capacity         string   `json:"capacity"                   yaml:"capacity"`
	VolumeMode       string   `json:"volumeMode"                 yaml:"volumeMode"`
	ToolNode         string   `json:"toolNode,omitempty"         yaml:"toolNode,omitempty"`
	PVCUID           string   `json:"pvcUID,omitempty"           yaml:"pvcUID,omitempty"`
	PVUID            string   `json:"pvUID,omitempty"            yaml:"pvUID,omitempty"`
	ObjectCount      int64    `json:"objectCount,omitempty"      yaml:"objectCount,omitempty"`
	TotalBytes       int64    `json:"totalBytes,omitempty"       yaml:"totalBytes,omitempty"`
	InventorySHA256  string   `json:"inventorySHA256,omitempty"  yaml:"inventorySHA256,omitempty"`
	DeleteExtraneous bool     `json:"deleteExtraneous,omitempty" yaml:"deleteExtraneous,omitempty"`
	Compression      string   `json:"compression"                yaml:"compression"`
	Warnings         []string `json:"warnings,omitempty"         yaml:"warnings,omitempty"`
}

type Result struct {
	Operation   string `json:"operation"             yaml:"operation"`
	OperationID string `json:"operationID,omitempty" yaml:"operationID,omitempty"`
	SessionID   string `json:"sessionID,omitempty"   yaml:"sessionID,omitempty"`
	Namespace   string `json:"namespace"             yaml:"namespace"`
	PVC         string `json:"pvc"                   yaml:"pvc"`
	Path        string `json:"path"                  yaml:"path"`
	Name        string `json:"name"                  yaml:"name"`
	Destination string `json:"destination"           yaml:"destination"`
	Mode        Mode   `json:"mode"                  yaml:"mode"`
	Status      string `json:"status"                yaml:"status"`
}

type PVCInfo struct {
	PVC       *corev1.PersistentVolumeClaim
	PV        *corev1.PersistentVolume
	Capacity  resource.Quantity
	Mode      corev1.PersistentVolumeMode
	Consumers []string
	Nodes     []string
}

func Preflight(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	restore bool,
) (*Plan, error) {
	return preflight(ctx, client, req, restore, "preflight")
}

func preflight(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	restore bool,
	stage string,
) (*Plan, error) {
	operation := "backup"
	if restore {
		operation = "restore"
	}

	logOperation(
		req,
		operation+" "+stage+" started",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
		"path",
		req.Path,
	)

	if client == nil || req.Store == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"backup preflight",
			"Kubernetes client and S3 store are required",
		)
	}

	normalizedPath, err := normalizeObjectTransferPath(req.Path)
	if err != nil {
		return nil, err
	}

	req.Path = normalizedPath
	if err := objectstore.ValidatePath(req.Path); err != nil {
		return nil, err
	}

	toolImage, err := kube.NormalizeToolImage(req.ToolImage)
	if err != nil {
		return nil, err
	}

	var (
		info        *PVCInfo
		infoErr     error
		quotaErr    error
		manifest    *objectstore.Manifest
		manifestErr error
		wg          sync.WaitGroup
	)
	wg.Go(func() {
		logOperation(
			req,
			operation+" "+stage+" inspecting PVC",
			"namespace",
			req.Namespace,
			"pvc",
			req.PVCName,
		)
		info, infoErr = inspectPVC(
			ctx,
			client,
			req.Namespace,
			req.PVCName,
			req.Online,
			req.AllowMounted,
			restore,
		)
	})
	wg.Go(func() {
		logOperation(
			req,
			operation+" "+stage+" checking tool quota",
			"namespace",
			req.Namespace,
			"pvc",
			req.PVCName,
		)
		quotaErr = checkToolQuota(ctx, client, req.Namespace)
	})
	wg.Go(func() {
		logOperation(
			req,
			operation+" "+stage+" checking object-store manifest",
			"namespace",
			req.Namespace,
			"pvc",
			req.PVCName,
		)
		manifest, manifestErr = req.Store.Manifest(ctx)
	})
	wg.Wait()

	if infoErr != nil {
		return nil, infoErr
	}
	if !restore && manifest == nil {
		if err := validateBackupOpenEBSState(ctx, req, info); err != nil {
			return nil, err
		}
	}

	if quotaErr != nil {
		return nil, quotaErr
	}

	if manifestErr != nil {
		return nil, manifestErr
	}

	toolNode, err := uniquePVToolNode(ctx, client, info.PV)
	if err != nil {
		return nil, err
	}

	plan := &Plan{
		Operation:        "backup",
		ToolImage:        toolImage,
		Namespace:        req.Namespace,
		PVC:              req.PVCName,
		Path:             transferDisplayPath(req.Path),
		Mode:             ModeOffline,
		Consistency:      backupConsistency(req.Online),
		Destination:      req.Store.Destination(),
		ManifestPresent:  manifest != nil,
		Capacity:         info.Capacity.String(),
		VolumeMode:       string(info.Mode),
		PVCUID:           string(info.PVC.UID),
		PVUID:            string(info.PV.UID),
		MountedPods:      append([]string(nil), info.Consumers...),
		ToolNode:         toolNode,
		DeleteExtraneous: restore && req.DeleteExtraneousFiles,
		Compression:      "none",
	}
	if req.Online {
		plan.Mode = ModeOnline

		plan.Consistency = "best-effort crash-consistent file copy"
		if node, nodeErr := rwoConsumerNode(info, "online backup scheduling"); nodeErr != nil {
			return nil, nodeErr
		} else if node != "" {
			plan.ToolNode = node
			plan.Warnings = append(
				plan.Warnings,
				"RWO online tool will be pinned to consumer node "+node,
			)
		}
	}

	if restore {
		plan.Operation = "restore"
		plan.Mode = ModeRestore
		plan.Consistency = "destination PVC write; application must be quiesced"

		if manifest == nil {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"restore preflight",
				"S3 completion manifest is missing; the backup is not a published recovery point",
			)
		}

		plan.ObjectCount = manifest.ObjectCount
		plan.TotalBytes = manifest.TotalBytes
		plan.InventorySHA256 = manifest.InventorySHA256

		logOperation(
			req,
			"restore "+stage+" verifying backup inventory",
			"namespace",
			req.Namespace,
			"pvc",
			req.PVCName,
		)

		if err := req.Store.VerifyInventory(ctx, *manifest); err != nil {
			return nil, wrapBackupError(
				domain.ErrorPrecondition,
				"restore preflight",
				"verify S3 backup inventory",
				err,
			)
		}

		if manifest.Capacity != "" {
			backupCapacity, parseErr := resource.ParseQuantity(manifest.Capacity)
			if parseErr != nil {
				return nil, domain.WrapError(
					domain.ErrorPrecondition,
					"restore preflight",
					"parse backup capacity",
					parseErr,
				)
			}

			if info.Capacity.Cmp(backupCapacity) < 0 {
				return nil, domain.NewError(
					domain.ErrorPrecondition,
					"restore preflight",
					fmt.Sprintf(
						"destination PVC capacity %s is below backup capacity %s",
						info.Capacity.String(),
						backupCapacity.String(),
					),
				)
			}
		}

		if manifest.VolumeMode != "" && manifest.VolumeMode != string(info.Mode) {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"restore preflight",
				fmt.Sprintf(
					"destination PVC volume mode %s differs from backup volume mode %s",
					info.Mode,
					manifest.VolumeMode,
				),
			)
		}

		if manifest.Path != req.Path {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"restore preflight",
				fmt.Sprintf("restore path %q differs from backup path %q", req.Path, manifest.Path),
			)
		}

		if req.AllowMounted && len(info.Consumers) > 0 {
			plan.Warnings = append(
				plan.Warnings,
				"restore is explicitly allowed while the destination PVC has consumers",
			)
		}

		if node, nodeErr := rwoConsumerNode(info, "restore scheduling"); nodeErr != nil {
			return nil, nodeErr
		} else if node != "" {
			plan.ToolNode = node
			plan.Warnings = append(
				plan.Warnings,
				"mounted RWO restore tool will be pinned to consumer node "+node,
			)
		}

		if req.DeleteExtraneousFiles {
			plan.Warnings = append(
				plan.Warnings,
				"restore will delete destination files absent from the published backup",
			)
		}

		return plan, nil
	}

	if manifest != nil {
		if req.BackupSession != nil {
			if err := validatePublishedBackupSession(ctx, req, manifest); err != nil {
				return nil, err
			}
			plan.ObjectCount = manifest.ObjectCount
			plan.TotalBytes = manifest.TotalBytes
			plan.InventorySHA256 = manifest.InventorySHA256
			return plan, nil
		}
		return nil, domain.NewError(
			domain.ErrorConflict,
			"backup preflight",
			"S3 completion manifest already exists; use a new backup name to preserve the published recovery point",
		)
	}

	return plan, nil
}

func transferDisplayPath(value string) string {
	if value == "" {
		return domain.VolumeRootPath
	}
	return value
}

func normalizeObjectTransferPath(value string) (string, error) {
	normalized, err := domain.NormalizeTransferPath(value)
	if err != nil {
		return "", domain.WrapError(
			domain.ErrorValidation,
			"transfer path",
			fmt.Sprintf("PVC path %q is invalid", value),
			err,
		)
	}

	if normalized == domain.VolumeRootPath {
		return "", nil
	}

	return normalized, nil
}

func uniquePVToolNode(
	ctx context.Context,
	client kubernetes.Interface,
	pv *corev1.PersistentVolume,
) (string, error) {
	if pv == nil || pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil ||
		len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return "", nil
	}

	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", domain.WrapError(
			domain.ErrorKubernetes,
			"backup preflight",
			"list nodes for PV tool placement",
			err,
		)
	}

	return kube.PVUniqueNodeName(pv, nodes.Items), nil
}

func Run(ctx context.Context, client kubernetes.Interface, req Request, restore bool) error {
	operation := "backup"
	if restore {
		operation = "restore"
	}

	plan, err := preflight(ctx, client, req, restore, "execution revalidation")
	if err != nil {
		return err
	}

	req.Path, err = normalizeObjectTransferPath(req.Path)
	if err != nil {
		return err
	}

	logOperation(
		req,
		operation+" execution started",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
		"toolNode",
		plan.ToolNode,
	)

	if restore {
		return runRestore(
			ctx,
			client,
			req,
			plan.PVCUID,
			plan.PVUID,
			plan.ObjectCount,
			plan.TotalBytes,
			plan.InventorySHA256,
		)
	}

	return runBackupWithSession(ctx, client, req, plan.PVCUID, plan.PVUID)
}

func runBackupWithSession(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	expectedPVCUID, expectedPVUID string,
) (retErr error) {
	if req.SessionStore == nil {
		return runBackup(ctx, client, req, expectedPVCUID, expectedPVUID)
	}

	pvc, pv, err := verifyPVCIdentity(ctx, client, req.Namespace, req.PVCName, expectedPVCUID, expectedPVUID)
	if err != nil {
		return err
	}
	session, err := buildBackupSession(req, pvc, pv)
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
		return domain.NewError(domain.ErrorValidation, "backup resume", "backup session is required")
	}
	if session.Spec.Backup == nil {
		return domain.NewError(domain.ErrorValidation, "backup resume", "backup session payload is required")
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
			cleanupErr := runWithPreservedCleanupTimeout(ctx, lockReleaseTimeout, func(cleanupCtx context.Context) error {
				return restoreBackupSharedMounts(cleanupCtx, req, session)
			})
			if cleanupErr != nil {
				retErr = errors.Join(retErr, cleanupErr)
			}
		}
		if fenceErr := backupSessionFenceError(ctx); fenceErr != nil {
			retErr = errors.Join(retErr, fenceErr)
			return
		}
		if retErr != nil {
			failureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lockReleaseTimeout)
			failureErr := failBackupSession(failureCtx, req, session, retErr)
			cancel()
			retErr = errors.Join(retErr, failureErr)
		}
	}()

	if session.Status.Phase == domain.PhaseCompleted {
		return nil
	}
	if err := updateBackupSession(ctx, req, session, domain.PhaseWarmCopying, "backup preparing source PVC"); err != nil {
		return err
	}

	info, err := inspectPVC(ctx, client, req.Namespace, req.PVCName, req.Online, req.AllowMounted, false)
	if err != nil {
		return err
	}
	if info.PV.Spec.CSI != nil && info.PV.Spec.CSI.Driver == kube.OpenEBSLVMCSIDriver && len(info.Consumers) > 0 {
		req.WritablePVCMount = true
	}
	if err := backupSessionOpenEBSState(ctx, req, session, info); err != nil {
		return err
	}
	if err := updateBackupSession(ctx, req, session, domain.PhaseWarmCopied, "backup source PVC prepared"); err != nil {
		return err
	}

	if err := runBackup(ctx, client, req, expectedPVCUID, expectedPVUID); err != nil {
		return err
	}
	if err := runWithPreservedCleanupTimeout(ctx, lockReleaseTimeout, func(cleanupCtx context.Context) error {
		return restoreBackupSharedMounts(cleanupCtx, req, session)
	}); err != nil {
		return err
	}
	sharedRestored = true

	if err := updateBackupSession(ctx, req, session, domain.PhaseCompleted, "backup completed"); err != nil {
		return err
	}
	return nil
}

func probeTransferToolImage(
	ctx context.Context,
	req Request,
	nodeName string,
	restore bool,
) (kube.ToolImageProbeResult, error) {
	if req.ToolImageProber == nil {
		return kube.ToolImageProbeResult{NodeName: nodeName}, nil
	}

	pvcName := ""
	if nodeName == "" || req.Path != "" || req.Online || req.WritablePVCMount {
		// Let the scheduler resolve the same storage topology as the real rclone
		// Pod when preflight cannot identify one unique node. A selected path
		// must also be checked through the real PVC mount before rclone starts.
		pvcName = req.PVCName
	}

	results, err := req.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
		OperationID: req.ID,
		Image:       req.ToolImage,
		Targets: []kube.ToolProbeTarget{{
			Namespace:        req.Namespace,
			NodeName:         nodeName,
			PVCName:          pvcName,
			RequiredPath:     req.Path,
			CreatePath:       restore && req.Path != "",
			WritablePVCMount: req.WritablePVCMount,
			Components:       []string{kube.ToolComponentRclone},
		}},
		Timeout: toolHelmTimeout(req.HelmTimeout),
		Writer:  req.Writer,
		Logger:  req.Logger,
	})
	if err != nil {
		return kube.ToolImageProbeResult{}, err
	}

	if len(results) != 1 || results[0].NodeName == "" {
		return kube.ToolImageProbeResult{}, domain.NewError(
			domain.ErrorInternal,
			"tool image probe",
			"rclone probe returned no scheduled node",
		)
	}

	return results[0], nil
}

func transferToolHelmValues(
	ctx context.Context,
	client kubernetes.Interface,
	probe kube.ToolImageProbeResult,
) ([]string, error) {
	if probe.NodeName == "" {
		return nil, nil
	}

	node, err := client.CoreV1().Nodes().Get(ctx, probe.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"tool scheduling",
			"read node "+probe.NodeName,
			err,
		)
	}

	values, err := kube.ToolComponentNodeHelmValues(kube.ToolComponentRclone, node)
	if err != nil {
		return nil, err
	}

	pullSecretValues, err := kube.ToolImagePullSecretHelmValues([]kube.ToolImageProbeResult{probe})
	if err != nil {
		return nil, err
	}

	return append(values, pullSecretValues...), nil
}

func validateTransferToolLaunch(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	expectedPVCUID, expectedPVUID string,
	probe kube.ToolImageProbeResult,
	restore bool,
) error {
	info, err := inspectPVC(
		ctx,
		client,
		req.Namespace,
		req.PVCName,
		req.Online,
		req.AllowMounted,
		restore,
	)
	if err != nil {
		return err
	}

	pvc, pv, err := verifyPVCIdentity(
		ctx,
		client,
		req.Namespace,
		req.PVCName,
		expectedPVCUID,
		expectedPVUID,
	)
	if err != nil {
		return err
	}

	if info.PVC.UID != pvc.UID || info.PV.UID != pv.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"tool scheduling",
			"PVC or PV identity changed during final tool launch validation",
		)
	}

	operation := "backup scheduling"
	if restore {
		operation = "restore scheduling"
	}

	requiredNode, err := rwoConsumerNode(info, operation)
	if err != nil {
		return err
	}

	if requiredNode == "" {
		requiredNode, err = uniquePVToolNode(ctx, client, pv)
		if err != nil {
			return err
		}
	}

	if requiredNode != "" && requiredNode != probe.NodeName {
		return domain.NewError(
			domain.ErrorConflict,
			"tool scheduling",
			fmt.Sprintf(
				"required tool node changed from %s to %s during image probe",
				probe.NodeName,
				requiredNode,
			),
		)
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
		Backend:          "s3",
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

func toolHelmTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return 10 * time.Minute
	}
	return timeout
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
				retErr = errors.Join(retErr, wrapBackupTargetLockError(req, "backup target lock ownership was lost", lockErr))
			}
			if deleteErr := runWithCleanupTimeout(
				lockReleaseTimeout,
				targetLock.Delete,
			); deleteErr != nil {
				retErr = errors.Join(retErr, wrapBackupTargetLockError(req, "delete backup target Lease", deleteErr))
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

	currentToolNode, err := onlineBackupToolNode(leaseCtx, client, req)
	if err != nil {
		return err
	}

	if currentToolNode == "" {
		currentToolNode, err = uniquePVToolNode(leaseCtx, client, currentPV)
		if err != nil {
			return err
		}
	}

	if err := validateBackupToolStart(leaseCtx, client, req); err != nil {
		return err
	}

	probeResult, err := probeTransferToolImage(leaseCtx, req, currentToolNode, false)
	if err != nil {
		return err
	}

	helmValues, err := transferToolHelmValues(leaseCtx, client, probeResult)
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
	logOperation(req, "acquiring Kubernetes backup target Lease", "destination", req.Store.Destination())
	lock, err := locker.AcquireSessionLock(ctx, req.SessionNamespace, lockID)
	if err != nil {
		return nil, nil, nil, wrapBackupTargetLockError(
			req,
			"another backup is already changing this recovery point",
			err,
		)
	}

	boundCtx, cancel := lock.Bind(ctx)
	logOperation(req, "Kubernetes backup target Lease acquired", "destination", req.Store.Destination())
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

func wrapBackupError(fallback domain.ErrorCategory, operation, message string, err error) error {
	if typed, ok := errors.AsType[*domain.Error](err); ok {
		fallback = typed.Category
		message += ": " + typed.Error()
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

func toolOperationID(holder string) string {
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

func rwoConsumerNode(info *PVCInfo, operation string) (string, error) {
	if len(info.Consumers) == 0 || !hasRWO(info.PVC) {
		return "", nil
	}

	if len(info.Nodes) != len(info.Consumers) {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			operation,
			"every mounted RWO consumer must be scheduled before launching the tool Pod",
		)
	}

	node := info.Nodes[0]
	for _, candidate := range info.Nodes[1:] {
		if candidate != node {
			return "", domain.NewError(
				domain.ErrorPrecondition,
				operation,
				fmt.Sprintf("RWO PVC consumers span nodes %s and %s", node, candidate),
			)
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

	toolNode, err := rwoConsumerNode(currentInfo, "restore scheduling")
	if err != nil {
		return err
	}

	if toolNode == "" {
		toolNode, err = uniquePVToolNode(leaseCtx, client, currentPV)
		if err != nil {
			return err
		}
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

func logOperation(req Request, message string, args ...any) {
	if req.Logger != nil {
		req.Logger.Info(message, args...)
	}
}

func runWithCleanupTimeout(timeout time.Duration, cleanup func(context.Context) error) error {
	return runWithPreservedCleanupTimeout(context.Background(), timeout, cleanup)
}

func runWithPreservedCleanupTimeout(
	parent context.Context,
	timeout time.Duration,
	cleanup func(context.Context) error,
) error {
	if cleanup == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()

	err := cleanup(ctx)
	if errors.Is(err, context.DeadlineExceeded) ||
		(err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded)) {
		return domain.WrapError(
			domain.ErrorTimeout,
			"release operation lock",
			"lock cleanup deadline exceeded",
			err,
		)
	}

	return err
}

func classifySyncError(ctx context.Context, operation string, err error) error {
	contextErr := ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(contextErr, context.DeadlineExceeded) {
		return domain.WrapError(
			domain.ErrorTimeout,
			operation,
			"S3 data synchronization deadline exceeded",
			err,
		)
	}

	if errors.Is(err, context.Canceled) || errors.Is(contextErr, context.Canceled) {
		return domain.WrapError(
			domain.ErrorTimeout,
			operation,
			"S3 data synchronization canceled",
			err,
		)
	}

	return domain.WrapError(domain.ErrorCopy, operation, "S3 data synchronization failed", err)
}

func classifyToolAndLeaseError(
	ctx context.Context,
	operation string,
	toolErr error,
	leaseErrors <-chan error,
) error {
	syncErr := classifySyncError(ctx, operation, toolErr)
	select {
	case leaseErr := <-leaseErrors:
		return errors.Join(leaseErr, syncErr)
	default:
		return syncErr
	}
}

func startToolLogs(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
) *kube.ToolLogStream {
	if !req.StreamToolLogs {
		return nil
	}

	return kube.StartPVMigrateToolLogs(ctx, client, kube.ToolLogOptions{
		Namespaces:  []string{req.Namespace},
		OperationID: req.ID,
		Writer:      req.Writer,
		Logger:      req.Logger,
		Structured:  req.StructuredLogs,
	})
}

func inspectPVC(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
	online, allowMounted, restore bool,
) (*PVCInfo, error) {
	var (
		pvc                          *corev1.PersistentVolumeClaim
		pvcErr                       error
		consumerNames, consumerNodes []string
		consumerErr                  error
		wg                           sync.WaitGroup
	)
	wg.Go(func() {
		pvc, pvcErr = client.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, name, metav1.GetOptions{})
	})
	wg.Go(func() {
		consumerNames, consumerNodes, consumerErr = pvcConsumerDetails(ctx, client, namespace, name)
	})
	wg.Wait()

	if err := validateInspectedPVC(pvc, pvcErr, namespace, name); err != nil {
		return nil, err
	}

	mode := pvcVolumeMode(pvc)

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "backup preflight", "read PV", err)
	}

	capacity, err := validateInspectedPV(pv, pvc)
	if err != nil {
		return nil, err
	}

	if consumerErr != nil {
		return nil, consumerErr
	}

	if err := validateInspectionConsumers(
		pvc,
		consumerNames,
		online,
		allowMounted,
		restore,
	); err != nil {
		return nil, err
	}

	return &PVCInfo{
		PVC:       pvc,
		PV:        pv,
		Capacity:  capacity,
		Mode:      mode,
		Consumers: consumerNames,
		Nodes:     consumerNodes,
	}, nil
}

func validateInspectedPVC(
	pvc *corev1.PersistentVolumeClaim,
	pvcErr error,
	namespace, name string,
) error {
	if pvcErr != nil {
		return domain.WrapError(domain.ErrorKubernetes, "backup preflight", "read PVC", pvcErr)
	}

	if pvc == nil || pvc.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"backup preflight",
			fmt.Sprintf("read PVC %s/%s returned an empty object", namespace, name),
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup preflight",
			fmt.Sprintf("PVC %s/%s must be Bound", namespace, name),
		)
	}

	if pvcVolumeMode(pvc) != corev1.PersistentVolumeFilesystem {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup preflight",
			"S3 backup and restore require a Filesystem PVC",
		)
	}

	return nil
}

func pvcVolumeMode(pvc *corev1.PersistentVolumeClaim) corev1.PersistentVolumeMode {
	if pvc.Spec.VolumeMode == nil {
		return corev1.PersistentVolumeFilesystem
	}
	return *pvc.Spec.VolumeMode
}

func validateInspectedPV(
	pv *corev1.PersistentVolume,
	pvc *corev1.PersistentVolumeClaim,
) (resource.Quantity, error) {
	if pv == nil || pv.Name == "" {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorKubernetes,
			"backup preflight",
			fmt.Sprintf("read PV %s returned an empty object", pvc.Spec.VolumeName),
		)
	}

	if pvc.UID == "" || pv.UID == "" {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			"backup preflight",
			"PVC and PV must have stable Kubernetes identities",
		)
	}

	if pv.Status.Phase != corev1.VolumeBound {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			"backup preflight",
			fmt.Sprintf("PV %s must be Bound", pv.Name),
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.UID != pvc.UID {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorConflict,
			"backup preflight",
			fmt.Sprintf(
				"PVC/PV binding identity changed: PV %s claimRef does not match PVC %s/%s UID %s",
				pv.Name,
				pvc.Namespace,
				pvc.Name,
				pvc.UID,
			),
		)
	}

	capacity, ok := pv.Spec.Capacity[corev1.ResourceStorage]
	if !ok || capacity.Sign() <= 0 {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			"backup preflight",
			"PV has no positive storage capacity",
		)
	}

	return capacity, nil
}

func validateInspectionConsumers(
	pvc *corev1.PersistentVolumeClaim,
	consumerNames []string,
	online, allowMounted, restore bool,
) error {
	if restore {
		if !kube.HasWritableAccessMode(pvc.Spec.AccessModes) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"restore preflight",
				"destination PVC has no writable access mode",
			)
		}

		if len(consumerNames) > 0 && !allowMounted {
			return domain.NewError(
				domain.ErrorPrecondition,
				"restore preflight",
				"destination PVC is referenced by Pod(s) "+strings.Join(consumerNames, ","),
			)
		}

		if len(consumerNames) > 0 && hasRWOP(pvc) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"restore preflight",
				"restore cannot mount an active ReadWriteOncePod PVC",
			)
		}

		return nil
	}

	if !online && len(consumerNames) > 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup preflight",
			"source PVC is referenced by Pod(s) "+strings.Join(consumerNames, ","),
		)
	}

	if online && hasRWOP(pvc) && len(consumerNames) > 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup preflight",
			"online backup cannot mount an active ReadWriteOncePod PVC",
		)
	}

	return nil
}

func checkToolQuota(ctx context.Context, client kubernetes.Interface, namespace string) error {
	var (
		quotas                  *corev1.ResourceQuotaList
		limitRanges             *corev1.LimitRangeList
		quotaErr, limitRangeErr error
		wg                      sync.WaitGroup
	)
	wg.Go(func() {
		quotas, quotaErr = client.CoreV1().ResourceQuotas(namespace).List(ctx, metav1.ListOptions{})
	})
	wg.Go(func() {
		limitRanges, limitRangeErr = client.CoreV1().
			LimitRanges(namespace).
			List(ctx, metav1.ListOptions{})
	})
	wg.Wait()

	if quotaErr != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"backup preflight",
			"list tool ResourceQuotas",
			quotaErr,
		)
	}

	if quotas == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"backup preflight",
			"list tool ResourceQuotas returned an empty object",
		)
	}

	demand := toolQuotaDemand()

	violations := make([]string, 0)
	for _, quota := range quotas.Items {
		// The zero-resource policy deliberately omits an ephemeral-storage
		// limit. Kubernetes requires an explicit limit when a namespace quota
		// tracks limits.ephemeral-storage, so this tool Pod would be rejected
		// by admission even though its requested quantity is zero.
		if _, bounded := quota.Spec.Hard[corev1.ResourceLimitsEphemeralStorage]; bounded {
			violations = append(
				violations,
				fmt.Sprintf(
					"%s/%s %s: tool omits an ephemeral-storage limit required by this quota",
					namespace,
					quota.Name,
					corev1.ResourceLimitsEphemeralStorage,
				),
			)
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
				violations = append(
					violations,
					fmt.Sprintf(
						"%s/%s %s: used %s + tool demand %s exceeds hard %s",
						namespace,
						quota.Name,
						name,
						used.String(),
						requested.String(),
						hard.String(),
					),
				)
			}
		}
	}

	if len(violations) > 0 {
		sort.Strings(violations)

		return domain.NewError(
			domain.ErrorPrecondition,
			"backup preflight",
			"tool resources exceed namespace quota: "+strings.Join(violations, "; "),
		)
	}

	if limitRangeErr != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"backup preflight",
			"list tool LimitRanges",
			limitRangeErr,
		)
	}

	if limitRanges == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"backup preflight",
			"list tool LimitRanges returned an empty object",
		)
	}

	for _, limitRange := range limitRanges.Items {
		for _, item := range limitRange.Spec.Limits {
			if item.Type != corev1.LimitTypeContainer && item.Type != corev1.LimitTypePod {
				continue
			}

			for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
				if minimum, ok := item.Min[name]; ok && minimum.Sign() > 0 {
					return domain.NewError(
						domain.ErrorPrecondition,
						"backup preflight",
						fmt.Sprintf(
							"tool resource %s=0 is below LimitRange %s minimum %s",
							name,
							limitRange.Name,
							minimum.String(),
						),
					)
				}
			}
		}
	}

	return nil
}

// toolQuotaDemand describes the objects created by the embedded pv-migrate
// bucket-storage chart. Every run creates one ServiceAccount, Job, and Job Pod
// plus one chart Secret. Helm's default Secret storage driver creates a second
// Secret for the release record; configmap storage creates a release ConfigMap.
// Memory and SQL drivers keep release state outside the namespace.
func toolQuotaDemand() map[corev1.ResourceName]resource.Quantity {
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
		// alias or fail before creating tool resources.
		add(corev1.ResourceSecrets, 2)
		add(corev1.ResourceName("count/secrets"), 2)
	}

	return demand
}

func pvcConsumerDetails(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, claim string,
) ([]string, []string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, domain.WrapError(
			domain.ErrorKubernetes,
			"backup preflight",
			"list PVC consumers",
			err,
		)
	}

	if pods == nil {
		return nil, nil, domain.NewError(
			domain.ErrorKubernetes,
			"backup preflight",
			"list PVC consumers returned an empty object",
		)
	}

	consumers := make([]string, 0)

	nodes := make([]string, 0)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !kube.ActivePodUsesPVC(pod, claim) {
			continue
		}

		consumers = append(consumers, pod.Name)
		if pod.Spec.NodeName != "" {
			nodes = append(nodes, pod.Spec.NodeName)
		}
	}

	return consumers, nodes, nil
}

func hasRWOP(pvc *corev1.PersistentVolumeClaim) bool {
	return slices.Contains(pvc.Spec.AccessModes, corev1.ReadWriteOncePod)
}

func hasRWO(pvc *corev1.PersistentVolumeClaim) bool {
	for _, mode := range pvc.Spec.AccessModes {
		if mode == corev1.ReadWriteOnce || mode == corev1.ReadWriteOncePod {
			return true
		}
	}

	return false
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
		var domainErr *domain.Error
		if errors.As(err, &domainErr) {
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

func verifyPVCIdentity(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name, expectedPVCUID, expectedPVUID string,
) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, error) {
	if expectedPVCUID == "" || expectedPVUID == "" {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			"backup identity",
			"expected PVC and PV identities are required",
		)
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, domain.WrapError(
			domain.ErrorKubernetes,
			"backup identity",
			"read PVC",
			err,
		)
	}

	if string(pvc.UID) != expectedPVCUID {
		return nil, nil, domain.NewError(
			domain.ErrorConflict,
			"backup identity",
			"PVC identity changed since preflight",
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			"backup identity",
			"PVC is no longer Bound",
		)
	}

	if pvc.Spec.VolumeName == "" {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			"backup identity",
			"PVC is no longer bound to a PV",
		)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, domain.WrapError(domain.ErrorKubernetes, "backup identity", "read PV", err)
	}

	if string(pv.UID) != expectedPVUID {
		return nil, nil, domain.NewError(
			domain.ErrorConflict,
			"backup identity",
			"PV identity changed since preflight",
		)
	}

	if pv.Status.Phase != corev1.VolumeBound {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			"backup identity",
			"PV is no longer Bound",
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != namespace ||
		pv.Spec.ClaimRef.Name != name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return nil, nil, domain.NewError(
			domain.ErrorConflict,
			"backup identity",
			"PVC and PV claimRef no longer identify the same binding",
		)
	}

	return pvc, pv, nil
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
