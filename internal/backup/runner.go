package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	rclonePreserveLinksArgs = "--links"
	lockReleaseTimeout      = 10 * time.Second
)

type Mode string

const (
	ModeOffline Mode = "offline"
	ModeOnline  Mode = "online"
	ModeRestore Mode = "restore"
)

type Request struct {
	ID                      string
	ToolImage               string
	Namespace               string
	PVCName                 string
	CreatePVC               bool
	DestinationStorageClass string
	DestinationAccessMode   string
	DestinationCapacity     string
	TargetNode              string
	Path                    string
	Online                  bool
	AllowMounted            bool
	DeleteExtraneousFiles   bool
	HelmTimeout             time.Duration
	KubeconfigPath          string
	KubeContext             string
	StreamToolLogs          bool
	StructuredLogs          bool
	Store                   S3RepositoryStore
	// SkipManifestCheck defers remote recovery-point validation to the
	// controller. The controller is the only component that can resolve a
	// repository's credentials when the submitting user cannot read its Secret.
	SkipManifestCheck bool
	// BackupRepository selects a user-owned namespaced repository containing
	// the complete object-store location and credentials reference.
	BackupRepository          string
	BackupRepositoryNamespace string
	BackupRepositoryBinding   *domain.BackupRepositoryBindingStatus
	ToolServiceAccountName    string
	Writer                    io.Writer
	Logger                    *slog.Logger
	ToolImageProber           kube.ToolImageProber
	SessionStore              kube.SessionStore
	SessionNamespace          string
	OpenEBSLVMEnableShared    bool
	OpenEBSLVMManager         kube.OpenEBSLVMSharedVolumeManager
	WritablePVCMount          bool
	BackupSession             *domain.Session
	ObjectStoreFactory        func(context.Context, objectstore.Config) (*objectstore.Store, error)
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
	CreatePVC        bool     `json:"createPVC,omitempty"        yaml:"createPVC,omitempty"`
	StorageClass     string   `json:"storageClass,omitempty"     yaml:"storageClass,omitempty"`
	AccessMode       string   `json:"accessMode,omitempty"       yaml:"accessMode,omitempty"`
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
			operation+" preflight",
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

	if restore && req.CreatePVC {
		existing, getErr := client.CoreV1().PersistentVolumeClaims(req.Namespace).
			Get(ctx, req.PVCName, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return preflightRestorePVCCreation(ctx, client, req, toolImage, nil)
		}

		if getErr != nil {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				restorePreflightPhase,
				"read destination PVC",
				getErr,
			)
		}

		creationPlan, creationErr := preflightRestorePVCCreation(
			ctx,
			client,
			req,
			toolImage,
			existing,
		)
		if creationErr != nil {
			return nil, creationErr
		}

		if creationPlan != nil {
			return creationPlan, nil
		}
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
		quotaErr = checkObjectTransferQuota(ctx, client, req, operation)
	})
	wg.Go(func() {
		if req.SkipManifestCheck {
			return
		}

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

	toolNode, err := preflightToolNode(ctx, client, req, operation, info)
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
	if req.SkipManifestCheck {
		plan.Warnings = append(
			plan.Warnings,
			"object-store manifest check deferred to the controller",
		)
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
		return prepareRestorePlan(ctx, req, info, manifest, plan, stage)
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
			backupPreflightPhase,
			"S3 completion manifest already exists; use a new backup name to preserve the published recovery point",
		)
	}

	return plan, nil
}

func preflightToolNode(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	operation string,
	info *PVCInfo,
) (string, error) {
	if !req.Online && !strings.EqualFold(operation, "restore") {
		return uniquePVToolNode(ctx, client, info.PV, operation+" preflight")
	}

	consumerNode, err := rwoConsumerNode(info, operation+" scheduling")
	if err != nil {
		return "", err
	}

	if strings.EqualFold(operation, "restore") && consumerNode != "" && req.TargetNode != "" {
		if _, err := selectRestoreToolNode(req.TargetNode, consumerNode, ""); err != nil {
			return "", err
		}
	}

	if consumerNode != "" && (!strings.EqualFold(operation, "restore") || req.TargetNode == "") {
		return "", nil
	}

	return uniquePVToolNode(ctx, client, info.PV, operation+" preflight")
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
	phase string,
) (string, error) {
	if pv == nil || pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil ||
		len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return "", nil
	}

	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", domain.WrapError(
			domain.ErrorKubernetes,
			phase,
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

	normalizedPath, err := normalizeObjectTransferPath(req.Path)
	if err != nil {
		return err
	}

	req.Path = normalizedPath

	plan, err := preflight(ctx, client, req, restore, "execution revalidation")
	if err != nil {
		return err
	}

	if restore && plan.CreatePVC {
		manifest, manifestErr := req.Store.Manifest(ctx)
		if manifestErr != nil {
			return manifestErr
		}

		if manifest == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"restore",
				"S3 completion manifest disappeared before destination PVC creation",
			)
		}

		if err := createRestorePVC(ctx, client, req, *manifest); err != nil {
			return err
		}

		plan, err = preflight(ctx, client, req, restore, "post-create revalidation")
		if err != nil {
			return err
		}
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
	serviceAccountName string,
) ([]string, error) {
	if probe.NodeName == "" {
		return nil, nil
	}

	node, err := client.CoreV1().Nodes().Get(ctx, probe.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			toolSchedulingPhase,
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

	identityValues, err := kube.ToolServiceAccountHelmValues(serviceAccountName)
	if err != nil {
		return nil, err
	}

	return append(append(values, pullSecretValues...), identityValues...), nil
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
			toolSchedulingPhase,
			"PVC or PV identity changed during final tool launch validation",
		)
	}

	operation := "backup scheduling"
	if restore {
		operation = restoreSchedulingPhase
	}

	consumerNode, err := rwoConsumerNode(info, operation)
	if err != nil {
		return err
	}

	if restore && consumerNode != "" && req.TargetNode != "" {
		if _, err := selectRestoreToolNode(req.TargetNode, consumerNode, ""); err != nil {
			return err
		}
	}

	requiredNode := consumerNode

	var pvNode string
	if requiredNode == "" || (restore && req.TargetNode != "") {
		operation := "backup"
		if restore {
			operation = "restore"
		}

		pvNode, err = uniquePVToolNode(ctx, client, pv, operation+" preflight")
		if err != nil {
			return err
		}
	}

	if restore {
		requiredNode, err = selectRestoreToolNode(req.TargetNode, consumerNode, pvNode)
		if err != nil {
			return err
		}
	} else if requiredNode == "" {
		requiredNode = pvNode
	}

	if requiredNode != "" && requiredNode != probe.NodeName {
		return domain.NewError(
			domain.ErrorConflict,
			toolSchedulingPhase,
			fmt.Sprintf(
				"required tool node changed from %s to %s during image probe",
				probe.NodeName,
				requiredNode,
			),
		)
	}

	return nil
}

func toolHelmTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return 10 * time.Minute
	}
	return timeout
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
	operation := "backup"
	if restore {
		operation = "restore"
	}

	phase := operation + " preflight"

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
		consumerNames, consumerNodes, consumerErr = pvcConsumerDetails(
			ctx,
			client,
			namespace,
			name,
			phase,
		)
	})
	wg.Wait()

	if err := validateInspectedPVC(pvc, pvcErr, namespace, name, phase); err != nil {
		return nil, err
	}

	mode := pvcVolumeMode(pvc)

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, phase, "read PV", err)
	}

	capacity, err := validateInspectedPV(pv, pvc, phase)
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
	phase string,
) error {
	if pvcErr != nil {
		return domain.WrapError(domain.ErrorKubernetes, phase, "read PVC", pvcErr)
	}

	if pvc == nil || pvc.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			phase,
			fmt.Sprintf("read PVC %s/%s returned an empty object", namespace, name),
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			phase,
			fmt.Sprintf("PVC %s/%s must be Bound", namespace, name),
		)
	}

	if pvcVolumeMode(pvc) != corev1.PersistentVolumeFilesystem {
		return domain.NewError(
			domain.ErrorPrecondition,
			phase,
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
	phase string,
) (resource.Quantity, error) {
	if pv == nil || pv.Name == "" {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorKubernetes,
			phase,
			fmt.Sprintf("read PV %s returned an empty object", pvc.Spec.VolumeName),
		)
	}

	if pvc.UID == "" || pv.UID == "" {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			phase,
			"PVC and PV must have stable Kubernetes identities",
		)
	}

	if pv.Status.Phase != corev1.VolumeBound {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			phase,
			fmt.Sprintf("PV %s must be Bound", pv.Name),
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.UID != pvc.UID {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorConflict,
			phase,
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
			phase,
			"PV has no positive storage capacity",
		)
	}

	if err := kube.ValidateBoundVolumeCapacity(pvc, pv, nil); err != nil {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			phase,
			err.Error(),
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
				restorePreflightPhase,
				"destination PVC has no writable access mode",
			)
		}

		if len(consumerNames) > 0 && !allowMounted {
			return domain.NewError(
				domain.ErrorPrecondition,
				restorePreflightPhase,
				"destination PVC is referenced by Pod(s) "+strings.Join(consumerNames, ","),
			)
		}

		if len(consumerNames) > 0 && hasRWOP(pvc) {
			return domain.NewError(
				domain.ErrorPrecondition,
				restorePreflightPhase,
				"restore cannot mount an active ReadWriteOncePod PVC",
			)
		}

		return nil
	}

	if !online && len(consumerNames) > 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			backupPreflightPhase,
			"source PVC is referenced by Pod(s) "+strings.Join(consumerNames, ","),
		)
	}

	if online && hasRWOP(pvc) && len(consumerNames) > 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			backupPreflightPhase,
			"online backup cannot mount an active ReadWriteOncePod PVC",
		)
	}

	return nil
}

func pvcConsumerDetails(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, claim string,
	phase string,
) ([]string, []string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, domain.WrapError(
			domain.ErrorKubernetes,
			phase,
			"list PVC consumers",
			err,
		)
	}

	if pods == nil {
		return nil, nil, domain.NewError(
			domain.ErrorKubernetes,
			phase,
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

func verifyPVCIdentity(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name, expectedPVCUID, expectedPVUID string,
) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, error) {
	if expectedPVCUID == "" || expectedPVUID == "" {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			backupIdentityPhase,
			"expected PVC and PV identities are required",
		)
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, domain.WrapError(
			domain.ErrorKubernetes,
			backupIdentityPhase,
			"read PVC",
			err,
		)
	}

	if string(pvc.UID) != expectedPVCUID {
		return nil, nil, domain.NewError(
			domain.ErrorConflict,
			backupIdentityPhase,
			"PVC identity changed since preflight",
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			backupIdentityPhase,
			"PVC is no longer Bound",
		)
	}

	if pvc.Spec.VolumeName == "" {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			backupIdentityPhase,
			"PVC is no longer bound to a PV",
		)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, domain.WrapError(
			domain.ErrorKubernetes,
			backupIdentityPhase,
			"read PV",
			err,
		)
	}

	if string(pv.UID) != expectedPVUID {
		return nil, nil, domain.NewError(
			domain.ErrorConflict,
			backupIdentityPhase,
			"PV identity changed since preflight",
		)
	}

	if pv.Status.Phase != corev1.VolumeBound {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			backupIdentityPhase,
			"PV is no longer Bound",
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != namespace ||
		pv.Spec.ClaimRef.Name != name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return nil, nil, domain.NewError(
			domain.ErrorConflict,
			backupIdentityPhase,
			"PVC and PV claimRef no longer identify the same binding",
		)
	}

	return pvc, pv, nil
}
