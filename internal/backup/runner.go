package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	// --links keeps symbolic links through the S3 round trip; --metadata
	// carries POSIX mode bits in object metadata, which plain rclone copy
	// otherwise normalizes away (e.g. 0640 source becomes 0644 on restore).
	rclonePreserveLinksArgs = "--links --metadata"
	lockReleaseTimeout      = 10 * time.Second
)

type PVCInfo struct {
	PVC       *corev1.PersistentVolumeClaim
	PV        *corev1.PersistentVolume
	Capacity  resource.Quantity
	Mode      corev1.PersistentVolumeMode
	Consumers []string
	Nodes     []string
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

func probeRcloneToolImage(
	ctx context.Context,
	prober kube.ToolImageProber,
	options kube.ToolImageProbeOptions,
) (kube.ToolImageProbeResult, error) {
	if len(options.Targets) != 1 {
		return kube.ToolImageProbeResult{}, domain.NewError(
			domain.ErrorInternal,
			"tool image probe",
			"rclone probe requires exactly one target",
		)
	}

	if prober == nil {
		return kube.ToolImageProbeResult{NodeName: options.Targets[0].NodeName}, nil
	}

	results, err := prober.Probe(ctx, options)
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
	namespace string,
	serviceAccountName string,
) (kube.HelmOverrides, error) {
	if probe.NodeName == "" {
		return kube.HelmOverrides{}, domain.NewError(
			domain.ErrorInternal,
			toolSchedulingPhase,
			"rclone probe returned no scheduled node",
		)
	}

	node, err := client.CoreV1().Nodes().Get(ctx, probe.NodeName, metav1.GetOptions{})
	if err != nil {
		return kube.HelmOverrides{}, domain.WrapError(
			domain.ErrorKubernetes,
			toolSchedulingPhase,
			"read node "+probe.NodeName,
			err,
		)
	}

	values, err := kube.ToolComponentNodeHelmValues(kube.ToolComponentRclone, node)
	if err != nil {
		return kube.HelmOverrides{}, err
	}

	pullSecretValues, err := kube.ToolImagePullSecretHelmValues([]kube.ToolImageProbeResult{probe})
	if err != nil {
		return kube.HelmOverrides{}, err
	}

	if strings.TrimSpace(serviceAccountName) == "" {
		if err := kube.EnsureTransferServiceAccount(ctx, client, namespace); err != nil {
			return kube.HelmOverrides{}, err
		}

		serviceAccountName = kube.TransferServiceAccountName
	}

	identityValues, err := kube.ToolServiceAccountHelmValues(serviceAccountName)
	if err != nil {
		return kube.HelmOverrides{}, err
	}

	return kube.HelmOverrides{
		Values: identityValues.Values,
		StringValues: append(
			append(values, pullSecretValues...),
			identityValues.StringValues...,
		),
	}, nil
}

func validateBackupToolLaunch(
	ctx context.Context,
	client kubernetes.Interface,
	source v1alpha1.ObjectReference,
	expectedPVUID types.UID,
	online bool,
	probedNode string,
) error {
	info, err := inspectBackupPVC(
		ctx,
		client,
		source.Namespace,
		source.Name,
		online,
	)
	if err != nil {
		return err
	}

	pvc, pv, err := verifyPVCIdentity(
		ctx,
		client,
		source.Namespace,
		source.Name,
		string(source.UID),
		string(expectedPVUID),
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

	consumerNode, err := rwoConsumerNode(info, operation)
	if err != nil {
		return err
	}

	requiredNode := consumerNode

	if requiredNode == "" {
		operation := "backup"

		requiredNode, err = uniquePVToolNode(ctx, client, pv, operation+" preflight")
		if err != nil {
			return err
		}
	}

	if requiredNode != "" && requiredNode != probedNode {
		return domain.NewError(
			domain.ErrorConflict,
			toolSchedulingPhase,
			fmt.Sprintf(
				"required tool node changed from %s to %s during image probe",
				probedNode,
				requiredNode,
			),
		)
	}

	return nil
}

func validateRestoreToolLaunch(
	ctx context.Context,
	client kubernetes.Interface,
	destination v1alpha1.ObjectReference,
	expectedPVUID types.UID,
	allowMounted bool,
	targetNode, probedNode string,
) error {
	info, err := inspectRestorePVC(
		ctx,
		client,
		destination.Namespace,
		destination.Name, allowMounted,
	)
	if err != nil {
		return err
	}

	pvc, pv, err := verifyPVCIdentity(
		ctx,
		client,
		destination.Namespace,
		destination.Name,
		string(destination.UID),
		string(expectedPVUID),
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

	operation := restoreSchedulingPhase

	consumerNode, err := rwoConsumerNode(info, operation)
	if err != nil {
		return err
	}

	if consumerNode != "" && targetNode != "" {
		if _, err := selectRestoreToolNode(targetNode, consumerNode, ""); err != nil {
			return err
		}
	}

	requiredNode := consumerNode

	var pvNode string
	if requiredNode == "" || targetNode != "" {
		operation := "restore"

		pvNode, err = uniquePVToolNode(ctx, client, pv, operation+" preflight")
		if err != nil {
			return err
		}
	}

	requiredNode, err = selectRestoreToolNode(targetNode, consumerNode, pvNode)
	if err != nil {
		return err
	}

	if requiredNode != "" && requiredNode != probedNode {
		return domain.NewError(
			domain.ErrorConflict,
			toolSchedulingPhase,
			fmt.Sprintf(
				"required tool node changed from %s to %s during image probe",
				probedNode,
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

	if operationID != "" {
		attempt = operationID + "/" + attempt
	}

	return objectstore.LockHolder(attempt), nil
}

func toolOperationID(holder string) string {
	digest := sha256.Sum256([]byte(holder))

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

func logOperation(logger *slog.Logger, message string, args ...any) {
	if logger != nil {
		logger.Info(message, args...)
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

func inspectBackupPVC(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
	online bool,
) (*PVCInfo, error) {
	info, err := inspectBoundPVC(ctx, client, namespace, name, backupPreflightPhase)
	if err != nil {
		return nil, err
	}

	if err := validateBackupConsumers(info.PVC, info.Consumers, online); err != nil {
		return nil, err
	}

	return info, nil
}

func inspectRestorePVC(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
	allowMounted bool,
) (*PVCInfo, error) {
	info, err := inspectBoundPVC(ctx, client, namespace, name, restorePreflightPhase)
	if err != nil {
		return nil, err
	}

	if err := validateRestoreConsumers(info.PVC, info.Consumers, allowMounted); err != nil {
		return nil, err
	}

	return info, nil
}

func inspectBoundPVC(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
	phase string,
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

func validateRestoreConsumers(
	pvc *corev1.PersistentVolumeClaim,
	consumerNames []string,
	allowMounted bool,
) error {
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

func validateBackupConsumers(
	pvc *corev1.PersistentVolumeClaim,
	consumerNames []string,
	online bool,
) error {
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
