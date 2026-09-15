package backup

import (
	"context"
	"errors"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// restoreTransfer owns destination locking and S3 synchronization. It receives
// only the Restore plan and the recovery point's expected inventory.
type restoreTransfer struct {
	client kubernetes.Interface
	store  S3RepositoryStore
	tools  ToolRuntime
}

func (r *restoreTransfer) toolRequest(
	namespace, id string, plan v1alpha1.RestorePlan, rcloneConfig string,
	helmValues *kube.HelmOverrides,
) (pvmigrate.Restore, error) {
	if err := requireS3RepositoryBackend(r.store); err != nil {
		return pvmigrate.Restore{}, err
	}

	configValue, err := rcloneConfigHelmValue(rcloneConfig)
	if err != nil {
		return pvmigrate.Restore{}, err
	}

	overrides := kube.HelmOverrides{}
	if helmValues != nil {
		overrides.Values = append([]string(nil), helmValues.Values...)
		overrides.StringValues = append([]string(nil), helmValues.StringValues...)
	}

	storeConfig := r.store.Config()

	imageValues, err := kube.ToolImageHelmValues(plan.ToolImage)
	if err != nil {
		return pvmigrate.Restore{}, err
	}

	return pvmigrate.Restore{
		ID: id,
		PVC: pvmigrate.PVC{
			KubeconfigPath: r.tools.KubeconfigPath,
			Context:        r.tools.KubeContext,
			Namespace:      namespace,
			Name:           plan.DestinationPVC.Name,
		},
		Backend:          string(domain.BackupBackendS3),
		Bucket:           storeConfig.Bucket,
		Name:             storeConfig.Name,
		Path:             plan.Path,
		Prefix:           storeConfig.Prefix,
		RcloneConfigFile: upstreamRawConfigSentinel(),
		Remote:           r.store.RemotePath(),
		RcloneExtraArgs:  rclonePreserveLinksArgs,
		// inspectPVC enforces the requested mounted policy immediately before
		// launch and excludes terminal Pods that upstream still counts.
		IgnoreMounted:         true,
		DeleteExtraneousFiles: plan.DeleteExtraneous,
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
		HelmTimeout:    toolHelmTimeout(r.tools.HelmTimeout),
		Writer:         r.tools.Writer,
		Logger:         r.tools.Logger,
		StructuredLogs: true,
	}, nil
}

func (r *restoreTransfer) run(
	ctx context.Context,
	namespace, id string,
	plan v1alpha1.RestorePlan,
	expectedPVUID types.UID,
	expectedInventory objectstore.Manifest,
) (retErr error) {
	expectedPVCUID := string(plan.DestinationPVC.UID)

	holder, err := operationLockHolder(id)
	if err != nil {
		return err
	}

	ttl := operationLockTTL(ctx, r.tools.HelmTimeout)
	logOperation(
		r.tools.Logger,
		"acquiring restore operation lock",
		"namespace",
		namespace,
		"pvc",
		plan.DestinationPVC.Name,
	)

	unlock, lockedPVCUID, err := acquireRestoreLock(
		ctx,
		r.client,
		namespace,
		plan.DestinationPVC.Name,
		holder,
		ttl,
		expectedPVCUID,
	)
	if err != nil {
		return err
	}

	logOperation(
		r.tools.Logger,
		"restore operation lock acquired",
		"namespace",
		namespace,
		"pvc",
		plan.DestinationPVC.Name,
	)

	leaseCtx, cancelLease := context.WithCancel(ctx)
	leaseErrors := make(chan error, 1)

	leaseDone := make(chan struct{})

	go renewRestoreLock(
		leaseCtx,
		cancelLease,
		r.client,
		namespace,
		plan.DestinationPVC.Name,
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
		r.client,
		namespace,
		plan.DestinationPVC.Name,
		expectedPVCUID,
		string(expectedPVUID),
	)
	if err != nil {
		return err
	}
	// A controller can recreate a consumer after the initial preflight. Recheck
	// immediately before the tool mounts the destination so restore never
	// silently writes into a newly active workload unless explicitly allowed.
	currentInfo, err := inspectRestorePVC(
		leaseCtx,
		r.client,
		namespace,
		plan.DestinationPVC.Name,
		plan.AllowMounted,
	)
	if err != nil {
		return err
	}

	logOperation(
		r.tools.Logger,
		"verifying backup inventory before restore synchronization",
		"namespace",
		namespace,
		"pvc",
		plan.DestinationPVC.Name,
	)

	if err := r.store.VerifyInventory(leaseCtx, expectedInventory); err != nil {
		return wrapBackupError(
			domain.ErrorPrecondition,
			"restore",
			"verify S3 backup inventory before synchronization",
			err,
		)
	}

	consumerNode, err := rwoConsumerNode(currentInfo, restoreSchedulingPhase)
	if err != nil {
		return err
	}

	if consumerNode != "" && plan.TargetNode != "" {
		if _, err := selectRestoreToolNode(plan.TargetNode, consumerNode, ""); err != nil {
			return err
		}
	}

	pvNode := ""
	if consumerNode == "" || plan.TargetNode != "" {
		pvNode, err = uniquePVToolNode(leaseCtx, r.client, currentPV, restorePreflightPhase)
		if err != nil {
			return err
		}
	}

	toolNode, err := selectRestoreToolNode(plan.TargetNode, consumerNode, pvNode)
	if err != nil {
		return err
	}

	probeResult, err := probeRcloneToolImage(
		leaseCtx,
		r.tools.ToolImageProber,
		kube.ToolImageProbeOptions{
			OperationID: id,
			Image:       plan.ToolImage,
			Targets: []kube.ToolProbeTarget{restoreToolProbeTarget(
				v1alpha1.ObjectReference{
					Namespace: namespace,
					Name:      plan.DestinationPVC.Name,
				},
				plan.Path,
				toolNode,
			)},
			Timeout: toolHelmTimeout(r.tools.HelmTimeout),
			Writer:  r.tools.Writer,
			Logger:  r.tools.Logger,
		},
	)
	if err != nil {
		return err
	}

	helmValues, err := transferToolHelmValues(
		leaseCtx,
		r.client,
		probeResult,
		namespace,
		r.tools.ToolServiceAccountName,
	)
	if err != nil {
		return err
	}

	toolID := toolOperationID(holder)

	restoreRequest, err := r.toolRequest(
		namespace, toolID, plan,
		r.store.RcloneConfig(),
		&helmValues,
	)
	if err != nil {
		return err
	}

	if err := validateRestoreToolLaunch(
		leaseCtx,
		r.client,
		v1alpha1.ObjectReference{
			Namespace: namespace,
			Name:      plan.DestinationPVC.Name,
			UID:       types.UID(expectedPVCUID),
		},
		expectedPVUID,
		plan.AllowMounted,
		plan.TargetNode,
		probeResult.NodeName,
	); err != nil {
		return err
	}

	logOperation(
		r.tools.Logger,
		"starting restore data synchronization",
		"namespace",
		namespace,
		"pvc",
		plan.DestinationPVC.Name,
		"toolNode",
		probeResult.NodeName,
	)

	var toolLogs *kube.ToolLogStream
	if r.tools.StreamToolLogs {
		toolLogs = kube.StartPVMigrateToolLogs(leaseCtx, r.client, kube.ToolLogOptions{
			Namespaces: []string{namespace}, OperationID: toolID,
			Writer: r.tools.Writer, Logger: r.tools.Logger,
			Structured: r.tools.StructuredLogs,
		})
	}

	toolErr := pvmigrate.RunRestore(leaseCtx, restoreRequest)

	toolLogs.Stop()

	toolErr = errors.Join(toolErr, toolLogs.ObservedError())
	if toolErr != nil {
		return classifyToolAndLeaseError(leaseCtx, "restore", toolErr, leaseErrors)
	}

	if _, _, err := verifyPVCIdentity(
		leaseCtx,
		r.client,
		namespace,
		plan.DestinationPVC.Name,
		expectedPVCUID,
		string(expectedPVUID),
	); err != nil {
		return err
	}

	logOperation(
		r.tools.Logger,
		"verifying backup inventory after restore synchronization",
		"namespace",
		namespace,
		"pvc",
		plan.DestinationPVC.Name,
	)

	if err := r.store.VerifyInventory(leaseCtx, expectedInventory); err != nil {
		return wrapBackupError(
			domain.ErrorConflict,
			"restore",
			"S3 backup inventory changed during synchronization",
			err,
		)
	}

	return nil
}
