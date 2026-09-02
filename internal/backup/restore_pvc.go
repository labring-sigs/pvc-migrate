package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	restoreBucketAnnotation = "pvc-migrate.io/restore-bucket"
	restorePrefixAnnotation = "pvc-migrate.io/restore-prefix"
	restoreNameAnnotation   = "pvc-migrate.io/restore-name"
)

func preflightRestorePVCCreation(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	toolImage string,
	existing *corev1.PersistentVolumeClaim,
) (*Plan, error) {
	if existing != nil {
		if err := validateRestorePVCOwnership(existing, req); err != nil {
			return nil, err
		}
	}

	if req.DestinationStorageClass == "" || req.DestinationAccessMode == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			restorePreflightPhase,
			"--create-pvc requires --destination-storage-class and --destination-access-mode",
		)
	}

	manifest, capacity, err := resolveRestoreManifestAndCapacity(ctx, req)
	if err != nil {
		return nil, err
	}

	accessMode, err := parseRestoreAccessMode(req.DestinationAccessMode)
	if err != nil {
		return nil, err
	}

	requiresPlacementValidation := existing == nil ||
		existing.Status.Phase != corev1.ClaimBound ||
		existing.Spec.VolumeName == ""
	if requiresPlacementValidation {
		if err := kube.ValidateStorageClassPlacement(
			ctx,
			client,
			req.DestinationStorageClass,
			req.TargetNode,
		); err != nil {
			return nil, err
		}
	}

	if existing == nil {
		report, policyErr := kube.CheckPVCAdmissionPolicies(
			ctx,
			client,
			[]kube.PVCAdmissionChange{{
				Namespace:             req.Namespace,
				Name:                  req.PVCName,
				RequestedStorage:      capacity,
				RequestedStorageClass: req.DestinationStorageClass,
			}},
		)
		if policyErr != nil {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				restorePreflightPhase,
				"check destination PVC admission policies",
				policyErr,
			)
		}

		if len(report.QuotaViolations) > 0 {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				restorePreflightPhase,
				"destination PVC quota rejected the request: "+strings.Join(
					report.QuotaViolations,
					"; ",
				),
			)
		}

		if len(report.LimitRangeViolations) > 0 {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				restorePreflightPhase,
				"destination PVC LimitRange rejected the request: "+strings.Join(
					report.LimitRangeViolations,
					"; ",
				),
			)
		}
	}

	if err := checkNamespaceAdmissionPolicies(
		ctx,
		client,
		req.Namespace,
		"restore",
		objectTransferToolResourceEstimate(),
	); err != nil {
		return nil, err
	}

	if existing != nil {
		if err := validateRestoreCreatedPVC(
			existing,
			req,
			capacity,
			accessMode,
			req.DestinationStorageClass,
		); err != nil {
			return nil, err
		}

		switch {
		case existing.Status.Phase == corev1.ClaimBound && existing.Spec.VolumeName != "":
			return nil, nil
		case existing.Status.Phase != corev1.ClaimPending:
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				restorePreflightPhase,
				fmt.Sprintf(
					"destination PVC %s/%s created by this restore is %s and cannot be resumed",
					req.Namespace,
					req.PVCName,
					existing.Status.Phase,
				),
			)
		}
	}

	plan := &Plan{
		Operation:       "restore",
		ToolImage:       toolImage,
		Namespace:       req.Namespace,
		PVC:             req.PVCName,
		Path:            transferDisplayPath(req.Path),
		Mode:            ModeRestore,
		Consistency:     "destination PVC write; application must be quiesced",
		Destination:     req.Store.Destination(),
		ManifestPresent: manifest != nil,
		Capacity:        capacity.String(),
		VolumeMode:      string(corev1.PersistentVolumeFilesystem),
		CreatePVC:       true,
		StorageClass:    req.DestinationStorageClass,
		AccessMode:      string(accessMode),
		ToolNode:        req.TargetNode,
		Compression:     "none",
		Warnings: []string{
			"destination PVC does not exist and will be created during restore",
		},
	}
	if manifest != nil {
		plan.ObjectCount = manifest.ObjectCount
		plan.TotalBytes = manifest.TotalBytes
		plan.InventorySHA256 = manifest.InventorySHA256
	} else {
		plan.Warnings = append(
			plan.Warnings,
			"object-store manifest and backup capacity validation are deferred to the controller",
		)
	}

	if existing != nil {
		plan.PVCUID = string(existing.UID)
		plan.Warnings = []string{
			"destination PVC created by this restore is Pending and will be probed again for binding",
		}
	}

	if manifest != nil && manifest.Path != req.Path {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
			fmt.Sprintf("restore path %q differs from backup path %q", req.Path, manifest.Path),
		)
	}

	return plan, nil
}

func resolveRestoreManifestAndCapacity(
	ctx context.Context,
	req Request,
) (*objectstore.Manifest, resource.Quantity, error) {
	if req.SkipManifestCheck {
		// A controller-backed submission intentionally has a config-only store:
		// repository credentials and the recovery-point manifest are resolved by
		// the controller. Keep the CLI's admission checks useful by requiring an
		// explicit capacity for the PVC it asks Kubernetes to create; the
		// controller performs the authoritative manifest capacity and volume-mode
		// validation before the transfer starts.
		if strings.TrimSpace(req.DestinationCapacity) == "" {
			return nil, resource.Quantity{}, domain.NewError(
				domain.ErrorPrecondition,
				restorePreflightPhase,
				"controller-backed restore with --create-pvc requires --destination-capacity because the backup manifest is resolved by the controller",
			)
		}

		capacity, err := resource.ParseQuantity(req.DestinationCapacity)
		if err != nil || capacity.Sign() <= 0 {
			if err == nil {
				err = errors.New("capacity must be greater than zero")
			}

			return nil, resource.Quantity{}, domain.WrapError(
				domain.ErrorValidation,
				restorePreflightPhase,
				"parse --destination-capacity",
				err,
			)
		}

		return nil, capacity, nil
	}

	manifest, err := req.Store.Manifest(ctx)
	if err != nil {
		return nil, resource.Quantity{}, err
	}

	if manifest == nil {
		return nil, resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
			"S3 completion manifest is missing; the backup is not a published recovery point",
		)
	}

	if err := req.Store.VerifyInventory(ctx, *manifest); err != nil {
		return nil, resource.Quantity{}, wrapBackupError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
			"verify S3 backup inventory",
			err,
		)
	}

	if manifest.VolumeMode != string(corev1.PersistentVolumeFilesystem) {
		return nil, resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
			"automatic destination PVC creation requires a Filesystem backup, got "+manifest.VolumeMode,
		)
	}

	capacity, err := restoreDestinationCapacity(*manifest, req.DestinationCapacity)
	if err != nil {
		return nil, resource.Quantity{}, err
	}

	return manifest, capacity, nil
}

func prepareRestorePlan(
	ctx context.Context,
	req Request,
	info *PVCInfo,
	manifest *objectstore.Manifest,
	plan *Plan,
	stage string,
) (*Plan, error) {
	plan.Operation = "restore"
	plan.Mode = ModeRestore
	plan.Consistency = "destination PVC write; application must be quiesced"

	if manifest == nil {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
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
			restorePreflightPhase,
			"verify S3 backup inventory",
			err,
		)
	}

	if manifest.Capacity != "" {
		backupCapacity, parseErr := resource.ParseQuantity(manifest.Capacity)
		if parseErr != nil {
			return nil, domain.WrapError(
				domain.ErrorPrecondition,
				restorePreflightPhase,
				"parse backup capacity",
				parseErr,
			)
		}

		if info.Capacity.Cmp(backupCapacity) < 0 {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				restorePreflightPhase,
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
			restorePreflightPhase,
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
			restorePreflightPhase,
			fmt.Sprintf("restore path %q differs from backup path %q", req.Path, manifest.Path),
		)
	}

	if req.AllowMounted && len(info.Consumers) > 0 {
		plan.Warnings = append(
			plan.Warnings,
			"restore is explicitly allowed while the destination PVC has consumers",
		)
	}

	consumerNode, err := rwoConsumerNode(info, restoreSchedulingPhase)
	if err != nil {
		return nil, err
	}

	toolNode, err := selectRestoreToolNode(req.TargetNode, consumerNode, plan.ToolNode)
	if err != nil {
		return nil, err
	}

	plan.ToolNode = toolNode

	if consumerNode != "" {
		plan.Warnings = append(
			plan.Warnings,
			"mounted RWO restore tool will be pinned to consumer node "+consumerNode,
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

func selectRestoreToolNode(targetNode, consumerNode, pvNode string) (string, error) {
	if targetNode == "" {
		if consumerNode != "" {
			return consumerNode, nil
		}
		return pvNode, nil
	}

	for _, required := range []struct {
		requirement string
		node        string
	}{
		{requirement: "mounted RWO consumer", node: consumerNode},
		{requirement: "PV topology", node: pvNode},
	} {
		if required.node != "" && required.node != targetNode {
			return "", domain.NewError(
				domain.ErrorConflict,
				restoreSchedulingPhase,
				fmt.Sprintf(
					"destination PVC %s requires node %s, but target node %s was requested",
					required.requirement,
					required.node,
					targetNode,
				),
			)
		}
	}

	return targetNode, nil
}

func restoreDestinationCapacity(
	manifest objectstore.Manifest,
	requested string,
) (resource.Quantity, error) {
	backupCapacity, err := resource.ParseQuantity(manifest.Capacity)
	if err != nil {
		return resource.Quantity{}, domain.WrapError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
			"parse backup capacity",
			err,
		)
	}

	if requested == "" {
		return backupCapacity, nil
	}

	capacity, err := resource.ParseQuantity(requested)
	if err != nil {
		return resource.Quantity{}, domain.WrapError(
			domain.ErrorValidation,
			restorePreflightPhase,
			"parse --destination-capacity",
			err,
		)
	}

	if capacity.Sign() <= 0 {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorValidation,
			restorePreflightPhase,
			"--destination-capacity must be positive",
		)
	}

	if capacity.Cmp(backupCapacity) < 0 {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
			fmt.Sprintf(
				"destination PVC capacity %s is below backup capacity %s",
				capacity.String(),
				backupCapacity.String(),
			),
		)
	}

	return capacity, nil
}

func parseRestoreAccessMode(value string) (corev1.PersistentVolumeAccessMode, error) {
	mode := corev1.PersistentVolumeAccessMode(value)
	switch mode {
	case corev1.ReadWriteOnce, corev1.ReadWriteOncePod, corev1.ReadWriteMany:
		return mode, nil
	default:
		return "", domain.NewError(
			domain.ErrorValidation,
			restorePreflightPhase,
			fmt.Sprintf(
				"unsupported --destination-access-mode %q; use ReadWriteOnce, ReadWriteOncePod, or ReadWriteMany",
				value,
			),
		)
	}
}

func createRestorePVC(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	manifest objectstore.Manifest,
) error {
	if err := kube.ValidateStorageClassPlacement(
		ctx,
		client,
		req.DestinationStorageClass,
		req.TargetNode,
	); err != nil {
		return err
	}

	capacity, err := restoreDestinationCapacity(manifest, req.DestinationCapacity)
	if err != nil {
		return err
	}

	accessMode, err := parseRestoreAccessMode(req.DestinationAccessMode)
	if err != nil {
		return err
	}

	storageClass := req.DestinationStorageClass
	volumeMode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.PVCName,
			Namespace: req.Namespace,
			Labels: map[string]string{
				kube.ManagedByLabel:    kube.ManagedByValue,
				kube.ResourceRoleLabel: kube.ResourceRoleDestination,
			},
			Annotations: map[string]string{
				restoreBucketAnnotation: req.Store.Config().Bucket,
				restorePrefixAnnotation: req.Store.Config().Prefix,
				restoreNameAnnotation:   req.Store.Config().Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{accessMode},
			StorageClassName: &storageClass,
			VolumeMode:       &volumeMode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: capacity},
			},
		},
	}

	existing, err := client.CoreV1().PersistentVolumeClaims(req.Namespace).
		Get(ctx, req.PVCName, metav1.GetOptions{})
	if err == nil {
		if err := validateRestoreCreatedPVC(
			existing,
			req,
			capacity,
			accessMode,
			storageClass,
		); err != nil {
			return err
		}

		return bindRestorePVC(ctx, client, req, existing)
	}

	if !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"restore",
			"read destination PVC",
			err,
		)
	}

	logOperation(
		req,
		"creating destination PVC",
		"namespace",
		req.Namespace,
		"pvc",
		req.PVCName,
		"capacity",
		capacity.String(),
		"storageClass",
		storageClass,
		"accessMode",
		string(accessMode),
	)

	created, err := client.CoreV1().PersistentVolumeClaims(req.Namespace).
		Create(ctx, pvc, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		appeared, getErr := client.CoreV1().PersistentVolumeClaims(req.Namespace).
			Get(ctx, req.PVCName, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"restore",
				"read destination PVC after concurrent create",
				getErr,
			)
		}

		if validateErr := validateRestoreCreatedPVC(
			appeared,
			req,
			capacity,
			accessMode,
			storageClass,
		); validateErr != nil {
			return validateErr
		}

		return bindRestorePVC(ctx, client, req, appeared)
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"restore",
			fmt.Sprintf("create destination PVC %s/%s", req.Namespace, req.PVCName),
			err,
		)
	}

	if created == nil || created.UID == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"restore",
			"created destination PVC has no stable Kubernetes UID",
		)
	}

	return bindRestorePVC(ctx, client, req, created)
}

func bindRestorePVC(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	pvc *corev1.PersistentVolumeClaim,
) error {
	if pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" {
		return nil
	}

	if pvc.Status.Phase != "" && pvc.Status.Phase != corev1.ClaimPending {
		return domain.NewError(
			domain.ErrorPrecondition,
			"restore",
			fmt.Sprintf(
				"destination PVC %s/%s is %s and cannot be bound for restore",
				req.Namespace,
				req.PVCName,
				pvc.Status.Phase,
			),
		)
	}

	if req.ToolImageProber == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore",
			"a tool image prober is required to bind the Pending destination PVC",
		)
	}

	if err := probeCreatedRestorePVC(ctx, req); err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"restore",
			"bind the created destination PVC with a restore tool probe",
			err,
		)
	}

	return waitForRestorePVCBound(ctx, client, req, pvc.UID)
}

func validateRestoreCreatedPVC(
	pvc *corev1.PersistentVolumeClaim,
	req Request,
	capacity resource.Quantity,
	accessMode corev1.PersistentVolumeAccessMode,
	storageClass string,
) error {
	if pvc == nil || pvc.UID == "" {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"existing destination PVC has no stable UID",
		)
	}

	if err := validateRestorePVCOwnership(pvc, req); err != nil {
		return err
	}

	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != storageClass ||
		len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != accessMode ||
		pvcVolumeMode(pvc) != corev1.PersistentVolumeFilesystem {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			fmt.Sprintf(
				"destination PVC %s/%s does not match the requested restore storage settings",
				req.Namespace,
				req.PVCName,
			),
		)
	}

	requested, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok || requested.Cmp(capacity) < 0 {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			fmt.Sprintf(
				"destination PVC %s/%s capacity is below the requested restore capacity",
				req.Namespace,
				req.PVCName,
			),
		)
	}

	return nil
}

func validateRestorePVCOwnership(pvc *corev1.PersistentVolumeClaim, req Request) error {
	if pvc.Annotations[restoreBucketAnnotation] != req.Store.Config().Bucket ||
		pvc.Annotations[restorePrefixAnnotation] != req.Store.Config().Prefix ||
		pvc.Annotations[restoreNameAnnotation] != req.Store.Config().Name {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			fmt.Sprintf(
				"destination PVC %s/%s already exists and is not owned by this restore",
				req.Namespace,
				req.PVCName,
			),
		)
	}

	return nil
}

func probeCreatedRestorePVC(ctx context.Context, req Request) error {
	results, err := req.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
		OperationID: req.ID,
		Image:       req.ToolImage,
		Targets: []kube.ToolProbeTarget{{
			Namespace:        req.Namespace,
			NodeName:         req.TargetNode,
			PVCName:          req.PVCName,
			RequiredPath:     req.Path,
			CreatePath:       req.Path != "",
			WritablePVCMount: true,
			Components:       []string{kube.ToolComponentRclone},
		}},
		Timeout: toolHelmTimeout(req.HelmTimeout),
		Writer:  req.Writer,
		Logger:  req.Logger,
	})
	if err != nil {
		return err
	}

	if len(results) != 1 || results[0].NodeName == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"tool image probe",
			"created destination PVC probe returned no scheduled node",
		)
	}

	return nil
}

func waitForRestorePVCBound(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	expectedUID types.UID,
) error {
	return kube.WaitFor(
		ctx,
		time.Second,
		"destination PVC "+req.Namespace+"/"+req.PVCName+" binding",
		func(waitCtx context.Context) (bool, error) {
			pvc, err := client.CoreV1().PersistentVolumeClaims(req.Namespace).
				Get(waitCtx, req.PVCName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}

			if pvc.UID != expectedUID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"restore",
					"destination PVC identity changed while waiting for binding",
				)
			}

			return pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "", nil
		},
	)
}
