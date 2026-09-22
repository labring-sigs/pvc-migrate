package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
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

func createRestorePVC(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, id string,
	plan v1alpha1.RestorePlan,
	config objectstore.Config,
	manifest objectstore.Manifest,
	checkpoint func(context.Context, *corev1.PersistentVolumeClaim) error,
	probe func(context.Context, *corev1.PersistentVolumeClaim) error,
) error {
	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	if manifest.VolumeMode != string(corev1.PersistentVolumeFilesystem) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"restore",
			"destination PVC creation requires a Filesystem backup",
		)
	}

	capacity, err := restoreDestinationCapacity(manifest, plan.DestinationCapacity)
	if err != nil {
		return err
	}

	accessMode, err := parseRestoreAccessMode(plan.DestinationAccessMode)
	if err != nil {
		return err
	}

	if err := kube.ValidateStorageClassPlacement(
		ctx,
		client,
		plan.DestinationStorageClass,
		plan.TargetNode,
	); err != nil {
		return err
	}

	storageClass := plan.DestinationStorageClass
	volumeMode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      plan.DestinationPVC.Name,
			Namespace: namespace,
			Labels: map[string]string{
				kube.SessionKey:        id,
				kube.ManagedByLabel:    kube.ManagedByValue,
				kube.ResourceRoleLabel: kube.ResourceRoleDestination,
			},
			Annotations: map[string]string{
				restoreBucketAnnotation: config.Bucket,
				restorePrefixAnnotation: config.Prefix,
				restoreNameAnnotation:   config.Name,
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

	existing, err := client.CoreV1().PersistentVolumeClaims(namespace).
		Get(ctx, plan.DestinationPVC.Name, metav1.GetOptions{})
	if err == nil {
		if plan.DestinationPVC.UID != "" && existing.UID != plan.DestinationPVC.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"restore",
				"destination PVC identity changed since planning",
			)
		}

		if err := validateRestoreCreatedPVC(
			existing,
			id,
			config,
			capacity,
			accessMode,
			storageClass,
		); err != nil {
			return err
		}

		return checkpointAndBindRestorePVC(ctx, client, checkpoint, probe, existing)
	}

	if !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"restore",
			"read destination PVC",
			err,
		)
	}

	if plan.DestinationPVC.UID != "" {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"planned destination PVC disappeared before binding",
		)
	}

	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	created, err := client.CoreV1().PersistentVolumeClaims(namespace).
		Create(ctx, pvc, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		appeared, getErr := client.CoreV1().PersistentVolumeClaims(namespace).
			Get(ctx, plan.DestinationPVC.Name, metav1.GetOptions{})
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
			id,
			config,
			capacity,
			accessMode,
			storageClass,
		); validateErr != nil {
			return validateErr
		}

		return checkpointAndBindRestorePVC(ctx, client, checkpoint, probe, appeared)
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"restore",
			fmt.Sprintf("create destination PVC %s/%s", namespace, plan.DestinationPVC.Name),
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

	return checkpointAndBindRestorePVC(ctx, client, checkpoint, probe, created)
}

func checkpointAndBindRestorePVC(ctx context.Context, client kubernetes.Interface,
	checkpoint, probe func(context.Context, *corev1.PersistentVolumeClaim) error,
	pvc *corev1.PersistentVolumeClaim,
) error {
	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	if checkpoint != nil {
		if err := checkpoint(ctx, pvc.DeepCopy()); err != nil {
			return err
		}
	}

	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	return bindRestorePVC(ctx, client, probe, pvc)
}

func bindRestorePVC(
	ctx context.Context,
	client kubernetes.Interface,
	probe func(context.Context, *corev1.PersistentVolumeClaim) error,
	pvc *corev1.PersistentVolumeClaim,
) error {
	if pvc == nil || pvc.UID == "" || pvc.DeletionTimestamp != nil {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"destination PVC must have a stable UID and must not be deleting",
		)
	}

	if pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" {
		return nil
	}

	if pvc.Status.Phase != "" && pvc.Status.Phase != corev1.ClaimPending {
		return domain.NewError(
			domain.ErrorPrecondition,
			"restore",
			fmt.Sprintf(
				"destination PVC %s/%s is %s and cannot be bound for restore",
				pvc.Namespace,
				pvc.Name,
				pvc.Status.Phase,
			),
		)
	}

	if probe == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore",
			"a tool image prober is required to bind the Pending destination PVC",
		)
	}

	expected := kube.PVCReference(pvc)
	if err := probe(ctx, pvc.DeepCopy()); err != nil {
		return wrapBackupError(
			domain.ErrorPrecondition,
			"restore",
			"bind the created destination PVC with a restore tool probe",
			err,
		)
	}

	return waitForRestorePVCBound(ctx, client, expected)
}

func validateRestoreCreatedPVC(
	pvc *corev1.PersistentVolumeClaim,
	id string,
	config objectstore.Config,
	capacity resource.Quantity,
	accessMode corev1.PersistentVolumeAccessMode,
	storageClass string,
) error {
	if pvc == nil || pvc.UID == "" || pvc.DeletionTimestamp != nil {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"existing destination PVC has no stable UID or is deleting",
		)
	}

	if err := validateRestorePVCOwnership(pvc, id, config); err != nil {
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
				pvc.Namespace,
				pvc.Name,
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
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	return nil
}

func validateRestorePVCOwnership(
	pvc *corev1.PersistentVolumeClaim,
	id string,
	config objectstore.Config,
) error {
	if pvc.Labels[kube.SessionKey] != id ||
		pvc.Annotations[restoreBucketAnnotation] != config.Bucket ||
		pvc.Annotations[restorePrefixAnnotation] != config.Prefix ||
		pvc.Annotations[restoreNameAnnotation] != config.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			fmt.Sprintf(
				"destination PVC %s/%s already exists and is not owned by this restore",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	return nil
}

func probeCreatedRestorePVC(
	ctx context.Context,
	prober kube.ToolImageProber,
	options kube.ToolImageProbeOptions,
) error {
	if prober == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore",
			"a tool image prober is required to bind the Pending destination PVC",
		)
	}

	results, err := prober.Probe(ctx, options)
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
	expected v1alpha1.ObjectReference,
) error {
	return kube.WaitFor(
		ctx,
		time.Second,
		"destination PVC "+expected.Namespace+"/"+expected.Name+" binding",
		func(waitCtx context.Context) (bool, error) {
			pvc, err := client.CoreV1().PersistentVolumeClaims(expected.Namespace).
				Get(waitCtx, expected.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}

			if pvc.UID != expected.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"restore",
					"destination PVC identity changed while waiting for binding",
				)
			}

			if pvc.DeletionTimestamp != nil || pvc.Status.Phase == corev1.ClaimLost {
				return false, domain.NewError(
					domain.ErrorConflict,
					"restore",
					"destination PVC is deleting or lost while waiting for binding",
				)
			}

			return pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "", nil
		},
	)
}

func validateRestoreDestinationIdentity(
	previousPVC, previousPV v1alpha1.ObjectReference,
	pvcUID, pvUID string,
) error {
	if previousPVC.UID != "" && string(previousPVC.UID) != pvcUID {
		return domain.NewError(
			domain.ErrorConflict,
			"restore destination identity",
			"destination PVC identity changed since the restore checkpoint",
		)
	}

	if previousPV.UID != "" && string(previousPV.UID) != pvUID {
		return domain.NewError(
			domain.ErrorConflict,
			"restore destination identity",
			"destination PV identity changed since the restore checkpoint",
		)
	}

	return nil
}

func checkpointRestoreDestinationIdentity(
	ctx context.Context,
	client kubernetes.Interface,
	expected v1alpha1.ObjectReference,
	expectedPVUID types.UID,
	destinationPVC, destinationPV *v1alpha1.ObjectReference,
	persist func(context.Context) error,
) error {
	if destinationPVC == nil || destinationPV == nil || persist == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore destination identity",
			"destination references and checkpoint writer are required",
		)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	if err := validateRestoreDestinationIdentity(
		*destinationPVC,
		*destinationPV,
		string(expected.UID),
		string(expectedPVUID),
	); err != nil {
		return err
	}

	pvc, pv, err := verifyPVCIdentity(
		ctx,
		client,
		expected.Namespace,
		expected.Name,
		string(expected.UID),
		string(expectedPVUID),
	)
	if err != nil {
		return err
	}

	pvcRef, pvRef := kube.PVCReference(pvc), kube.PVReference(pv)
	if *destinationPVC == pvcRef && *destinationPV == pvRef {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := kube.LeaseFenceError(ctx); err != nil {
		return err
	}

	previousPVC, previousPV := *destinationPVC, *destinationPV

	*destinationPVC, *destinationPV = pvcRef, pvRef
	if err := persist(ctx); err != nil {
		*destinationPVC, *destinationPV = previousPVC, previousPV

		return domain.WrapError(
			domain.ErrorKubernetes,
			"restore destination identity",
			"persist destination PVC and PV checkpoint",
			err,
		)
	}

	return kube.LeaseFenceError(ctx)
}
