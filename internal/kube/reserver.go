package kube

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

type Reserver struct {
	client           kubernetes.Interface
	poll             time.Duration
	toolLogs         *ToolLogOptions
	logger           *slog.Logger
	trustedToolImage string
}

func NewReserver(client kubernetes.Interface) *Reserver {
	return &Reserver{client: client, poll: time.Second}
}

// WithTrustedToolImage pins reservation consumer Pods to the controller's
// administrator-selected image. An empty value keeps session-mode behavior.
func (r *Reserver) WithTrustedToolImage(image string) *Reserver {
	if r != nil {
		r.trustedToolImage = strings.TrimSpace(image)
	}

	return r
}

func (r *Reserver) toolImage(requested string) string {
	if r != nil && r.trustedToolImage != "" {
		return r.trustedToolImage
	}
	return requested
}

// ReservationRequest contains only the execution settings used to provision a PVC.
type ReservationRequest struct {
	SessionID  string
	TargetNode string
	ToolImage  string
}

// WithToolLogs enables log streaming for reservation consumer Pods.
func (r *Reserver) WithToolLogs(options ToolLogOptions) *Reserver {
	options.Namespaces = slices.Clone(options.Namespaces)
	options.Writer = NewSynchronizedWriter(options.Writer)
	r.toolLogs = &options
	return r
}

// WithLogger enables progress logs for reservation and cleanup waits.
func (r *Reserver) WithLogger(logger *slog.Logger) *Reserver {
	r.logger = logger
	return r
}

func (r *Reserver) waitFor(
	ctx context.Context,
	description string,
	condition func(context.Context) (bool, error),
) error {
	if r.logger != nil {
		r.logger.Info(
			"waiting for Kubernetes resource",
			"operation",
			"reservation",
			"description",
			description,
		)
	}

	return WaitFor(ctx, r.poll, description, condition)
}

func (r *Reserver) ValidateVolumeReservation(
	ctx context.Context,
	request ReservationRequest,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	sourceCapacity string,
	desired *corev1.PersistentVolumeClaim,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
) error {
	pvc, err := r.prepareReservation(
		ctx,
		request.SessionID,
		sourcePVC,
		sourcePV,
		sourceCapacity,
		desired,
	)
	if err != nil {
		return err
	}

	if err := validateReservationCheckpoint(sourcePVC.Name, pvc, checkpoint); err != nil {
		return err
	}

	destinationPVC, destinationPV := PVCReference(pvc), v1alpha1.ObjectReference{}
	if checkpoint.DestinationPVC != nil {
		destinationPVC = *checkpoint.DestinationPVC
	}

	if checkpoint.DestinationPV != nil {
		destinationPV = *checkpoint.DestinationPV
	}

	return r.reserveVolumeDryRun(
		ctx,
		request.SessionID,
		destinationPVC,
		destinationPV,
		checkpoint.Reserved,
		pvc,
	)
}

func (r *Reserver) ReserveVolume(
	ctx context.Context,
	request ReservationRequest,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	sourceCapacity string,
	desired *corev1.PersistentVolumeClaim,
	checkpoint *v1alpha1.ClusterVolumeReservationStatus,
) error {
	if checkpoint == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"reserve volume",
			"reservation status is required",
		)
	}

	pvc, err := r.prepareReservation(
		ctx,
		request.SessionID,
		sourcePVC,
		sourcePV,
		sourceCapacity,
		desired,
	)
	if err != nil {
		return err
	}

	if err := validateReservationCheckpoint(sourcePVC.Name, pvc, *checkpoint); err != nil {
		return err
	}

	storageClass := ""
	if pvc.Spec.StorageClassName != nil {
		storageClass = *pvc.Spec.StorageClassName
	}

	if err := r.validateDestinationPlacement(
		ctx,
		PVCReference(pvc),
		storageClass,
		request.TargetNode,
		pvc.Spec.Resources.Requests[corev1.ResourceStorage],
	); err != nil {
		return err
	}

	if err := AcquirePVC(ctx, r.client, sourcePVC, request.SessionID); err != nil {
		return err
	}

	if err := r.retainPV(
		ctx,
		sourcePV.Name,
		sourcePV.UID,
		request.SessionID,
		ResourceRoleSource,
	); err != nil {
		return err
	}

	checkpoint.SourcePVCName = sourcePVC.Name

	destinationPVC, destinationPV := PVCReference(pvc), v1alpha1.ObjectReference{}
	if checkpoint.DestinationPVC != nil {
		destinationPVC = *checkpoint.DestinationPVC
	}

	if checkpoint.DestinationPV != nil {
		destinationPV = *checkpoint.DestinationPV
	}

	err = r.reserveVolumeLive(
		ctx,
		request,
		sourcePVC.Name,
		pvc,
		&destinationPVC,
		&destinationPV,
		&checkpoint.DestinationPolicy,
		&checkpoint.Reserved,
	)
	if destinationPVC.UID != "" {
		if checkpoint.DestinationPVC == nil {
			checkpoint.DestinationPVC = &destinationPVC
		} else {
			*checkpoint.DestinationPVC = destinationPVC
		}
	}

	if destinationPV.UID != "" {
		if checkpoint.DestinationPV == nil {
			checkpoint.DestinationPV = &destinationPV
		} else {
			*checkpoint.DestinationPV = destinationPV
		}
	}

	return err
}

func (r *Reserver) prepareReservation(
	ctx context.Context,
	sessionID string,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	sourceCapacity string,
	desired *corev1.PersistentVolumeClaim,
) (*corev1.PersistentVolumeClaim, error) {
	if sessionID == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"reserve volume",
			"session ID is required",
		)
	}

	if desired == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"reserve volume",
			"destination PVC manifest is required",
		)
	}

	if sourcePVC.Namespace == "" || sourcePVC.Name == "" || sourcePVC.UID == "" ||
		sourcePV.Name == "" || sourcePV.UID == "" || desired.Namespace == "" || desired.Name == "" {
		return nil, domain.NewError(domain.ErrorValidation, "reserve volume",
			"source PVC, source PV, and destination PVC identities are required")
	}

	if err := r.verifySourceIdentity(
		ctx,
		sessionID,
		sourcePVC,
		sourcePV,
		sourceCapacity,
	); err != nil {
		return nil, err
	}

	if !HasWritableAccessMode(desired.Spec.AccessModes) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"reserve volume",
			fmt.Sprintf(
				"PVC %s/%s has no writable access mode",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
		)
	}

	capacity := desired.Spec.Resources.Requests[corev1.ResourceStorage]
	if capacity.Sign() <= 0 {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"reserve volume",
			"capacity must be positive",
		)
	}

	pvc := desired.DeepCopy()
	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}

	pvc.Labels[ManagedByLabel] = ManagedByValue
	pvc.Labels[SessionKey] = sessionID

	pvc.Labels[ResourceRoleLabel] = ResourceRoleDestination
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}

	pvc.Annotations[SessionKey] = sessionID
	pvc.Annotations[SourcePVCUIDAnnotation] = string(sourcePVC.UID)
	pvc.Annotations[SourcePVAnnotation] = sourcePV.Name

	return pvc, nil
}

func validateReservationCheckpoint(
	sourcePVCName string,
	pvc *corev1.PersistentVolumeClaim,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
) error {
	if checkpoint.SourcePVCName != "" && checkpoint.SourcePVCName != sourcePVCName {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"reservation checkpoint belongs to another source PVC",
		)
	}

	if ref := checkpoint.DestinationPVC; ref != nil &&
		(ref.Name != pvc.Name || ref.Namespace != pvc.Namespace) {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"reservation checkpoint differs from destination PVC manifest",
		)
	}

	return nil
}

func (r *Reserver) validateDestinationPlacement(
	ctx context.Context,
	destinationPVC v1alpha1.ObjectReference,
	storageClass, targetNode string,
	required resource.Quantity,
) error {
	existing, err := r.client.CoreV1().
		PersistentVolumeClaims(destinationPVC.Namespace).
		Get(ctx, destinationPVC.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve volume",
			fmt.Sprintf(
				"read destination PVC %s/%s before placement validation",
				destinationPVC.Namespace,
				destinationPVC.Name,
			),
			err,
		)
	}

	if err == nil && existing.Status.Phase == corev1.ClaimBound && existing.Spec.VolumeName != "" {
		pv, getErr := r.client.CoreV1().PersistentVolumes().Get(
			ctx,
			existing.Spec.VolumeName,
			metav1.GetOptions{},
		)
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"reserve volume",
				"read existing destination PV before capacity validation",
				getErr,
			)
		}

		if capacityErr := ValidateBoundVolumeCapacity(existing, pv, &required); capacityErr != nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"reserve volume",
				capacityErr.Error(),
			)
		}

		return nil
	}

	return ValidateStorageClassPlacement(
		ctx,
		r.client,
		storageClass,
		targetNode,
	)
}

func (r *Reserver) reserveVolumeDryRun(
	ctx context.Context,
	sessionID string,
	destinationPVC, destinationPV v1alpha1.ObjectReference,
	reserved bool,
	pvc *corev1.PersistentVolumeClaim,
) error {
	_, err := r.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(
		ctx,
		pvc,
		metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}},
	)
	if !apierrors.IsAlreadyExists(err) {
		if err != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"reserve volume dry-run",
				fmt.Sprintf("PVC %s/%s was rejected", pvc.Namespace, pvc.Name),
				err,
			)
		}

		return validateMissingReservationDestination(destinationPVC, destinationPV, reserved)
	}

	existing, getErr := r.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(
		ctx,
		pvc.Name,
		metav1.GetOptions{},
	)
	if getErr != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve volume dry-run",
			fmt.Sprintf("read existing PVC %s/%s", pvc.Namespace, pvc.Name),
			getErr,
		)
	}

	if err := validateDestinationPVC(existing, pvc, destinationPVC.UID); err != nil {
		return err
	}

	if reserved &&
		(destinationPVC.UID == "" || destinationPV.Name == "" || destinationPV.UID == "") {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve volume",
			fmt.Sprintf(
				"recorded destination identity for PVC %s/%s is incomplete",
				existing.Namespace,
				existing.Name,
			),
		)
	}

	if reserved || destinationPV.Name != "" ||
		existing.Status.Phase == corev1.ClaimBound {
		return r.verifyDestinationIdentity(
			ctx,
			existing,
			sessionID,
			destinationPVC,
			destinationPV,
			reserved,
		)
	}

	return nil
}

func (r *Reserver) reserveVolumeLive(
	ctx context.Context,
	request ReservationRequest,
	sourcePVCName string,
	pvc *corev1.PersistentVolumeClaim,
	destinationPVC, destinationPV *v1alpha1.ObjectReference,
	destinationPolicy *corev1.PersistentVolumeReclaimPolicy,
	reserved *bool,
) error {
	existing, err := r.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Get(ctx, pvc.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if err := validateMissingReservationDestination(
			*destinationPVC,
			*destinationPV,
			*reserved,
		); err != nil {
			return err
		}
		if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
			return err
		}

		existing, err = r.client.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Create(ctx, pvc, metav1.CreateOptions{})
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve volume",
			fmt.Sprintf("create PVC %s/%s", pvc.Namespace, pvc.Name),
			err,
		)
	}
	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	if err := validateDestinationPVC(existing, pvc, destinationPVC.UID); err != nil {
		return err
	}

	destinationPVC.UID = existing.UID

	destinationPVC.ResourceVersion = existing.ResourceVersion
	if existing.Status.Phase != corev1.ClaimBound {
		if err := r.provisionOnTarget(
			ctx,
			request,
			sourcePVCName,
			*destinationPVC,
		); err != nil {
			return err
		}
	} else if err := r.cleanupReservationPod(ctx, request.SessionID, sourcePVCName, *destinationPVC); err != nil {
		return err
	}

	bound, err := r.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve volume",
			"read bound destination PVC",
			err,
		)
	}

	if err := validateDestinationPVC(bound, pvc, destinationPVC.UID); err != nil {
		return err
	}

	if bound.Status.Phase != corev1.ClaimBound || bound.Spec.VolumeName == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve volume",
			fmt.Sprintf("PVC %s/%s did not bind", bound.Namespace, bound.Name),
		)
	}

	destinationPVC.UID = bound.UID
	destinationPVC.ResourceVersion = bound.ResourceVersion

	selectedNode := bound.Annotations["volume.kubernetes.io/selected-node"]
	if selectedNode != "" && request.TargetNode != "" && selectedNode != request.TargetNode {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve volume",
			fmt.Sprintf("PVC selected node %s, expected %s", selectedNode, request.TargetNode),
		)
	}

	pv, err := r.client.CoreV1().
		PersistentVolumes().
		Get(ctx, bound.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve volume",
			"read destination PV",
			err,
		)
	}

	if destinationPV.Name != "" &&
		(pv.Name != destinationPV.Name || destinationPV.UID == "" || pv.UID != destinationPV.UID) {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PV %s identity changed", destinationPV.Name),
		)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != bound.Namespace ||
		pv.Spec.ClaimRef.Name != bound.Name || pv.Spec.ClaimRef.UID != bound.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("PV %s claimRef does not match destination PVC UID", pv.Name),
		)
	}

	if err := r.validateDestinationPVNode(ctx, pv, request.TargetNode); err != nil {
		return err
	}

	capacity := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if err := ValidateBoundVolumeCapacity(bound, pv, &capacity); err != nil {
		return domain.NewError(domain.ErrorPrecondition, "reserve volume", err.Error())
	}

	if err := r.retainPV(
		ctx,
		pv.Name,
		pv.UID,
		request.SessionID,
		ResourceRoleDestination,
	); err != nil {
		return err
	}

	current, err := r.client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve volume",
			"read retained destination PV",
			err,
		)
	}

	if err := validateRetainedDestinationPV(current, pv.UID, bound, request.SessionID); err != nil {
		return err
	}

	*destinationPV = v1alpha1.ObjectReference{
		APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolume,
		Name: current.Name, UID: current.UID, ResourceVersion: current.ResourceVersion,
	}

	*destinationPolicy = corev1.PersistentVolumeReclaimPolicy(
		current.Annotations[OriginalPolicyAnnotation],
	)
	if *destinationPolicy == "" {
		*destinationPolicy = pv.Spec.PersistentVolumeReclaimPolicy
	}

	*reserved = true

	return nil
}

func (r *Reserver) validateDestinationPVNode(
	ctx context.Context,
	pv *corev1.PersistentVolume,
	targetNode string,
) error {
	if targetNode == "" {
		return nil
	}

	node, err := r.client.CoreV1().Nodes().Get(ctx, targetNode, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes, "reserve volume", "read target node for PV topology", err,
		)
	}

	if !PVSupportsNode(pv, node) {
		return domain.NewError(
			domain.ErrorPrecondition, "reserve volume",
			fmt.Sprintf("destination PV %s topology excludes target node %s", pv.Name, node.Name),
		)
	}

	return nil
}

func validateRetainedDestinationPV(
	pv *corev1.PersistentVolume,
	expectedUID types.UID,
	pvc *corev1.PersistentVolumeClaim,
	sessionID string,
) error {
	if pv.UID != expectedUID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"destination PV was replaced after retaining it",
		)
	}

	if err := validateReservationPVOwnership(
		pv,
		sessionID,
		ResourceRoleDestination,
		true,
	); err != nil {
		return err
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"destination PV binding changed after retaining it",
		)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"destination PV reclaim policy changed after retaining it",
		)
	}

	return nil
}

func validateMissingReservationDestination(pvc, pv v1alpha1.ObjectReference, reserved bool) error {
	if reserved || pvc.UID != "" || pv.Name != "" {
		return domain.NewError(domain.ErrorConflict, "reserve volume",
			fmt.Sprintf("recorded destination PVC %s/%s no longer exists", pvc.Namespace, pvc.Name))
	}

	return nil
}

func (r *Reserver) verifySourceIdentity(
	ctx context.Context,
	sessionID string,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	sourceCapacity string,
) error {
	pvc, err := r.client.CoreV1().
		PersistentVolumeClaims(sourcePVC.Namespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "reserve volume", "read source PVC", err)
	}

	if pvc.UID != sourcePVC.UID || pvc.Spec.VolumeName != sourcePV.Name ||
		pvc.Status.Phase != corev1.ClaimBound {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("source PVC %s/%s identity or binding changed", pvc.Namespace, pvc.Name),
		)
	}

	for _, owner := range []string{pvc.Annotations[SessionKey], pvc.Labels[SessionKey]} {
		if owner != "" && owner != sessionID {
			return domain.NewError(
				domain.ErrorConflict,
				"reserve volume",
				fmt.Sprintf(
					"source PVC %s/%s belongs to session %s",
					pvc.Namespace,
					pvc.Name,
					owner,
				),
			)
		}
	}

	pv, err := r.client.CoreV1().
		PersistentVolumes().
		Get(ctx, sourcePV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "reserve volume", "read source PV", err)
	}

	if pv.UID != sourcePV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("source PV %s identity or claimRef changed", pv.Name),
		)
	}

	if err := ValidateBoundVolumeCapacity(pvc, pv, nil); err != nil {
		return domain.NewError(domain.ErrorConflict, "reserve volume", err.Error())
	}

	if sourceCapacity != "" {
		planned, parseErr := resource.ParseQuantity(sourceCapacity)

		actual, hasActual := pv.Spec.Capacity[corev1.ResourceStorage]
		if parseErr != nil || !hasActual || actual.Cmp(planned) != 0 {
			return domain.NewError(
				domain.ErrorConflict,
				"reserve volume",
				fmt.Sprintf(
					"source PV %s capacity changed after planning; generate a new plan",
					pv.Name,
				),
			)
		}
	}

	if err := validateReservationPVOwnership(pv, sessionID, ResourceRoleSource, false); err != nil {
		return err
	}

	return nil
}

func validateDestinationPVC(
	pvc, expected *corev1.PersistentVolumeClaim,
	expectedUID types.UID,
) error {
	if pvc == nil || expected == nil || pvc.Namespace != expected.Namespace ||
		pvc.Name != expected.Name ||
		pvc.UID == "" {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"destination PVC identity is incomplete or differs from the planned manifest",
		)
	}

	if pvc.DeletionTimestamp != nil {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s is terminating", pvc.Namespace, pvc.Name),
		)
	}

	sessionID := expected.Labels[SessionKey]
	if pvc.Labels[ManagedByLabel] != ManagedByValue || pvc.Labels[SessionKey] != sessionID ||
		pvc.Labels[ResourceRoleLabel] != ResourceRoleDestination ||
		pvc.Annotations[SessionKey] != sessionID ||
		pvc.Annotations[SourcePVCUIDAnnotation] != expected.Annotations[SourcePVCUIDAnnotation] ||
		pvc.Annotations[SourcePVAnnotation] != expected.Annotations[SourcePVAnnotation] {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf(
				"destination PVC %s/%s belongs to another operation",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	if expectedUID != "" && pvc.UID != expectedUID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s UID changed", pvc.Namespace, pvc.Name),
		)
	}

	if pvc.Spec.StorageClassName == nil || expected.Spec.StorageClassName == nil ||
		*pvc.Spec.StorageClassName != *expected.Spec.StorageClassName {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s StorageClass changed", pvc.Namespace, pvc.Name),
		)
	}

	if effectiveVolumeMode(pvc) != effectiveVolumeMode(expected) {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s VolumeMode changed", pvc.Namespace, pvc.Name),
		)
	}

	if !accessModesEqual(pvc.Spec.AccessModes, expected.Spec.AccessModes) {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s AccessModes changed", pvc.Namespace, pvc.Name),
		)
	}

	capacity := expected.Spec.Resources.Requests[corev1.ResourceStorage]

	request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if request.Cmp(capacity) < 0 {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf(
				"destination PVC %s/%s capacity %s is below %s",
				pvc.Namespace,
				pvc.Name,
				request.String(),
				capacity.String(),
			),
		)
	}

	return nil
}

func (r *Reserver) verifyDestinationIdentity(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	sessionID string,
	destinationPVC, destinationPV v1alpha1.ObjectReference,
	requireOwned bool,
) error {
	if destinationPVC.UID != "" && pvc.UID != destinationPVC.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s UID changed", pvc.Namespace, pvc.Name),
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s binding changed", pvc.Namespace, pvc.Name),
		)
	}

	if destinationPV.Name != "" &&
		(destinationPV.UID == "" || pvc.Spec.VolumeName != destinationPV.Name) {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s binding changed", pvc.Namespace, pvc.Name),
		)
	}

	pvName := pvc.Spec.VolumeName

	pv, err := r.client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve volume",
			"read destination PV "+pvName,
			err,
		)
	}

	if destinationPV.UID != "" && pv.UID != destinationPV.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PV %s identity changed", pv.Name),
		)
	}

	if err := validateReservationPVOwnership(
		pv,
		sessionID,
		ResourceRoleDestination,
		requireOwned,
	); err != nil {
		return err
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PV %s identity or claimRef changed", pv.Name),
		)
	}

	return nil
}

func validateReservationPVOwnership(
	pv *corev1.PersistentVolume,
	sessionID, expectedRole string,
	requireOwned bool,
) error {
	owner := pv.Labels[SessionKey]
	role := pv.Labels[ResourceRoleLabel]

	managedBy := pv.Labels[ManagedByLabel]
	if owner == "" && role == "" && managedBy == "" && !requireOwned {
		return nil
	}

	if owner != sessionID || role != expectedRole || managedBy != ManagedByValue {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("PV %s has unexpected session ownership or role", pv.Name),
		)
	}

	return nil
}

func (r *Reserver) provisionOnTarget(
	ctx context.Context,
	request ReservationRequest,
	sourcePVCName string,
	destinationPVC v1alpha1.ObjectReference,
) error {
	toolImage, err := NormalizeToolImage(r.toolImage(request.ToolImage))
	if err != nil {
		return domain.WrapError(
			domain.ErrorValidation,
			"provision target PVC",
			"validate tool image",
			err,
		)
	}

	if request.TargetNode == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"provision target PVC",
			"target node is required",
		)
	}

	node, err := r.client.CoreV1().Nodes().Get(ctx, request.TargetNode, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"provision target PVC",
			"read target node",
			err,
		)
	}

	hostname := node.Labels[corev1.LabelHostname]
	if hostname == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"provision target PVC",
			fmt.Sprintf("node %s lacks %s", node.Name, corev1.LabelHostname),
		)
	}

	podName := toolPodName(request.SessionID, sourcePVCName)
	automountServiceAccountToken := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: destinationPVC.Namespace,
			Labels: map[string]string{
				ManagedByLabel:    ManagedByValue,
				SessionKey:        request.SessionID,
				ResourceRoleLabel: ResourceRoleReservationConsumer,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			AutomountServiceAccountToken:  &automountServiceAccountToken,
			TerminationGracePeriodSeconds: new(int64(1)),
			NodeSelector:                  map[string]string{corev1.LabelHostname: hostname},
			Tolerations:                   nodeTolerations(node),
			Containers: []corev1.Container{{
				Name:            "verify-volume",
				Image:           toolImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:  new(int64(0)),
					RunAsGroup: new(int64(0)),
				},
				Command:      []string{"sh", "-c", "test -d /data && exec sleep 3600"},
				Resources:    ZeroResourceRequirements(),
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: destinationPVC.Name,
						},
					},
				},
			},
		},
	}

	existing, err := r.client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
			return err
		}

		existing, err = r.client.CoreV1().
			Pods(pod.Namespace).
			Create(ctx, pod, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			existing, err = r.client.CoreV1().
				Pods(pod.Namespace).
				Get(ctx, pod.Name, metav1.GetOptions{})
		}
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"provision target PVC",
			fmt.Sprintf("create tool Pod %s/%s", pod.Namespace, pod.Name),
			err,
		)
	}
	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	if err := validateReservationPod(
		existing,
		request.SessionID,
		destinationPVC.Name,
	); err != nil {
		return err
	}

	var toolLogs *ToolLogStream
	if r.toolLogs != nil {
		toolLogs = StartPodLogs(ctx, r.client, existing, *r.toolLogs)
	}
	defer toolLogs.Stop()

	if err := r.waitFor(
		ctx,
		fmt.Sprintf("reservation Pod %s/%s readiness", pod.Namespace, pod.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := r.client.CoreV1().
				Pods(pod.Namespace).
				Get(waitCtx, pod.Name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}

			if current.UID != existing.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"provision target PVC",
					fmt.Sprintf(
						"tool Pod %s/%s was replaced while waiting for readiness",
						pod.Namespace,
						pod.Name,
					),
				)
			}

			if err := validateReservationPod(
				current,
				request.SessionID,
				destinationPVC.Name,
			); err != nil {
				return false, err
			}

			if current.Status.Phase == corev1.PodFailed {
				return false, domain.NewError(
					domain.ErrorPrecondition,
					"provision target PVC",
					fmt.Sprintf("tool Pod %s/%s failed", pod.Namespace, pod.Name),
				)
			}

			return PodReady(current), nil
		},
	); err != nil {
		return err
	}
	// Stop following before deleting the short-lived Pod. Kubelet may remove
	// its log file during deletion, which otherwise surfaces a misleading
	// "failed to try resolving symlinks" line as tool output.
	toolLogs.Stop()

	return r.cleanupReservationPod(ctx, request.SessionID, sourcePVCName, destinationPVC)
}

func (r *Reserver) cleanupReservationPod(
	ctx context.Context,
	sessionID string,
	sourcePVCName string,
	destinationPVC v1alpha1.ObjectReference,
) error {
	namespace := destinationPVC.Namespace
	name := toolPodName(sessionID, sourcePVCName)

	pod, err := r.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"clean up reservation Pod",
			fmt.Sprintf("read tool Pod %s/%s", namespace, name),
			err,
		)
	}

	if err := validateReservationPod(
		pod,
		sessionID,
		destinationPVC.Name,
	); err != nil {
		return err
	}

	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}
	if err := r.client.CoreV1().
		Pods(namespace).
		Delete(ctx, name, options); err != nil &&
		!apierrors.IsNotFound(err) {
		if apierrors.IsConflict(err) {
			return domain.WrapError(
				domain.ErrorConflict,
				"clean up reservation Pod",
				fmt.Sprintf("tool Pod %s/%s changed while deleting", namespace, name),
				err,
			)
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"clean up reservation Pod",
			fmt.Sprintf("delete tool Pod %s/%s", namespace, name),
			err,
		)
	}
	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	return r.waitFor(
		ctx,
		fmt.Sprintf("reservation Pod %s/%s deletion", namespace, name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := r.client.CoreV1().
				Pods(namespace).
				Get(waitCtx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}

			if getErr == nil && current.UID != pod.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"clean up reservation Pod",
					fmt.Sprintf("tool Pod %s/%s name was reused", namespace, name),
				)
			}

			return false, getErr
		},
	)
}

func validateReservationPod(pod *corev1.Pod, sessionID, destinationPVC string) error {
	if pod == nil || pod.Namespace == "" || pod.Name == "" || pod.UID == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"validate reservation Pod",
			"Kubernetes returned an incomplete reservation Pod identity",
		)
	}

	if pod.Labels[ManagedByLabel] != ManagedByValue || pod.Labels[SessionKey] != sessionID ||
		pod.Labels[ResourceRoleLabel] != ResourceRoleReservationConsumer {
		return domain.NewError(
			domain.ErrorConflict,
			"validate reservation Pod",
			fmt.Sprintf(
				"tool Pod %s/%s is not owned by session %s as a reservation consumer",
				pod.Namespace,
				pod.Name,
				sessionID,
			),
		)
	}

	if !PodUsesPVC(pod, destinationPVC) {
		return domain.NewError(
			domain.ErrorConflict,
			"validate reservation Pod",
			fmt.Sprintf(
				"tool Pod %s/%s does not mount destination PVC %s",
				pod.Namespace,
				pod.Name,
				destinationPVC,
			),
		)
	}

	return nil
}

func (r *Reserver) retainPV(
	ctx context.Context,
	name string,
	uid types.UID,
	sessionID, role string,
) error {
	if name == "" || uid == "" || sessionID == "" || role == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"retain PV",
			"PV name, UID, session ID, and role are required",
		)
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pv, err := r.client.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if pv.UID != uid {
			return domain.NewError(
				domain.ErrorConflict,
				"retain PV",
				fmt.Sprintf("PV %s UID changed", name),
			)
		}

		if owner := pv.Labels[SessionKey]; owner != "" && owner != sessionID {
			return domain.NewError(
				domain.ErrorConflict,
				"retain PV",
				fmt.Sprintf("PV %s belongs to session %s", name, owner),
			)
		}

		if pv.Labels == nil {
			pv.Labels = map[string]string{}
		}

		if pv.Annotations == nil {
			pv.Annotations = map[string]string{}
		}

		changed := markPVSession(pv.Labels, sessionID, role)
		if pv.Annotations[OriginalPolicyAnnotation] == "" {
			pv.Annotations[OriginalPolicyAnnotation] = string(pv.Spec.PersistentVolumeReclaimPolicy)
			changed = true
		}

		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
			changed = true
		}

		if !changed {
			return nil
		}

		if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
			return err
		}

		_, err = r.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

		return errors.Join(err, ctx.Err(), LeaseFenceError(ctx))
	})
	if err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}
		return domain.WrapError(domain.ErrorKubernetes, "retain PV", "update PV "+name, err)
	}

	return nil
}

func toolPodName(sessionID, pvcName string) string {
	return BoundedName("pvc-migrate-bind", sessionID, pvcName)
}

func nodeTolerations(node *corev1.Node) []corev1.Toleration {
	result := make([]corev1.Toleration, 0, len(node.Spec.Taints))
	for _, taint := range node.Spec.Taints {
		if taint.Effect != corev1.TaintEffectNoSchedule &&
			taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}

		toleration := corev1.Toleration{Key: taint.Key, Effect: taint.Effect}
		if taint.Value == "" {
			toleration.Operator = corev1.TolerationOpExists
		} else {
			toleration.Operator = corev1.TolerationOpEqual
			toleration.Value = taint.Value
		}

		result = append(result, toleration)
	}

	return result
}

// PVSupportsNode reports whether a PV's required node affinity permits a node.
func PVSupportsNode(pv *corev1.PersistentVolume, node *corev1.Node) bool {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil ||
		len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return true
	}

	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		matched := true
		for _, requirement := range term.MatchExpressions {
			actual, exists := node.Labels[requirement.Key]
			if !exists && requirement.Key == "kubernetes.io/hostname" && node.Name != "" {
				actual, exists = node.Name, true
			}

			if !nodeRequirementMatches(requirement, actual, exists) {
				matched = false
				break
			}
		}

		for _, requirement := range term.MatchFields {
			actual, exists := nodeFieldValue(node, requirement.Key)
			if !nodeRequirementMatches(requirement, actual, exists) {
				matched = false
				break
			}
		}

		if matched {
			return true
		}
	}

	return false
}

// PVUniqueNodeName returns the object name of the sole current node allowed by
// the PV's required node affinity. An empty result leaves placement to the
// scheduler. Resolving against Node objects also handles hostname labels whose
// values differ from metadata.name.
func PVUniqueNodeName(pv *corev1.PersistentVolume, nodes []corev1.Node) string {
	if pv == nil || pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil ||
		len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		return ""
	}

	var candidate string
	for index := range nodes {
		if !PVSupportsNode(pv, &nodes[index]) {
			continue
		}

		if candidate != "" {
			return ""
		}

		candidate = nodes[index].Name
	}

	return candidate
}

func nodeFieldValue(node *corev1.Node, key string) (string, bool) {
	switch key {
	case "metadata.name":
		return node.Name, node.Name != ""
	case "metadata.uid":
		return string(node.UID), node.UID != ""
	default:
		return "", false
	}
}

func effectiveVolumeMode(pvc *corev1.PersistentVolumeClaim) corev1.PersistentVolumeMode {
	if pvc.Spec.VolumeMode == nil {
		return corev1.PersistentVolumeFilesystem
	}
	return *pvc.Spec.VolumeMode
}

func accessModesEqual(left, right []corev1.PersistentVolumeAccessMode) bool {
	leftCopy := append([]corev1.PersistentVolumeAccessMode(nil), left...)
	rightCopy := append([]corev1.PersistentVolumeAccessMode(nil), right...)

	slices.Sort(leftCopy)
	slices.Sort(rightCopy)

	if len(leftCopy) != len(rightCopy) {
		return false
	}

	for i := range leftCopy {
		if leftCopy[i] != rightCopy[i] {
			return false
		}
	}

	return true
}

func HasWritableAccessMode(modes []corev1.PersistentVolumeAccessMode) bool {
	for _, mode := range modes {
		switch mode {
		case corev1.ReadWriteOnce, corev1.ReadWriteOncePod, corev1.ReadWriteMany:
			return true
		}
	}

	return false
}

func HasAccessMode(
	modes []corev1.PersistentVolumeAccessMode,
	wanted corev1.PersistentVolumeAccessMode,
) bool {
	return slices.Contains(modes, wanted)
}

func PodUsesPVC(pod *corev1.Pod, claim string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim {
			return true
		}
	}

	return false
}

func ActivePodUsesPVC(pod *corev1.Pod, claim string) bool {
	return pod.Status.Phase != corev1.PodSucceeded &&
		pod.Status.Phase != corev1.PodFailed &&
		PodUsesPVC(pod, claim)
}

// PodBlocksPVCDeletion follows the PVC protection controller's boundary for
// ordinary PVC volumes: only a scheduled Pod can have reached kubelet and
// mounted the claim.
func PodBlocksPVCDeletion(pod *corev1.Pod, claim string) bool {
	return pod.Spec.NodeName != "" && PodUsesPVC(pod, claim)
}

// PodPreventsSafePVCDeletion includes active Pods that may still be scheduled
// and terminal Pods that remain inside the PVC protection boundary.
func PodPreventsSafePVCDeletion(pod *corev1.Pod, claim string) bool {
	return ActivePodUsesPVC(pod, claim) || PodBlocksPVCDeletion(pod, claim)
}

func nodeRequirementMatches(
	requirement corev1.NodeSelectorRequirement,
	actual string,
	exists bool,
) bool {
	switch requirement.Operator {
	case corev1.NodeSelectorOpIn:
		return exists && slices.Contains(requirement.Values, actual)
	case corev1.NodeSelectorOpNotIn:
		return !exists || !slices.Contains(requirement.Values, actual)
	case corev1.NodeSelectorOpExists:
		return exists
	case corev1.NodeSelectorOpDoesNotExist:
		return !exists
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		if !exists || len(requirement.Values) != 1 {
			return false
		}

		left, leftErr := strconv.ParseInt(actual, 10, 64)

		right, rightErr := strconv.ParseInt(requirement.Values[0], 10, 64)
		if leftErr != nil || rightErr != nil {
			return false
		}

		if requirement.Operator == corev1.NodeSelectorOpGt {
			return left > right
		}

		return left < right
	default:
		return false
	}
}

//go:fix inline
