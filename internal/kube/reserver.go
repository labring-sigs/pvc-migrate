package kube

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

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
	client   kubernetes.Interface
	poll     time.Duration
	toolLogs *ToolLogOptions
	logger   *slog.Logger
}

func NewReserver(client kubernetes.Interface) *Reserver {
	return &Reserver{client: client, poll: time.Second}
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

func (r *Reserver) ReserveVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	dryRun bool,
) error {
	if session == nil || session.ID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"reserve volume",
			"session and session ID are required",
		)
	}

	if volume == nil || status == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"reserve volume",
			"volume specification and status are required",
		)
	}

	if !validReservationIdentities(volume) {
		return domain.NewError(
			domain.ErrorValidation,
			"reserve volume",
			"source PVC, source PV, and destination PVC identities are required",
		)
	}

	if err := r.verifySourceIdentity(ctx, session.ID, volume); err != nil {
		return err
	}

	if !HasWritableAccessMode(volume.AccessModes) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve volume",
			fmt.Sprintf(
				"PVC %s/%s has no writable access mode",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if !dryRun {
		if err := r.validateDestinationPlacement(ctx, session, volume); err != nil {
			return err
		}
	}

	if !dryRun {
		if err := AcquirePVC(ctx, r.client, volume.SourcePVC, session.ID); err != nil {
			return err
		}

		if err := r.retainPV(
			ctx,
			volume.SourcePV.Name,
			volume.SourcePV.UID,
			session.ID,
			ResourceRoleSource,
		); err != nil {
			return err
		}
	}

	capacity, err := resource.ParseQuantity(volume.Capacity)
	if err != nil {
		return domain.WrapError(domain.ErrorInternal, "reserve volume", "parse capacity", err)
	}

	pvc := destinationPVCForReservation(session, volume, capacity)
	if dryRun {
		return r.reserveVolumeDryRun(ctx, session, volume, status, pvc, capacity)
	}

	return r.reserveVolumeLive(ctx, session, volume, status, pvc, capacity)
}

func (r *Reserver) validateDestinationPlacement(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
) error {
	existing, err := r.client.CoreV1().
		PersistentVolumeClaims(volume.DestinationPVC.Namespace).
		Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"reserve volume",
			fmt.Sprintf(
				"read destination PVC %s/%s before placement validation",
				volume.DestinationPVC.Namespace,
				volume.DestinationPVC.Name,
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

		required, parseErr := resource.ParseQuantity(volume.Capacity)
		if parseErr != nil || required.Sign() <= 0 {
			return domain.NewError(
				domain.ErrorValidation,
				"reserve volume",
				fmt.Sprintf("destination capacity %q is invalid", volume.Capacity),
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
		volume.StorageClass,
		session.Spec.WorkflowOptions().TargetNode,
	)
}

func validReservationIdentities(volume *domain.VolumeSpec) bool {
	return volume.SourcePVC.Namespace != "" && volume.SourcePVC.Name != "" &&
		volume.SourcePVC.UID != "" && volume.SourcePV.Name != "" &&
		volume.SourcePV.UID != "" && volume.DestinationPVC.Namespace != "" &&
		volume.DestinationPVC.Name != ""
}

func destinationPVCForReservation(
	session *domain.Session,
	volume *domain.VolumeSpec,
	capacity resource.Quantity,
) *corev1.PersistentVolumeClaim {
	storageClass := volume.StorageClass
	volumeMode := volume.VolumeMode

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volume.DestinationPVC.Name,
			Namespace: volume.DestinationPVC.Namespace,
			Labels: map[string]string{
				ManagedByLabel:    ManagedByValue,
				SessionKey:        session.ID,
				ResourceRoleLabel: ResourceRoleDestination,
			},
			Annotations: map[string]string{
				SessionKey:             session.ID,
				SourcePVCUIDAnnotation: string(volume.SourcePVC.UID),
				SourcePVAnnotation:     volume.SourcePV.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: append(
				[]corev1.PersistentVolumeAccessMode(nil),
				volume.AccessModes...,
			),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: capacity},
			},
			StorageClassName: &storageClass,
			VolumeMode:       &volumeMode,
		},
	}
}

func (r *Reserver) reserveVolumeDryRun(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	pvc *corev1.PersistentVolumeClaim,
	capacity resource.Quantity,
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

		return nil
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

	if err := validateDestinationPVC(existing, session.ID, volume, capacity); err != nil {
		return err
	}

	if status.Reserved &&
		(volume.DestinationPVC.UID == "" || volume.DestinationPV.Name == "" || volume.DestinationPV.UID == "") {
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

	if status.Reserved || volume.DestinationPV.Name != "" ||
		existing.Status.Phase == corev1.ClaimBound {
		return r.verifyDestinationIdentity(ctx, existing, session.ID, volume, status.Reserved)
	}

	return nil
}

func (r *Reserver) reserveVolumeLive(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	pvc *corev1.PersistentVolumeClaim,
	capacity resource.Quantity,
) error {
	existing, err := r.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Get(ctx, pvc.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
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

	if err := validateDestinationPVC(existing, session.ID, volume, capacity); err != nil {
		return err
	}

	volume.DestinationPVC.UID = existing.UID

	volume.DestinationPVC.ResourceVersion = existing.ResourceVersion
	if existing.Status.Phase != corev1.ClaimBound {
		if err := r.provisionOnTarget(ctx, session, volume); err != nil {
			return err
		}
	} else if err := r.cleanupReservationPod(ctx, session, volume); err != nil {
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

	if err := validateDestinationPVC(bound, session.ID, volume, capacity); err != nil {
		return err
	}

	if bound.Status.Phase != corev1.ClaimBound || bound.Spec.VolumeName == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve volume",
			fmt.Sprintf("PVC %s/%s did not bind", bound.Namespace, bound.Name),
		)
	}

	volume.DestinationPVC.UID = bound.UID
	volume.DestinationPVC.ResourceVersion = bound.ResourceVersion
	options := session.Spec.WorkflowOptions()

	selectedNode := bound.Annotations["volume.kubernetes.io/selected-node"]
	if selectedNode != "" && options.TargetNode != "" && selectedNode != options.TargetNode {
		return domain.NewError(
			domain.ErrorPrecondition,
			"reserve volume",
			fmt.Sprintf("PVC selected node %s, expected %s", selectedNode, options.TargetNode),
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

	if volume.DestinationPV.Name != "" &&
		(pv.Name != volume.DestinationPV.Name || volume.DestinationPV.UID == "" || pv.UID != volume.DestinationPV.UID) {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PV %s identity changed", volume.DestinationPV.Name),
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

	if options.TargetNode != "" {
		node, nodeErr := r.client.CoreV1().Nodes().Get(ctx, options.TargetNode, metav1.GetOptions{})
		if nodeErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"reserve volume",
				"read target node for PV topology",
				nodeErr,
			)
		}

		if !PVSupportsNode(pv, node) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"reserve volume",
				fmt.Sprintf(
					"destination PV %s topology excludes target node %s",
					pv.Name,
					node.Name,
				),
			)
		}
	}

	if err := ValidateBoundVolumeCapacity(bound, pv, &capacity); err != nil {
		return domain.NewError(domain.ErrorPrecondition, "reserve volume", err.Error())
	}

	if err := r.retainPV(ctx, pv.Name, pv.UID, session.ID, ResourceRoleDestination); err != nil {
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

	volume.DestinationPV = domain.ObjectReference{
		APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolume,
		Name: current.Name, UID: current.UID, ResourceVersion: current.ResourceVersion,
	}

	volume.DestinationPolicy = corev1.PersistentVolumeReclaimPolicy(
		current.Annotations[OriginalPolicyAnnotation],
	)
	if volume.DestinationPolicy == "" {
		volume.DestinationPolicy = pv.Spec.PersistentVolumeReclaimPolicy
	}

	status.Reserved = true

	return nil
}

func (r *Reserver) verifySourceIdentity(
	ctx context.Context,
	sessionID string,
	volume *domain.VolumeSpec,
) error {
	pvc, err := r.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "reserve volume", "read source PVC", err)
	}

	if pvc.UID != volume.SourcePVC.UID || pvc.Spec.VolumeName != volume.SourcePV.Name ||
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
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "reserve volume", "read source PV", err)
	}

	if pv.UID != volume.SourcePV.UID || pv.Spec.ClaimRef == nil ||
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

	if volume.SourceCapacity != "" {
		planned, parseErr := resource.ParseQuantity(volume.SourceCapacity)

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
	pvc *corev1.PersistentVolumeClaim,
	sessionID string,
	volume *domain.VolumeSpec,
	capacity resource.Quantity,
) error {
	if pvc.DeletionTimestamp != nil {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s is terminating", pvc.Namespace, pvc.Name),
		)
	}

	if pvc.Labels[ManagedByLabel] != ManagedByValue || pvc.Labels[SessionKey] != sessionID ||
		pvc.Labels[ResourceRoleLabel] != ResourceRoleDestination ||
		pvc.Annotations[SessionKey] != sessionID ||
		pvc.Annotations[SourcePVCUIDAnnotation] != string(volume.SourcePVC.UID) ||
		pvc.Annotations[SourcePVAnnotation] != volume.SourcePV.Name {
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

	if volume.DestinationPVC.UID != "" && pvc.UID != volume.DestinationPVC.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s UID changed", pvc.Namespace, pvc.Name),
		)
	}

	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != volume.StorageClass {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s StorageClass changed", pvc.Namespace, pvc.Name),
		)
	}

	if effectiveVolumeMode(pvc) != volume.VolumeMode {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s VolumeMode changed", pvc.Namespace, pvc.Name),
		)
	}

	if !accessModesEqual(pvc.Spec.AccessModes, volume.AccessModes) {
		return domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			fmt.Sprintf("destination PVC %s/%s AccessModes changed", pvc.Namespace, pvc.Name),
		)
	}

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
	volume *domain.VolumeSpec,
	requireOwned bool,
) error {
	if volume.DestinationPVC.UID != "" && pvc.UID != volume.DestinationPVC.UID {
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

	if volume.DestinationPV.Name != "" &&
		(volume.DestinationPV.UID == "" || pvc.Spec.VolumeName != volume.DestinationPV.Name) {
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

	if volume.DestinationPV.UID != "" && pv.UID != volume.DestinationPV.UID {
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
	session *domain.Session,
	volume *domain.VolumeSpec,
) error {
	options := session.Spec.WorkflowOptions()

	toolImage, err := NormalizeToolImage(options.ToolImage)
	if err != nil {
		return domain.WrapError(
			domain.ErrorValidation,
			"provision target PVC",
			"validate tool image",
			err,
		)
	}

	if options.TargetNode == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"provision target PVC",
			"target node is required",
		)
	}

	node, err := r.client.CoreV1().Nodes().Get(ctx, options.TargetNode, metav1.GetOptions{})
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

	podName := toolPodName(session.ID, volume.SourcePVC.Name)
	automountServiceAccountToken := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: volume.DestinationPVC.Namespace,
			Labels: map[string]string{
				ManagedByLabel:    ManagedByValue,
				SessionKey:        session.ID,
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
							ClaimName: volume.DestinationPVC.Name,
						},
					},
				},
			},
		},
	}

	existing, err := r.client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
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

	if err := validateReservationPod(existing, session.ID, volume.DestinationPVC.Name); err != nil {
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
				session.ID,
				volume.DestinationPVC.Name,
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

	return r.cleanupReservationPod(ctx, session, volume)
}

func (r *Reserver) cleanupReservationPod(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
) error {
	namespace := volume.DestinationPVC.Namespace
	name := toolPodName(session.ID, volume.SourcePVC.Name)

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

	if err := validateReservationPod(pod, session.ID, volume.DestinationPVC.Name); err != nil {
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

		_, err = r.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

		return err
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
