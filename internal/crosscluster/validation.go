package crosscluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func hasSharedAccessMode(modes []corev1.PersistentVolumeAccessMode) bool {
	for _, mode := range modes {
		if mode == corev1.ReadWriteMany || mode == corev1.ReadOnlyMany {
			return true
		}
	}

	return false
}

func (s *Service) validateClients() error {
	if s == nil || s.source == nil || s.destination == nil || s.source.Kubernetes == nil ||
		s.destination.Kubernetes == nil {
		return errors.New("source and destination Kubernetes clients are required")
	}

	return nil
}

func (s *Service) validateSession(ctx context.Context, session *Session) error {
	if err := session.Validate(); err != nil {
		return err
	}

	src, err := kube.Identity(ctx, s.source)
	if err != nil {
		return err
	}

	dst, err := kube.Identity(ctx, s.destination)
	if err != nil {
		return err
	}

	if src.ID != session.Spec.SourceCluster.ID || dst.ID != session.Spec.DestinationCluster.ID {
		return errors.New("connected cluster identity does not match session")
	}

	return nil
}

func (s *Service) validateTransferVolume(ctx context.Context, session *Session, index int) error {
	if index < 0 || index >= len(session.Spec.Volumes) {
		return fmt.Errorf("cross-cluster volume index %d is invalid", index)
	}

	volume := &session.Spec.Volumes[index]

	sourcePVC, err := s.source.Kubernetes.CoreV1().
		PersistentVolumeClaims(volume.Source.PVC.Namespace).
		Get(ctx, volume.Source.PVC.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf(
			"read source PVC %s/%s before copy: %w",
			volume.Source.PVC.Namespace,
			volume.Source.PVC.Name,
			err,
		)
	}

	if sourcePVC.UID != volume.Source.PVC.UID ||
		sourcePVC.Spec.VolumeName != volume.Source.PV.Name ||
		sourcePVC.Status.Phase != corev1.ClaimBound {
		return fmt.Errorf(
			"source PVC %s/%s identity or binding changed; generate a new session",
			sourcePVC.Namespace,
			sourcePVC.Name,
		)
	}

	sourcePV, err := s.source.Kubernetes.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.Source.PV.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read source PV %s before copy: %w", volume.Source.PV.Name, err)
	}

	if sourcePV.UID != volume.Source.PV.UID || sourcePV.Spec.ClaimRef == nil ||
		sourcePV.Spec.ClaimRef.Namespace != sourcePVC.Namespace ||
		sourcePV.Spec.ClaimRef.Name != sourcePVC.Name ||
		sourcePV.Spec.ClaimRef.UID != sourcePVC.UID {
		return fmt.Errorf(
			"source PV %s identity or claim reference changed; generate a new session",
			sourcePV.Name,
		)
	}

	sourceCapacity, parseErr := resource.ParseQuantity(volume.Source.Capacity)

	actualSourceCapacity, hasSourceCapacity := sourcePV.Spec.Capacity[corev1.ResourceStorage]
	if parseErr != nil || !hasSourceCapacity || actualSourceCapacity.Cmp(sourceCapacity) != 0 {
		return fmt.Errorf("source PV %s capacity changed; generate a new session", sourcePV.Name)
	}

	if err := kube.ValidateBoundVolumeCapacity(sourcePVC, sourcePV, nil); err != nil {
		return err
	}

	consumers, err := activeConsumers(ctx, s.source.Kubernetes, sourcePVC.Namespace, sourcePVC.Name)
	if err != nil {
		return fmt.Errorf(
			"check source PVC %s/%s consumers: %w",
			sourcePVC.Namespace,
			sourcePVC.Name,
			err,
		)
	}

	if !session.Spec.Online && len(consumers) > 0 {
		return fmt.Errorf(
			"source PVC %s/%s has active consumers: %s",
			sourcePVC.Namespace,
			sourcePVC.Name,
			strings.Join(consumers, ", "),
		)
	}

	if session.Spec.Online && len(consumers) > 0 &&
		!hasSharedAccessMode(sourcePVC.Spec.AccessModes) {
		return fmt.Errorf(
			"online cross-cluster copy requires RWX/ROX while PVC %s/%s has active consumers",
			sourcePVC.Namespace,
			sourcePVC.Name,
		)
	}

	return s.validateDestinationVolume(ctx, session, index)
}

func (s *Service) validateDestinationVolume(
	ctx context.Context,
	session *Session,
	index int,
) error {
	if index < 0 || index >= len(session.Spec.Volumes) {
		return fmt.Errorf("cross-cluster volume index %d is invalid", index)
	}

	volume := &session.Spec.Volumes[index]

	destinationPVC, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumeClaims(volume.Destination.PVC.Namespace).
		Get(ctx, volume.Destination.PVC.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf(
			"read destination PVC %s/%s before copy: %w",
			volume.Destination.PVC.Namespace,
			volume.Destination.PVC.Name,
			err,
		)
	}

	if destinationPVC.UID != volume.Destination.PVC.UID ||
		destinationPVC.Labels[ManagedByLabel] != ManagedBy ||
		destinationPVC.Labels[SessionKey] != session.ID {
		return fmt.Errorf(
			"destination PVC %s/%s identity or ownership changed; refusing to copy",
			destinationPVC.Namespace,
			destinationPVC.Name,
		)
	}

	if err := validateDestinationPVCSpec(destinationPVC, volume); err != nil {
		return err
	}

	if destinationPVC.Spec.VolumeName == "" ||
		destinationPVC.Spec.VolumeName != volume.Destination.PV.Name {
		return fmt.Errorf(
			"destination PVC %s/%s binding changed; refusing to copy",
			destinationPVC.Namespace,
			destinationPVC.Name,
		)
	}

	destinationPV, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.Destination.PV.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read destination PV %s before copy: %w", volume.Destination.PV.Name, err)
	}

	if destinationPV.UID != volume.Destination.PV.UID || destinationPV.Spec.ClaimRef == nil ||
		destinationPV.Spec.ClaimRef.Namespace != destinationPVC.Namespace ||
		destinationPV.Spec.ClaimRef.Name != destinationPVC.Name ||
		destinationPV.Spec.ClaimRef.UID != destinationPVC.UID {
		return fmt.Errorf(
			"destination PV %s identity or claim reference changed; refusing to copy",
			destinationPV.Name,
		)
	}

	destinationCapacity, parseErr := resource.ParseQuantity(volume.Destination.Capacity)

	actualDestinationCapacity, hasDestinationCapacity := destinationPV.Spec.Capacity[corev1.ResourceStorage]
	if parseErr != nil || !hasDestinationCapacity ||
		actualDestinationCapacity.Cmp(destinationCapacity) < 0 {
		return fmt.Errorf(
			"destination PV %s capacity is smaller than the session request; refusing to copy",
			destinationPV.Name,
		)
	}

	if err := kube.ValidateBoundVolumeCapacity(
		destinationPVC,
		destinationPV,
		&destinationCapacity,
	); err != nil {
		return err
	}

	return nil
}
