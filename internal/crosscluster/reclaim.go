package crosscluster

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) validateCleanupConsumers(
	ctx context.Context,
	session *Session,
	index int,
	pvc *corev1.PersistentVolumeClaim,
) error {
	pods, err := s.destination.Kubernetes.CoreV1().
		Pods(pvc.Namespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	reserved := session.Status.Volumes[index].Reservation.ConsumerPod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Name == reserved.Name && pod.Namespace == reserved.Namespace &&
			pod.UID == reserved.UID &&
			pod.Labels[SessionKey] == session.ID &&
			pod.Labels[ManagedByLabel] == ManagedBy {
			continue
		}

		if kube.ActivePodUsesPVC(pod, pvc.Name) {
			return fmt.Errorf(
				"destination PVC %s/%s has consumer Pod %s; stop it before cleanup or select Retain",
				pvc.Namespace,
				pvc.Name,
				pod.Name,
			)
		}
	}

	return nil
}

func (s *Service) inspectCleanupDestination(
	ctx context.Context,
	session *Session,
	index int,
	deleting bool,
) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, error) {
	v := session.Spec.Volumes[index].Destination

	pvc, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumeClaims(v.PVC.Namespace).
		Get(ctx, v.PVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		pvc = nil
	} else if err != nil {
		return nil, nil, err
	}

	if pvc != nil {
		owned := pvc.Labels[SessionKey] == session.ID && pvc.Labels[ManagedByLabel] == ManagedBy

		released := !deleting && v.PVC.UID == pvc.UID && pvc.Labels[SessionKey] == "" &&
			pvc.Labels[ManagedByLabel] == ""
		if (v.PVC.UID != "" && v.PVC.UID != pvc.UID) || (!owned && !released) {
			return nil, nil, fmt.Errorf(
				"destination PVC %s/%s UID or ownership changed",
				pvc.Namespace,
				pvc.Name,
			)
		}

		if v.PV.Name != "" && pvc.Spec.VolumeName != "" && v.PV.Name != pvc.Spec.VolumeName {
			return nil, nil, fmt.Errorf(
				"destination PVC %s/%s binding changed",
				pvc.Namespace,
				pvc.Name,
			)
		}

		v.PVC.UID = pvc.UID
		if v.PV.Name == "" {
			v.PV.Name = pvc.Spec.VolumeName
		}
	}

	if v.PV.Name == "" {
		return pvc, nil, nil
	}

	pv, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumes().
		Get(ctx, v.PV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return pvc, nil, nil
	}

	if err != nil {
		return nil, nil, err
	}

	claim := pv.Spec.ClaimRef
	if (v.PV.UID != "" && v.PV.UID != pv.UID) || claim == nil || v.PVC.UID == "" ||
		claim.UID != v.PVC.UID || claim.Name != v.PVC.Name || claim.Namespace != v.PVC.Namespace {
		return nil, nil, fmt.Errorf(
			"destination PV %s identity or claim reference changed",
			pv.Name,
		)
	}

	return pvc, pv, nil
}

func (s *Service) retainDestinationVolume(ctx context.Context, session *Session, index int) error {
	if err := s.deleteReservationConsumer(ctx, session, index); err != nil {
		return err
	}

	pvc, pv, err := s.inspectCleanupDestination(ctx, session, index, false)
	if err != nil {
		return err
	}
	// Persist recovered identities before releasing ownership so retries can
	// distinguish our retained output from a same-name replacement.
	v := &session.Spec.Volumes[index].Destination
	if pvc != nil {
		v.PVC.UID = pvc.UID
	}

	if pv != nil {
		v.PV.Name, v.PV.UID = pv.Name, pv.UID
		v.PV.ClusterID, v.PV.Kind = session.Spec.DestinationCluster.ID, "PersistentVolume"
	}

	if err := s.save(ctx, session, false); err != nil {
		return err
	}

	if pv != nil && (pvc == nil || pvc.DeletionTimestamp != nil) &&
		pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		if err := requireSessionLease(ctx); err != nil {
			return err
		}

		pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		if _, err := s.destination.Kubernetes.CoreV1().
			PersistentVolumes().
			Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
			return err
		}

		if err := requireSessionLease(ctx); err != nil {
			return err
		}
	}

	if pvc != nil {
		if err := requireSessionLease(ctx); err != nil {
			return err
		}

		delete(pvc.Labels, SessionKey)
		delete(pvc.Labels, ManagedByLabel)
		_, err = s.destination.Kubernetes.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Update(ctx, pvc, metav1.UpdateOptions{})
		if err == nil {
			err = requireSessionLease(ctx)
		}
	}

	return err
}
