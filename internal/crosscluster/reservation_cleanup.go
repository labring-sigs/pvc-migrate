package crosscluster

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) CleanupReservation(
	ctx context.Context,
	session *ReservationSession,
	unusedStoragePolicy string,
	deleteSession bool,
) error {
	if s.locker != nil {
		return s.withReservationLock(ctx, session, func(locked context.Context) error {
			return s.cleanupReservation(locked, session, unusedStoragePolicy, deleteSession)
		})
	}

	return s.cleanupReservation(ctx, session, unusedStoragePolicy, deleteSession)
}

func (s *Service) cleanupReservation(
	ctx context.Context,
	session *ReservationSession,
	unusedStoragePolicy string,
	deleteSession bool,
) error {
	if err := s.ValidateReservationCleanup(ctx, session, unusedStoragePolicy); err != nil {
		return err
	}

	policy := session.Spec.UnusedStoragePolicy
	if unusedStoragePolicy != "" {
		policy = v1alpha1.UnusedStoragePolicy(unusedStoragePolicy)
	}

	deleteDestination := domain.DeletesUnusedStorage(policy)

	session.Status.Phase = PhaseCleaning
	session.Status.Message = "cleaning cross-cluster reservation resources"
	s.touchReservation(session)

	if err := s.saveReservation(ctx, session, false); err != nil {
		return err
	}

	for i := range session.Spec.Volumes {
		state := reservationState{
			ID:     session.ID,
			Spec:   &session.Spec.SessionContext,
			Status: &session.Status.Volumes[i].Reservation,
			Save: func(saveCtx context.Context) error {
				return s.saveReservation(saveCtx, session, false)
			},
		}

		var err error
		if deleteDestination {
			err = s.cleanupReservationDestinationVolume(ctx, state, &session.Spec.Volumes[i])
		} else {
			err = s.retainReservationDestinationVolume(ctx, state, &session.Spec.Volumes[i])
		}

		if err != nil {
			return err
		}

		session.Status.Message = fmt.Sprintf(
			"cleaned destination PVC %s/%s",
			session.Spec.Volumes[i].Destination.PVC.Namespace,
			session.Spec.Volumes[i].Destination.PVC.Name,
		)
		s.touchReservation(session)

		if err := s.saveReservation(ctx, session, false); err != nil {
			return err
		}
	}

	session.Status.Phase = PhaseCleaned

	session.Status.Message = "cross-cluster reservation cleanup completed"
	if !deleteDestination {
		session.Status.Message = "cross-cluster reservation cleaned; destination resources were retained"
	}

	s.touchReservation(session)

	if deleteSession {
		return s.deleteSession(ctx, session.Spec.SessionNamespace, session.ID, func() error {
			return s.deleteReservation(ctx, session)
		})
	}

	return s.saveReservation(ctx, session, false)
}

func (s *Service) ValidateReservationCleanup(
	ctx context.Context,
	session *ReservationSession,
	unusedStoragePolicy string,
) error {
	if err := s.validateReservationSession(ctx, session); err != nil {
		return err
	}

	policy := session.Spec.UnusedStoragePolicy
	if unusedStoragePolicy != "" {
		policy = v1alpha1.UnusedStoragePolicy(unusedStoragePolicy)
	}

	if err := domain.ValidateUnusedStoragePolicy(policy); err != nil {
		return err
	}

	deleting := domain.DeletesUnusedStorage(policy)
	for i := range session.Spec.Volumes {
		pvc, err := s.inspectReservationCleanupDestination(
			ctx, session.ID, &session.Spec.Volumes[i], deleting,
		)
		if err != nil {
			return err
		}

		if deleting && pvc != nil {
			state := reservationState{
				ID:     session.ID,
				Spec:   &session.Spec.SessionContext,
				Status: &session.Status.Volumes[i].Reservation,
			}
			if err := s.validateReservationCleanupConsumers(ctx, state, pvc); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Service) validateReservationCleanupConsumers(
	ctx context.Context,
	state reservationState,
	pvc *corev1.PersistentVolumeClaim,
) error {
	pods, err := s.destination.Kubernetes.CoreV1().
		Pods(pvc.Namespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	reserved := state.Status.ConsumerPod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Name == reserved.Name && pod.Namespace == reserved.Namespace &&
			pod.UID == reserved.UID && pod.Labels[SessionKey] == state.ID &&
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

func (s *Service) inspectReservationCleanupDestination(
	ctx context.Context,
	id string,
	volume *VolumeSpec,
	deleting bool,
) (*corev1.PersistentVolumeClaim, error) {
	v := &volume.Destination

	pvc, err := s.destination.Kubernetes.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).
		Get(ctx, v.PVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		pvc = nil
	} else if err != nil {
		return nil, err
	}

	if pvc != nil {
		owned := pvc.Labels[SessionKey] == id && pvc.Labels[ManagedByLabel] == ManagedBy

		released := !deleting && v.PVC.UID == pvc.UID && pvc.Labels[SessionKey] == "" &&
			pvc.Labels[ManagedByLabel] == ""
		if (v.PVC.UID != "" && v.PVC.UID != pvc.UID) || (!owned && !released) {
			return nil, fmt.Errorf(
				"destination PVC %s/%s UID or ownership changed",
				pvc.Namespace,
				pvc.Name,
			)
		}

		if v.PV.Name != "" && pvc.Spec.VolumeName != "" && v.PV.Name != pvc.Spec.VolumeName {
			return nil, fmt.Errorf("destination PVC %s/%s binding changed", pvc.Namespace, pvc.Name)
		}

		v.PVC.UID = pvc.UID
		if v.PV.Name == "" {
			v.PV.Name = pvc.Spec.VolumeName
		}
	}

	if v.PV.Name == "" {
		return pvc, nil
	}

	pv, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumes().
		Get(ctx, v.PV.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return pvc, nil
	}

	if err != nil {
		return nil, err
	}

	claim := pv.Spec.ClaimRef
	if (v.PV.UID != "" && v.PV.UID != pv.UID) || claim == nil || v.PVC.UID == "" ||
		claim.UID != v.PVC.UID || claim.Name != v.PVC.Name || claim.Namespace != v.PVC.Namespace {
		return nil, fmt.Errorf("destination PV %s identity or claim reference changed", pv.Name)
	}

	return pvc, nil
}

func (s *Service) cleanupReservationDestinationVolume(
	ctx context.Context,
	state reservationState,
	volume *VolumeSpec,
) error {
	if err := s.deleteReservationConsumer(ctx, state); err != nil {
		return err
	}

	client := s.destination.Kubernetes

	pvc, err := client.CoreV1().PersistentVolumeClaims(volume.Destination.PVC.Namespace).
		Get(ctx, volume.Destination.PVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return s.cleanupDestinationPV(ctx, volume, "")
	}

	if err != nil {
		return fmt.Errorf(
			"read destination PVC %s/%s: %w",
			volume.Destination.PVC.Namespace,
			volume.Destination.PVC.Name,
			err,
		)
	}

	if volume.Destination.PVC.UID != "" && pvc.UID != volume.Destination.PVC.UID {
		return fmt.Errorf("destination PVC %s/%s UID changed", pvc.Namespace, pvc.Name)
	}

	if pvc.Labels[SessionKey] != state.ID || pvc.Labels[ManagedByLabel] != ManagedBy {
		return fmt.Errorf(
			"destination PVC %s/%s ownership changed; refusing to delete it",
			pvc.Namespace,
			pvc.Name,
		)
	}

	volume.Destination.PVC.UID = pvc.UID

	pvName := pvc.Spec.VolumeName
	if volume.Destination.PV.Name != "" && pvName != "" && volume.Destination.PV.Name != pvName {
		return fmt.Errorf(
			"destination PVC %s/%s is bound to unexpected PV %s",
			pvc.Namespace,
			pvc.Name,
			pvName,
		)
	}

	if err := ensureNoActiveConsumers(ctx, client, pvc.Namespace, pvc.Name); err != nil {
		return err
	}

	uid := pvc.UID

	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	if err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(
		ctx, pvc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
	); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete destination PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}

	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	return s.cleanupDestinationPV(ctx, volume, pvName)
}

func (s *Service) retainReservationDestinationVolume(
	ctx context.Context,
	state reservationState,
	volume *VolumeSpec,
) error {
	if err := s.deleteReservationConsumer(ctx, state); err != nil {
		return err
	}

	pvc, err := s.inspectReservationCleanupDestination(ctx, state.ID, volume, false)
	if err != nil {
		return err
	}

	if pvc != nil {
		volume.Destination.PVC.UID = pvc.UID
	}

	if err := state.Save(ctx); err != nil {
		return err
	}

	var pv *corev1.PersistentVolume
	if volume.Destination.PV.Name != "" {
		pv, err = s.destination.Kubernetes.CoreV1().PersistentVolumes().Get(
			ctx, volume.Destination.PV.Name, metav1.GetOptions{},
		)
		if apierrors.IsNotFound(err) {
			pv = nil
		} else if err != nil {
			return err
		}
	}

	if pv != nil {
		volume.Destination.PV.Name, volume.Destination.PV.UID = pv.Name, pv.UID
		volume.Destination.PV.ClusterID, volume.Destination.PV.Kind = state.Spec.DestinationCluster.ID, "PersistentVolume"
	}

	if err := state.Save(ctx); err != nil {
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

	if pvc == nil {
		return nil
	}

	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	delete(pvc.Labels, SessionKey)
	delete(pvc.Labels, ManagedByLabel)

	_, err = s.destination.Kubernetes.CoreV1().PersistentVolumeClaims(pvc.Namespace).
		Update(ctx, pvc, metav1.UpdateOptions{})
	if err == nil {
		err = requireSessionLease(ctx)
	}

	return err
}
