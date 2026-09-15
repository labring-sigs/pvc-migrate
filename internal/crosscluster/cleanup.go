package crosscluster

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) Cleanup(
	ctx context.Context,
	session *Session,
	destinationPolicy string,
	deleteSession bool,
) error {
	if s.locker != nil {
		return s.withLock(ctx, session, func(locked context.Context) error {
			return s.cleanup(locked, session, destinationPolicy, deleteSession)
		})
	}

	return s.cleanup(ctx, session, destinationPolicy, deleteSession)
}

func (s *Service) cleanup(
	ctx context.Context,
	session *Session,
	destinationPolicy string,
	deleteSession bool,
) error {
	if err := s.ValidateCleanup(ctx, session, destinationPolicy); err != nil {
		return err
	}

	if destinationPolicy == "" {
		destinationPolicy = session.Spec.DestinationPVCReclaimPolicy
	}

	deleteDestination := destinationPolicy == "Delete"

	session.Status.Phase = PhaseCleaning
	session.Status.Message = "cleaning cross-cluster resources"
	s.touch(session)

	if err := s.save(ctx, session, false); err != nil {
		return err
	}

	if deleteDestination {
		for i := range session.Spec.Volumes {
			if err := s.cleanupDestinationVolume(ctx, session, i); err != nil {
				return err
			}

			session.Status.Message = fmt.Sprintf(
				"cleaned destination PVC %s/%s",
				session.Spec.Volumes[i].Destination.PVC.Namespace,
				session.Spec.Volumes[i].Destination.PVC.Name,
			)
			s.touch(session)

			if err := s.save(ctx, session, false); err != nil {
				return err
			}
		}
	} else {
		for i := range session.Spec.Volumes {
			if err := s.retainDestinationVolume(ctx, session, i); err != nil {
				return err
			}
		}
	}

	session.Status.Phase = PhaseCleaned

	session.Status.Message = "cross-cluster cleanup completed"
	if !deleteDestination {
		session.Status.Message = "cross-cluster session cleaned; destination resources were retained"
	}

	s.touch(session)

	if deleteSession {
		if err := s.delete(ctx, session); err != nil {
			return err
		}

		return s.leases.DeleteSessionLease(ctx, session.Spec.SessionNamespace, session.ID)
	}

	return s.save(ctx, session, false)
}

// ValidateCleanup checks policy, identities and consumers without changing the session.
func (s *Service) ValidateCleanup(ctx context.Context, session *Session, policy string) error {
	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if policy == "" {
		policy = session.Spec.DestinationPVCReclaimPolicy
	}

	if err := domain.ValidateReclaimPolicies("", policy); err != nil {
		return err
	}

	for i := range session.Spec.Volumes {
		pvc, _, err := s.inspectCleanupDestination(ctx, session, i, policy == "Delete")
		if err != nil {
			return err
		}

		if policy == "Delete" && pvc != nil {
			if err := s.validateCleanupConsumers(ctx, session, i, pvc); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Service) cleanupDestinationVolume(ctx context.Context, session *Session, index int) error {
	volume := &session.Spec.Volumes[index]

	client := s.destination.Kubernetes
	if err := s.deleteReservationConsumer(ctx, session, index); err != nil {
		return err
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(volume.Destination.PVC.Namespace).
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

	if pvc.Labels[SessionKey] != session.ID || pvc.Labels[ManagedByLabel] != ManagedBy {
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
	if err := client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Delete(ctx, pvc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil &&
		!apierrors.IsNotFound(err) {
		return fmt.Errorf("delete destination PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}

	return s.cleanupDestinationPV(ctx, volume, pvName)
}

func (s *Service) cleanupDestinationPV(
	ctx context.Context,
	volume *VolumeSpec,
	pvName string,
) error {
	if pvName == "" {
		pvName = volume.Destination.PV.Name
	}

	if pvName == "" {
		return nil
	}

	client := s.destination.Kubernetes.CoreV1().PersistentVolumes()

	return kube.WaitFor(
		ctx,
		s.interval,
		"destination PV "+pvName+" release",
		func(waitCtx context.Context) (bool, error) {
			pv, err := client.Get(waitCtx, pvName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			if err != nil {
				return false, err
			}

			if volume.Destination.PV.UID != "" && pv.UID != volume.Destination.PV.UID {
				return false, fmt.Errorf("destination PV %s UID changed; refusing cleanup", pvName)
			}

			if pv.Spec.ClaimRef != nil &&
				(pv.Spec.ClaimRef.Namespace != volume.Destination.PVC.Namespace || pv.Spec.ClaimRef.Name != volume.Destination.PVC.Name || volume.Destination.PVC.UID == "" || pv.Spec.ClaimRef.UID != volume.Destination.PVC.UID) {
				return false, fmt.Errorf(
					"destination PV %s claim reference changed; refusing cleanup",
					pvName,
				)
			}

			if pv.Status.Phase == corev1.VolumeFailed {
				return false, fmt.Errorf(
					"destination PV %s is Failed; inspect the provisioner before deleting it",
					pvName,
				)
			}

			if pv.Status.Phase != corev1.VolumeReleased &&
				pv.Status.Phase != corev1.VolumeAvailable {
				return false, nil
			}

			uid := pv.UID

			preconditions := &metav1.Preconditions{UID: &uid}
			if pv.ResourceVersion != "" {
				resourceVersion := pv.ResourceVersion
				preconditions.ResourceVersion = &resourceVersion
			}

			if err := client.Delete(
				waitCtx,
				pv.Name,
				metav1.DeleteOptions{Preconditions: preconditions},
			); err != nil &&
				!apierrors.IsNotFound(err) {
				return false, err
			}

			return true, nil
		},
	)
}

func (s *Service) deleteReservationConsumer(
	ctx context.Context,
	session *Session,
	index int,
) error {
	ref := session.Status.Volumes[index].Reservation.ConsumerPod
	if ref.Name == "" {
		return nil
	}

	pods := s.destination.Kubernetes.CoreV1().Pods(ref.Namespace)

	pod, err := pods.Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if (ref.UID != "" && pod.UID != ref.UID) || pod.Labels[SessionKey] != session.ID ||
		pod.Labels[ManagedByLabel] != ManagedBy {
		return fmt.Errorf("reservation Pod %s/%s ownership or UID changed", pod.Namespace, pod.Name)
	}

	uid := pod.UID
	if err := pods.Delete(
		ctx,
		pod.Name,
		metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
	); err != nil &&
		!apierrors.IsNotFound(err) {
		return err
	}

	return kube.WaitFor(
		ctx,
		s.interval,
		"reservation Pod "+pod.Namespace+"/"+pod.Name+" deletion",
		func(waitCtx context.Context) (bool, error) {
			current, err := pods.Get(waitCtx, pod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			if err != nil {
				return false, err
			}

			if current.UID != pod.UID {
				return false, fmt.Errorf(
					"reservation Pod %s/%s UID changed during deletion",
					pod.Namespace,
					pod.Name,
				)
			}

			return false, nil
		},
	)
}
