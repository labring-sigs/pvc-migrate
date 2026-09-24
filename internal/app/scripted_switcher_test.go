package app

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// scriptedSwitcher scripts cutover against a fake cluster: it records calls,
// applies the durable PVC mutations activation and rollback perform, and
// reports them through the checkpoint progress callback.
type scriptedSwitcher struct {
	client        kubernetes.Interface
	activateCalls []string
	rollbackCalls []string
}

func (s *scriptedSwitcher) VerifyActivationRecovery(
	context.Context,
	string,
	[]kube.PVCTransferBindings,
) error {
	return nil
}

func (s *scriptedSwitcher) VerifyVolumeOffline(
	context.Context,
	kube.PVCTransferBindings,
) error {
	return nil
}

func (s *scriptedSwitcher) VerifyVolumesOfflineForSession(
	context.Context,
	string,
	[]kube.PVCTransferBindings,
) error {
	return nil
}

func (s *scriptedSwitcher) ActivatePVC(
	ctx context.Context,
	workflowID string,
	activateNamespace string,
	volume kube.PVCTransferBindings,
	_ *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
	progress kube.ProgressFunc,
) error {
	s.activateCalls = append(s.activateCalls, volume.SourcePVC.Name)

	if s.client != nil {
		if err := s.client.CoreV1().PersistentVolumeClaims(volume.SourcePVC.Namespace).Delete(
			ctx, volume.DestinationPVC.Name, metav1.DeleteOptions{},
		); clientIgnoreNotFound(err) != nil {
			return clientIgnoreNotFound(err)
		}

		active := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: activateNamespace,
				Name:      volume.SourcePVC.Name,
				UID:       volume.SourcePVC.UID,
				Labels: map[string]string{
					kube.ManagedByLabel: kube.ManagedByValue,
					kube.SessionKey:     workflowID,
				},
				Annotations: map[string]string{
					kube.SessionKey: workflowID,
				},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: volume.DestinationPV.Name},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}

		created, err := s.client.CoreV1().PersistentVolumeClaims(active.Namespace).Create(
			ctx, active, metav1.CreateOptions{},
		)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return err
			}

			if created, err = s.client.CoreV1().PersistentVolumeClaims(active.Namespace).Update(
				ctx, active, metav1.UpdateOptions{},
			); err != nil {
				return err
			}
		}

		pv, err := s.client.CoreV1().PersistentVolumes().Get(
			ctx, volume.DestinationPV.Name, metav1.GetOptions{},
		)
		if apierrors.IsNotFound(err) {
			pv = &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: volume.DestinationPV.Name,
					UID:  volume.DestinationPV.UID,
					Labels: map[string]string{
						kube.ManagedByLabel: kube.ManagedByValue,
						kube.SessionKey:     workflowID,
					},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				},
			}
			if pv, err = s.client.CoreV1().PersistentVolumes().Create(
				ctx, pv, metav1.CreateOptions{},
			); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
		} else if err != nil {
			return err
		}

		pv.Spec.ClaimRef = &corev1.ObjectReference{
			Kind:            "PersistentVolumeClaim",
			Namespace:       created.Namespace,
			Name:            created.Name,
			UID:             created.UID,
			ResourceVersion: created.ResourceVersion,
		}
		if pv.Labels == nil {
			pv.Labels = map[string]string{}
		}

		pv.Labels[kube.ManagedByLabel] = kube.ManagedByValue

		pv.Labels[kube.SessionKey] = workflowID
		if _, err := s.client.CoreV1().PersistentVolumes().Update(
			ctx, pv, metav1.UpdateOptions{},
		); err != nil {
			return err
		}
	}

	if status != nil {
		// Activation renames the reserved claim into the source identity,
		// landing in the activation namespace (the source namespace unless the
		// workflow migrates across namespaces).
		active := volume.SourcePVC
		active.Namespace = activateNamespace
		now := metav1.Now()
		status.ActivePVC = &active
		status.ActivatedAt = &now
	}

	// The real switcher persists its checkpoint through the progress callback.
	if progress != nil {
		if err := progress(); err != nil {
			return err
		}
	}

	return nil
}

func (s *scriptedSwitcher) RollbackPVC(
	ctx context.Context,
	workflowID string,
	activateNamespace string,
	volume kube.PVCTransferBindings,
	_ *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
	progress kube.ProgressFunc,
) error {
	s.rollbackCalls = append(s.rollbackCalls, volume.SourcePVC.Name)

	if s.client != nil {
		restored := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: volume.SourcePVC.Namespace,
				Name:      volume.SourcePVC.Name,
				UID:       volume.SourcePVC.UID,
				Labels: map[string]string{
					kube.ManagedByLabel: kube.ManagedByValue,
					kube.SessionKey:     workflowID,
				},
				Annotations: map[string]string{
					kube.SessionKey: workflowID,
				},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: volume.SourcePV.Name},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}

		created, err := s.client.CoreV1().PersistentVolumeClaims(restored.Namespace).Create(
			ctx, restored, metav1.CreateOptions{},
		)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return err
			}

			if created, err = s.client.CoreV1().PersistentVolumeClaims(restored.Namespace).Update(
				ctx, restored, metav1.UpdateOptions{},
			); err != nil {
				return err
			}
		}

		pv, err := s.client.CoreV1().PersistentVolumes().Get(
			ctx, volume.SourcePV.Name, metav1.GetOptions{},
		)
		if apierrors.IsNotFound(err) {
			pv = &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: volume.SourcePV.Name,
					UID:  volume.SourcePV.UID,
					Labels: map[string]string{
						kube.ManagedByLabel: kube.ManagedByValue,
						kube.SessionKey:     workflowID,
					},
				},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				},
			}
			if pv, err = s.client.CoreV1().PersistentVolumes().Create(
				ctx, pv, metav1.CreateOptions{},
			); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
		} else if err != nil {
			return err
		}

		pv.Spec.ClaimRef = &corev1.ObjectReference{
			Kind:            "PersistentVolumeClaim",
			Namespace:       created.Namespace,
			Name:            created.Name,
			UID:             created.UID,
			ResourceVersion: created.ResourceVersion,
		}
		if _, err := s.client.CoreV1().PersistentVolumes().Update(
			ctx, pv, metav1.UpdateOptions{},
		); err != nil {
			return err
		}
	}

	if status != nil {
		// Rollback restores the source claim identity and records it as the
		// last active identity alongside the rollback checkpoint.
		restored := volume.SourcePVC
		now := metav1.Now()
		status.ActivePVC = &restored
		status.RolledBackAt = &now
	}

	if progress != nil {
		if err := progress(); err != nil {
			return err
		}
	}

	return nil
}

// fakeSwitcher accepts every cutover without cluster access.
type fakeSwitcher struct{}

func (fakeSwitcher) VerifyActivationRecovery(
	context.Context,
	string,
	[]kube.PVCTransferBindings,
) error {
	return nil
}

func (fakeSwitcher) VerifyVolumeOffline(context.Context, kube.PVCTransferBindings) error {
	return nil
}

func (fakeSwitcher) VerifyVolumesOfflineForSession(
	context.Context,
	string,
	[]kube.PVCTransferBindings,
) error {
	return nil
}

func (fakeSwitcher) ActivatePVC(
	context.Context,
	string,
	string,
	kube.PVCTransferBindings,
	*corev1.PersistentVolumeClaim,
	*v1alpha1.ClusterVolumeActivationStatus,
	kube.ProgressFunc,
) error {
	return nil
}

func (fakeSwitcher) RollbackPVC(
	context.Context,
	string,
	string,
	kube.PVCTransferBindings,
	*corev1.PersistentVolumeClaim,
	*v1alpha1.ClusterVolumeActivationStatus,
	kube.ProgressFunc,
) error {
	return nil
}

func clientIgnoreNotFound(err error) error {
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
