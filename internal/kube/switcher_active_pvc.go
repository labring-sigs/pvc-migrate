package kube

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Switcher) activePVC(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"activate volume",
			"read application PVC",
			err,
		)
	}

	if pvc.UID == volume.SourcePVC.UID && pvc.Spec.VolumeName == volume.SourcePV.Name {
		if err := s.verifyBinding(ctx, pvc, volume.SourcePV); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if pvc.Spec.VolumeName == volume.DestinationPV.Name &&
		pvc.Annotations[SessionKey] == session.ID {
		return pvc, nil
	}

	return nil, domain.NewError(
		domain.ErrorConflict,
		"activate volume",
		fmt.Sprintf("PVC %s/%s has unexpected UID or binding", pvc.Namespace, pvc.Name),
	)
}

func (s *Switcher) createActivePVC(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	pvRef domain.ObjectReference,
	storageClass string,
) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := activePVCManifest(session, volume, pvRef, storageClass)
	if err != nil {
		return nil, err
	}

	created, err := s.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Create(ctx, pvc, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = s.client.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Get(ctx, pvc.Name, metav1.GetOptions{})
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"create active PVC",
			fmt.Sprintf("create %s/%s", pvc.Namespace, pvc.Name),
			err,
		)
	}

	if created == nil || created.UID == "" {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"create active PVC",
			fmt.Sprintf("create %s/%s returned an object without a UID", pvc.Namespace, pvc.Name),
		)
	}

	if created.Spec.VolumeName != pvRef.Name || created.Labels[ManagedByLabel] != ManagedByValue ||
		created.Labels[SessionKey] != session.ID ||
		created.Annotations[SessionKey] != session.ID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"create active PVC",
			fmt.Sprintf(
				"PVC %s/%s exists with an unexpected binding",
				created.Namespace,
				created.Name,
			),
		)
	}

	bound := created
	if err := s.waitFor(
		ctx,
		fmt.Sprintf("PVC %s/%s binding to PV %s", created.Namespace, created.Name, pvRef.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := s.client.CoreV1().
				PersistentVolumeClaims(created.Namespace).
				Get(waitCtx, created.Name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}

			if current.UID != created.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"create active PVC",
					fmt.Sprintf(
						"PVC %s/%s was replaced while waiting for binding",
						current.Namespace,
						current.Name,
					),
				)
			}

			if current.Labels[ManagedByLabel] != ManagedByValue ||
				current.Labels[SessionKey] != session.ID ||
				current.Annotations[SessionKey] != session.ID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"create active PVC",
					fmt.Sprintf(
						"PVC %s/%s ownership changed while waiting for binding",
						current.Namespace,
						current.Name,
					),
				)
			}

			if current.Spec.VolumeName != pvRef.Name {
				return false, domain.NewError(
					domain.ErrorConflict,
					"create active PVC",
					"PVC bound to PV "+current.Spec.VolumeName,
				)
			}

			bound = current

			return current.Status.Phase == corev1.ClaimBound, nil
		},
	); err != nil {
		return nil, err
	}

	return bound, nil
}

func (s *Switcher) validateActivePVC(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	pvRef domain.ObjectReference,
	storageClass string,
) error {
	pvc, err := activePVCManifest(session, volume, pvRef, storageClass)
	if err != nil {
		return err
	}

	if _, err := s.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Create(ctx, pvc, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"validate active PVC",
			fmt.Sprintf("server-side dry-run rejected %s/%s", pvc.Namespace, pvc.Name),
			err,
		)
	}

	return nil
}

func activePVCManifest(
	session *domain.Session,
	volume *domain.VolumeSpec,
	pvRef domain.ObjectReference,
	storageClass string,
) (*corev1.PersistentVolumeClaim, error) {
	spec := *volume.SourcePVCSpec.DeepCopy()
	if pvRef.Name == volume.DestinationPV.Name {
		capacity, err := resource.ParseQuantity(volume.Capacity)
		if err != nil || capacity.Sign() <= 0 {
			if err == nil {
				err = errors.New("capacity must be positive")
			}

			return nil, domain.NewError(
				domain.ErrorValidation,
				"active PVC",
				fmt.Sprintf("destination capacity %q is invalid: %v", volume.Capacity, err),
			)
		}

		if spec.Resources.Requests == nil {
			spec.Resources.Requests = corev1.ResourceList{}
		}

		spec.Resources.Requests[corev1.ResourceStorage] = capacity
	}

	spec.VolumeName = pvRef.Name
	spec.Selector = nil
	spec.DataSource = nil
	spec.DataSourceRef = nil
	spec.StorageClassName = &storageClass
	metadata := volume.SourcePVCMetadata

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            volume.SourcePVC.Name,
			Namespace:       volume.SourcePVC.Namespace,
			Labels:          maps.Clone(metadata.Labels),
			Annotations:     PVCAnnotationsForRecreation(metadata.Annotations),
			OwnerReferences: append([]metav1.OwnerReference(nil), metadata.OwnerReferences...),
		},
		Spec: spec,
	}
	if pvc.Labels == nil {
		pvc.Labels = map[string]string{}
	}

	pvc.Labels[ManagedByLabel] = ManagedByValue
	pvc.Labels[SessionKey] = session.ID
	pvc.Annotations[SessionKey] = session.ID

	rollbackPV := volume.SourcePV.Name
	if pvRef.Name == volume.SourcePV.Name {
		rollbackPV = volume.DestinationPV.Name
	}

	pvc.Annotations[RollbackPVAnnotation] = rollbackPV

	return pvc, nil
}

func (s *Switcher) completeActivation(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	pvc *corev1.PersistentVolumeClaim,
	progress ProgressFunc,
) error {
	if err := validateActivePVCRequest(pvc, volume); err != nil {
		return err
	}

	if err := s.verifyBinding(ctx, pvc, volume.DestinationPV); err != nil {
		return err
	}

	if err := s.markPVPair(ctx, session.ID, volume, false); err != nil {
		return err
	}

	now := metav1.NewTime(s.now().UTC())
	status.Activation.ActivePVC = domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
	status.Activation.ActivatedAt = &now
	status.Activation.TemporaryPVCDeleted = true
	status.Activation.SourcePVCDeleted = true
	status.Activation.DestinationReserved = true

	return callProgress(progress)
}

func validateActivePVCRequest(pvc *corev1.PersistentVolumeClaim, volume *domain.VolumeSpec) error {
	if pvc == nil || volume == nil {
		return domain.NewError(domain.ErrorValidation, "active PVC", "PVC and volume are required")
	}

	capacity, err := resource.ParseQuantity(volume.Capacity)
	if err != nil || capacity.Sign() <= 0 {
		if err == nil {
			err = errors.New("capacity must be positive")
		}

		return domain.NewError(
			domain.ErrorValidation,
			"active PVC",
			fmt.Sprintf("destination capacity %q is invalid: %v", volume.Capacity, err),
		)
	}

	requested, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok || requested.Cmp(capacity) != 0 {
		actual := "missing"
		if ok {
			actual = requested.String()
		}

		return domain.NewError(
			domain.ErrorConflict,
			"active PVC",
			fmt.Sprintf(
				"PVC %s/%s requests %s, session requires %s",
				pvc.Namespace,
				pvc.Name,
				actual,
				capacity.String(),
			),
		)
	}

	return nil
}

func (s *Switcher) completeRollback(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	pvc *corev1.PersistentVolumeClaim,
	progress ProgressFunc,
) error {
	if err := s.verifyBinding(ctx, pvc, volume.SourcePV); err != nil {
		return err
	}

	if err := s.markPVPair(ctx, session.ID, volume, true); err != nil {
		return err
	}

	now := metav1.NewTime(s.now().UTC())
	status.Activation.ActivePVC = domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
	status.Activation.RolledBackAt = &now

	return callProgress(progress)
}
