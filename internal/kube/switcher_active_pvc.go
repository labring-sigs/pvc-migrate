package kube

import (
	"context"
	"errors"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Switcher) activePVC(
	ctx context.Context,
	sessionID string,
	sourcePVC, sourcePV, destinationPV v1alpha1.ObjectReference,
) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(sourcePVC.Namespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
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

	if pvc.UID == sourcePVC.UID && pvc.Spec.VolumeName == sourcePV.Name {
		if err := s.verifyBinding(ctx, pvc, sourcePV); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if pvc.Spec.VolumeName == destinationPV.Name &&
		pvc.Annotations[SessionKey] == sessionID {
		return pvc, nil
	}

	return nil, domain.NewError(
		domain.ErrorConflict,
		"activate volume",
		fmt.Sprintf("PVC %s/%s has unexpected UID or binding", pvc.Namespace, pvc.Name),
	)
}

func (s *Switcher) createBoundPVC(
	ctx context.Context,
	sessionID string,
	pvc *corev1.PersistentVolumeClaim,
) (*corev1.PersistentVolumeClaim, error) {
	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
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

	if created.Spec.VolumeName != pvc.Spec.VolumeName ||
		created.Labels[ManagedByLabel] != ManagedByValue ||
		created.Labels[SessionKey] != sessionID ||
		created.Annotations[SessionKey] != sessionID {
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
		fmt.Sprintf(
			"PVC %s/%s binding to PV %s",
			created.Namespace,
			created.Name,
			pvc.Spec.VolumeName,
		),
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
				current.Labels[SessionKey] != sessionID ||
				current.Annotations[SessionKey] != sessionID {
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

			if current.Spec.VolumeName != pvc.Spec.VolumeName {
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

func (s *Switcher) validateBoundPVC(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) error {
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

// BoundPVCManifest recreates a claim from its recorded specification and metadata.
func BoundPVCManifest(
	sessionID string,
	claim v1alpha1.ObjectReference,
	pvName string,
	sourceSpec corev1.PersistentVolumeClaimSpec,
	metadata v1alpha1.PVCMetadata,
) *corev1.PersistentVolumeClaim {
	spec := *sourceSpec.DeepCopy()
	spec.VolumeName = pvName
	spec.Selector = nil
	spec.DataSource = nil

	spec.DataSourceRef = nil
	if spec.StorageClassName == nil {
		storageClass := ""
		spec.StorageClassName = &storageClass
	}

	metadata = *metadata.DeepCopy()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            claim.Name,
			Namespace:       claim.Namespace,
			Labels:          metadata.Labels,
			Annotations:     PVCAnnotationsForRecreation(metadata.Annotations),
			OwnerReferences: metadata.OwnerReferences,
		},
		Spec: spec,
	}
	if pvc.Labels == nil {
		pvc.Labels = map[string]string{}
	}

	pvc.Labels[ManagedByLabel] = ManagedByValue
	pvc.Labels[SessionKey] = sessionID
	pvc.Annotations[SessionKey] = sessionID

	return pvc
}

func (s *Switcher) completeActivation(
	ctx context.Context,
	sessionID string,
	volume PVCTransferBindings,
	desired *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
	pvc *corev1.PersistentVolumeClaim,
	progress ProgressFunc,
) error {
	capacity := desired.Spec.Resources.Requests[corev1.ResourceStorage]
	if err := validateActivePVCRequest(pvc, capacity.String()); err != nil {
		return err
	}

	if err := s.verifyBinding(ctx, pvc, volume.DestinationPV); err != nil {
		return err
	}

	if err := s.markPVPair(
		ctx,
		sessionID,
		volume.SourcePV,
		volume.DestinationPV,
		false,
	); err != nil {
		return err
	}

	now := metav1.NewTime(s.now().UTC())
	before := status.DeepCopy()
	status.ActivePVC = &v1alpha1.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
	status.ActivatedAt = &now
	status.TemporaryPVCDeleted = true
	status.SourcePVCDeleted = true
	status.DestinationReserved = true

	return saveActivationCheckpoint(ctx, status, before, progress)
}

func validateActivePVCRequest(pvc *corev1.PersistentVolumeClaim, requiredCapacity string) error {
	if pvc == nil {
		return domain.NewError(domain.ErrorValidation, "active PVC", "PVC is required")
	}

	capacity, err := resource.ParseQuantity(requiredCapacity)
	if err != nil || capacity.Sign() <= 0 {
		if err == nil {
			err = errors.New("capacity must be positive")
		}

		return domain.NewError(
			domain.ErrorValidation,
			"active PVC",
			fmt.Sprintf("destination capacity %q is invalid: %v", requiredCapacity, err),
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
	sessionID string,
	volume PVCTransferBindings,
	status *v1alpha1.ClusterVolumeActivationStatus,
	pvc *corev1.PersistentVolumeClaim,
	progress ProgressFunc,
) error {
	if err := s.verifyBinding(ctx, pvc, volume.SourcePV); err != nil {
		return err
	}

	if err := s.markPVPair(
		ctx,
		sessionID,
		volume.SourcePV,
		volume.DestinationPV,
		true,
	); err != nil {
		return err
	}

	before := status.DeepCopy()
	now := metav1.NewTime(s.now().UTC())
	status.ActivePVC = &v1alpha1.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
	status.RolledBackAt = &now

	return saveActivationCheckpoint(ctx, status, before, progress)
}
