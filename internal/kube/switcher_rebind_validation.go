package kube

import (
	"context"
	"fmt"
	"maps"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VerifyPVCSourceSnapshot protects the original claim template before the first
// rebind. Once rebinding starts, recovery follows persisted resource identities.
func (s *Switcher) VerifyPVCSourceSnapshot(
	ctx context.Context,
	claim, volume v1alpha1.ObjectReference,
	template v1alpha1.PVCSourceTemplate,
) error {
	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(claim.Namespace).
		Get(ctx, claim.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify source snapshot",
			"read source PVC",
			err,
		)
	}

	if pvc.UID != claim.UID || !apiequality.Semantic.DeepEqual(pvc.Spec, template.Spec) {
		return domain.NewError(
			domain.ErrorConflict,
			"verify source snapshot",
			"source PVC identity or spec changed since planning",
		)
	}

	if !maps.Equal(pvc.Labels, template.Metadata.Labels) ||
		!maps.Equal(PVCAnnotationsForRecreation(pvc.Annotations), template.Metadata.Annotations) ||
		!apiequality.Semantic.DeepEqual(pvc.OwnerReferences, template.Metadata.OwnerReferences) {
		return domain.NewError(
			domain.ErrorConflict,
			"verify source snapshot",
			"source PVC metadata changed since planning",
		)
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, volume.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify source snapshot",
			"read source PV",
			err,
		)
	}

	if pv.UID != volume.UID || pv.Spec.PersistentVolumeReclaimPolicy != template.ReclaimPolicy {
		return domain.NewError(
			domain.ErrorConflict,
			"verify source snapshot",
			"source PV identity or reclaim policy changed since planning",
		)
	}

	return nil
}

// VerifyPVCRebind validates an offline identity change. Recovery is permitted
// only after the caller has persisted its rebind stage.
func (s *Switcher) VerifyPVCRebind(
	ctx context.Context,
	id string,
	from, to, pv v1alpha1.ObjectReference,
	recovering bool,
) error {
	if from.Namespace == "" || from.Name == "" || from.UID == "" ||
		to.Namespace == "" || to.Name == "" || pv.Name == "" || pv.UID == "" ||
		(from.Namespace == to.Namespace && from.Name == to.Name) {
		return domain.NewError(
			domain.ErrorValidation,
			"rebind PVC",
			"distinct PVC endpoints and source PVC/PV identities are required",
		)
	}

	source, err := s.client.CoreV1().
		PersistentVolumeClaims(from.Namespace).
		Get(ctx, from.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && recovering {
		return s.VerifyPVCRebindRecovery(ctx, id, from, to, pv)
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rebind PVC",
			fmt.Sprintf("read source PVC %s/%s", from.Namespace, from.Name),
			err,
		)
	}

	if source.UID != from.UID {
		return domain.NewError(domain.ErrorConflict, "rebind PVC", "source PVC UID changed")
	}

	_, err = s.client.CoreV1().
		PersistentVolumeClaims(to.Namespace).
		Get(ctx, to.Name, metav1.GetOptions{})
	if err == nil {
		return domain.NewError(domain.ErrorConflict, "rebind PVC", "both PVC endpoints exist")
	}

	if !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rebind PVC",
			fmt.Sprintf("read destination PVC %s/%s", to.Namespace, to.Name),
			err,
		)
	}

	return s.VerifyPVCOffline(ctx, id, from, pv)
}
