package app

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func openEBSLVMSource(
	ctx context.Context,
	client kubernetes.Interface,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
) (bool, error) {
	if client == nil {
		return false, nil
	}

	if sourcePVC.Namespace == "" || sourcePVC.Name == "" ||
		sourcePV.Name == "" {
		return false, domain.NewError(
			domain.ErrorValidation,
			"OpenEBS LVM shared mount",
			"source PVC and PV identities are required",
		)
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(sourcePVC.Namespace).
		Get(ctx, sourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("read source PVC %s/%s", sourcePVC.Namespace, sourcePVC.Name),
			err,
		)
	}

	if pvc.UID != sourcePVC.UID || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != sourcePV.Name {
		return false, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf(
				"source PVC %s/%s identity or binding changed",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
		)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(ctx, sourcePV.Name, metav1.GetOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"read source PV "+sourcePV.Name,
			err,
		)
	}

	if pv.UID != sourcePV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("source PV %s identity or claimRef changed", sourcePV.Name),
		)
	}

	return pv.Spec.CSI != nil && pv.Spec.CSI.Driver == kube.OpenEBSLVMCSIDriver, nil
}
