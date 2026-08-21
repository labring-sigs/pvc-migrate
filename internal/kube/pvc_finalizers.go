package kube

import (
	"fmt"
	"slices"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
)

// BlockingPVCFinalizers returns finalizers that Kubernetes cannot resolve
// through its built-in PVC protection controller.
func BlockingPVCFinalizers(pvc *corev1.PersistentVolumeClaim) []string {
	if pvc == nil {
		return nil
	}

	blocking := make([]string, 0, len(pvc.Finalizers))
	for _, finalizer := range pvc.Finalizers {
		if finalizer == "" || finalizer == PVCProtectionFinalizer {
			continue
		}

		blocking = append(blocking, finalizer)
	}

	slices.Sort(blocking)

	return slices.Compact(blocking)
}

func validatePVCDeletionFinalizers(pvc *corev1.PersistentVolumeClaim, operation string) error {
	blocking := BlockingPVCFinalizers(pvc)
	if len(blocking) == 0 {
		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		operation,
		fmt.Sprintf(
			"PVC %s/%s has custom finalizer(s) %s; resolve them with their owning controller or explicitly remove them before retrying",
			pvc.Namespace,
			pvc.Name,
			strings.Join(blocking, ", "),
		),
	)
}
