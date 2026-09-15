package backup

import (
	"errors"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	restoreBucketAnnotation = kube.MetadataDomain + "/restore-bucket"
	restorePrefixAnnotation = kube.MetadataDomain + "/restore-prefix"
	restoreNameAnnotation   = kube.MetadataDomain + "/restore-name"
)

// A submission can defer repository access to the controller. The requested
// capacity remains a lower bound until the recovery point has been verified.
func deferredRestoreCapacity(requested string) (resource.Quantity, error) {
	if strings.TrimSpace(requested) == "" {
		return resource.Quantity{}, nil
	}

	capacity, err := resource.ParseQuantity(requested)
	if err != nil || capacity.Sign() <= 0 {
		if err == nil {
			err = errors.New("capacity must be greater than zero")
		}

		return resource.Quantity{}, domain.WrapError(
			domain.ErrorValidation, restorePreflightPhase, "parse --destination-capacity", err,
		)
	}

	return capacity, nil
}

func selectRestoreToolNode(targetNode, consumerNode, pvNode string) (string, error) {
	if targetNode == "" {
		if consumerNode != "" {
			return consumerNode, nil
		}
		return pvNode, nil
	}

	for _, required := range []struct {
		requirement string
		node        string
	}{
		{requirement: "mounted RWO consumer", node: consumerNode},
		{requirement: "PV topology", node: pvNode},
	} {
		if required.node != "" && required.node != targetNode {
			return "", domain.NewError(
				domain.ErrorConflict,
				restoreSchedulingPhase,
				fmt.Sprintf(
					"destination PVC %s requires node %s, but target node %s was requested",
					required.requirement,
					required.node,
					targetNode,
				),
			)
		}
	}

	return targetNode, nil
}

func restoreDestinationCapacity(
	manifest objectstore.Manifest,
	requested string,
) (resource.Quantity, error) {
	backupCapacity, err := resource.ParseQuantity(manifest.Capacity)
	if err != nil {
		return resource.Quantity{}, domain.WrapError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
			"parse backup capacity",
			err,
		)
	}

	if backupCapacity.Sign() <= 0 {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
			"backup capacity must be positive",
		)
	}

	if requested == "" {
		return backupCapacity, nil
	}

	capacity, err := resource.ParseQuantity(requested)
	if err != nil {
		return resource.Quantity{}, domain.WrapError(
			domain.ErrorValidation,
			restorePreflightPhase,
			"parse --destination-capacity",
			err,
		)
	}

	if capacity.Sign() <= 0 {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorValidation,
			restorePreflightPhase,
			"--destination-capacity must be positive",
		)
	}

	if capacity.Cmp(backupCapacity) < 0 {
		return resource.Quantity{}, domain.NewError(
			domain.ErrorPrecondition,
			restorePreflightPhase,
			fmt.Sprintf(
				"destination PVC capacity %s is below backup capacity %s",
				capacity.String(),
				backupCapacity.String(),
			),
		)
	}

	return capacity, nil
}

func parseRestoreAccessMode(value string) (corev1.PersistentVolumeAccessMode, error) {
	mode := corev1.PersistentVolumeAccessMode(value)
	switch mode {
	case corev1.ReadWriteOnce, corev1.ReadWriteOncePod, corev1.ReadWriteMany:
		return mode, nil
	default:
		return "", domain.NewError(
			domain.ErrorValidation,
			restorePreflightPhase,
			fmt.Sprintf(
				"unsupported --destination-access-mode %q; use ReadWriteOnce, ReadWriteOncePod, or ReadWriteMany",
				value,
			),
		)
	}
}
