package kube

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/apimachinery/pkg/api/resource"
)

// VerifyVolumeShrinkUsage checks whether trusted whole-volume usage fits the
// destination. A selected path cannot be proven safe by an oversized whole-volume
// measurement; skipping this check requires the caller's explicit approval.
func VerifyVolumeShrinkUsage(
	ctx context.Context,
	reader VolumeUsageReader,
	logger *slog.Logger,
	sourcePVC, sourcePV v1alpha1.ObjectReference,
	sourceCapacity, destinationCapacity, sourcePath string,
	skip bool,
) error {
	source, sourceErr := resource.ParseQuantity(sourceCapacity)

	destination, destinationErr := resource.ParseQuantity(destinationCapacity)
	if sourceErr != nil || destinationErr != nil || destination.Cmp(source) >= 0 {
		return nil
	}

	if skip {
		if logger != nil {
			logger.Warn(
				"source usage check skipped by explicit approval",
				"pvc",
				sourcePVC.Name,
				"destinationCapacity",
				destination.String(),
			)
		}

		return nil
	}

	if reader == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			domain.ErrorOperationSourceUsageCheck,
			fmt.Sprintf(
				"PVC %s/%s has no trusted storage-backend CRD usage reader; pass --skip-source-usage-check only after independently verifying that its data fits destination capacity %s",
				sourcePVC.Namespace,
				sourcePVC.Name,
				destination.String(),
			),
		)
	}

	usage, err := reader.Read(
		ctx,
		VolumeUsageReadOptions{SourcePVC: sourcePVC, SourcePV: sourcePV},
	)
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			domain.ErrorOperationSourceUsageCheck,
			fmt.Sprintf(
				"PVC %s/%s usage could not be read from its storage backend CRD; pass --skip-source-usage-check only after independently verifying that its data fits",
				sourcePVC.Namespace,
				sourcePVC.Name,
			),
			err,
		)
	}

	if usage.UsedBytes < 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			domain.ErrorOperationSourceUsageCheck,
			fmt.Sprintf(
				"PVC %s/%s storage backend returned invalid used bytes %d",
				sourcePVC.Namespace,
				sourcePVC.Name,
				usage.UsedBytes,
			),
		)
	}

	usageSource := strings.TrimSpace(usage.Source)
	if usageSource == "" {
		usageSource = "the storage backend CRD"
	}

	if usage.UsedBytes > destination.Value() {
		if sourcePath != "" && sourcePath != domain.VolumeRootPath {
			return domain.NewError(
				domain.ErrorConflict,
				domain.ErrorOperationSourceUsageCheck,
				fmt.Sprintf(
					"PVC %s/%s whole-volume usage is %d bytes according to %s, above destination capacity %s; this cannot prove that selected source directory %q fits; abort this session and create a new one with a larger destination, or use --skip-source-usage-check only after independently measuring the selected data",
					sourcePVC.Namespace,
					sourcePVC.Name,
					usage.UsedBytes,
					usageSource,
					destination.String(),
					sourcePath,
				),
			)
		}

		return domain.NewError(
			domain.ErrorConflict,
			domain.ErrorOperationSourceUsageCheck,
			fmt.Sprintf(
				"PVC %s/%s uses %d bytes according to %s, above destination capacity %s; increase --destination-capacity or abort this shrink",
				sourcePVC.Namespace,
				sourcePVC.Name,
				usage.UsedBytes,
				usageSource,
				destination.String(),
			),
		)
	}

	return nil
}
