package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (s *Service) verifyShrinkUsage(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "source usage check", "session is nil")
	}

	options := session.Spec.WorkflowOptions()
	for _, volume := range session.Spec.Volumes {
		source, sourceErr := resource.ParseQuantity(volume.SourceCapacity)

		destination, destinationErr := resource.ParseQuantity(volume.Capacity)
		if sourceErr != nil || destinationErr != nil || destination.Cmp(source) >= 0 {
			continue
		}

		if options.SkipSourceUsageCheck {
			s.logWarn(
				"source usage check skipped by explicit approval",
				"session",
				session.ID,
				"pvc",
				volume.SourcePVC.Name,
				"destinationCapacity",
				destination.String(),
			)

			continue
		}

		if s.config.VolumeUsageReader == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s has no trusted storage-backend CRD usage reader; pass --skip-source-usage-check only after independently verifying that its data fits destination capacity %s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					destination.String(),
				),
			)
		}

		usage, err := s.config.VolumeUsageReader.Read(
			ctx,
			kube.VolumeUsageReadOptions{SourcePVC: volume.SourcePVC, SourcePV: volume.SourcePV},
		)
		if err != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s usage could not be read from its storage backend CRD; pass --skip-source-usage-check only after independently verifying that its data fits",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
				err,
			)
		}

		if usage.UsedBytes < 0 {
			return domain.NewError(
				domain.ErrorPrecondition,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s storage backend returned invalid used bytes %d",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					usage.UsedBytes,
				),
			)
		}

		usageSource := strings.TrimSpace(usage.Source)
		if usageSource == "" {
			usageSource = "the storage backend CRD"
		}

		if usage.UsedBytes > destination.Value() {
			if sourcePath := domain.SourceTransferPath(
				volume.TransferScope,
			); sourcePath != domain.VolumeRootPath {
				return domain.NewError(
					domain.ErrorConflict,
					"source usage check",
					fmt.Sprintf(
						"PVC %s/%s whole-volume usage is %d bytes according to %s, above destination capacity %s; this cannot prove that selected source directory %q fits; abort this session and create a new one with a larger destination, or use --skip-source-usage-check only after independently measuring the selected data",
						volume.SourcePVC.Namespace,
						volume.SourcePVC.Name,
						usage.UsedBytes,
						usageSource,
						destination.String(),
						sourcePath,
					),
				)
			}

			return domain.NewError(
				domain.ErrorConflict,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s uses %d bytes according to %s, above destination capacity %s; increase --destination-capacity or abort this shrink",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					usage.UsedBytes,
					usageSource,
					destination.String(),
				),
			)
		}
	}

	return nil
}

// validateResumePrerequisites performs the checks shared by every resumable
// workflow and returns the persisted phase that should be continued.
func (s *Service) validateResumePrerequisites(
	ctx context.Context,
	session *domain.Session,
) (domain.Phase, error) {
	if session == nil {
		return "", domain.NewError(domain.ErrorValidation, "resume dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return "", err
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return "", err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return "", err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	return phase, nil
}

func invalidWorkflowResumePhase(phase domain.Phase, operation domain.Operation) error {
	return domain.NewError(
		domain.ErrorPrecondition,
		"resume "+string(operation),
		fmt.Sprintf("phase %s cannot be resumed for operation %s", phase, operation),
	)
}
