package backup

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func validateBackupObject(object *v1alpha1.Backup) error {
	if object == nil || object.Name == "" || object.Namespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"backup",
			"workflow name and namespace are required",
		)
	}

	if object.DeletionTimestamp == nil {
		if object.Status.ObservedGeneration != 0 &&
			object.Status.ObservedGeneration != object.Generation {
			return domain.NewError(
				domain.ErrorConflict,
				"backup",
				"workflow spec changed after planning",
			)
		}

		if err := validateBackupSpecPlan(object.Spec, object.Status.Plan); err != nil {
			return err
		}
	}

	if err := validateRepositoryStatus(
		object.Status.WorkflowStatus,
		object.Status.Plan != nil,
	); err != nil {
		return err
	}

	plan := object.Status.Plan
	if plan == nil {
		if object.Status.Repository != nil || len(object.Status.OpenEBSLVMSharedMounts) != 0 {
			return domain.NewError(
				domain.ErrorValidation,
				"backup",
				"resource checkpoints require an execution plan",
			)
		}

		return nil
	}

	if plan.SourcePVC.Name == "" || plan.SourcePVC.UID == "" || plan.SourcePV.Name == "" ||
		plan.SourcePV.UID == "" ||
		plan.Name == "" ||
		plan.RepositoryRef.Name == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"backup",
			"execution plan requires source identities and repository location",
		)
	}

	for _, mount := range object.Status.OpenEBSLVMSharedMounts {
		if mount.SourcePV.Name != plan.SourcePV.Name || mount.SourcePV.UID != plan.SourcePV.UID ||
			mount.LVMVolume.Name == "" ||
			mount.LVMVolume.UID == "" {
			return domain.NewError(
				domain.ErrorValidation,
				"backup",
				"shared mount checkpoint differs from the planned source",
			)
		}
	}

	return nil
}

func validateBackupSpecPlan(spec v1alpha1.BackupSpec, plan *v1alpha1.BackupPlan) error {
	if spec.SourcePVC.Name == "" || spec.RepositoryRef.Name == "" || spec.Name == "" ||
		(spec.OpenEBSLVMEnableShared && !spec.Online) {
		return domain.NewError(
			domain.ErrorValidation,
			"backup",
			"source PVC, repository and backup name are required; shared mounts require online backup",
		)
	}

	if plan != nil {
		if plan.SourcePVC.Name != spec.SourcePVC.Name ||
			(spec.SourcePVC.UID != "" && spec.SourcePVC.UID != plan.SourcePVC.UID) ||
			plan.RepositoryRef != spec.RepositoryRef ||
			plan.Name != spec.Name ||
			plan.Path != spec.Path ||
			plan.Online != spec.Online ||
			plan.OpenEBSLVMEnableShared != spec.OpenEBSLVMEnableShared {
			return domain.NewError(
				domain.ErrorConflict,
				"backup",
				"execution plan differs from the requested backup",
			)
		}

		if spec.SourcePV != nil &&
			(spec.SourcePV.Name != plan.SourcePV.Name || (spec.SourcePV.UID != "" && spec.SourcePV.UID != plan.SourcePV.UID)) {
			return domain.NewError(
				domain.ErrorConflict,
				"backup",
				"planned source PV differs from the requested identity",
			)
		}
	}

	return nil
}

func validateRepositoryStatus(status v1alpha1.WorkflowStatus, planned bool) error {
	if status.FailureReason != "" {
		return domain.NewError(
			domain.ErrorValidation,
			"repository workflow",
			"transfer failure reasons do not belong to repository operations",
		)
	}

	allowed := []v1alpha1.WorkflowPhase{
		domain.PhasePlanned,
		domain.PhaseWarmCopying,
		domain.PhaseWarmCopied,
		domain.PhaseCompleted,
		domain.PhaseAborting,
		domain.PhaseAborted,
		domain.PhaseFailed,
	}

	if status.Phase == "" && status.ResumeFrom == "" && !planned {
		return nil
	}

	if !slices.Contains(allowed, status.Phase) ||
		(status.ResumeFrom != "" && !slices.Contains(allowed, status.ResumeFrom)) {
		return domain.NewError(
			domain.ErrorValidation,
			"repository workflow",
			"invalid lifecycle phase",
		)
	}

	phase := status.Phase
	if phase == domain.PhaseAborting && status.ResumeFrom != domain.PhasePlanned &&
		status.ResumeFrom != domain.PhaseWarmCopying && status.ResumeFrom != domain.PhaseWarmCopied &&
		status.ResumeFrom != domain.PhaseAborting {
		return domain.NewError(
			domain.ErrorValidation,
			"repository workflow",
			"aborting workflow requires a recoverable phase",
		)
	}

	if phase == domain.PhaseFailed {
		phase = status.ResumeFrom
		if phase != domain.PhasePlanned && phase != domain.PhaseWarmCopying &&
			phase != domain.PhaseWarmCopied &&
			phase != domain.PhaseAborting {
			return domain.NewError(
				domain.ErrorValidation,
				"repository workflow",
				"failed workflow requires a recoverable phase",
			)
		}
	}

	if !planned && phase != domain.PhasePlanned && phase != domain.PhaseAborted &&
		phase != domain.PhaseAborting {
		return domain.NewError(
			domain.ErrorValidation,
			"repository workflow",
			"execution requires a persisted plan",
		)
	}

	return nil
}

// normalizeDeletedRepositoryStatus makes deletion idempotent for objects
// persisted by older or interrupted controllers. A failed repository
// workflow without a resume phase cannot continue execution, but deletion
// still needs a valid phase from which abort cleanup can converge.
func normalizeDeletedRepositoryStatus(status *v1alpha1.WorkflowStatus) {
	if status != nil && status.Phase == domain.PhaseFailed && status.ResumeFrom == "" {
		status.ResumeFrom = domain.PhasePlanned
	}
}

func (b *BackupExecutor) validateSharedRestore(
	ctx context.Context,
	id string,
	mounts []v1alpha1.SharedMountStatus,
) error {
	if len(mounts) == 0 {
		return nil
	}

	manager := b.config.SharedVolumeManager
	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"backup",
			"shared mount manager is required for recovery",
		)
	}

	for _, mount := range mounts {
		if err := manager.ValidateRestoreShared(ctx, id, mount); err != nil {
			return err
		}
	}

	return nil
}
