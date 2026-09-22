package backup

import (
	"context"
	"errors"
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

func prepareBackupSharedMounts(
	ctx context.Context,
	manager kube.OpenEBSLVMSharedVolumeManager,
	id string,
	enableShared bool,
	mounts *[]v1alpha1.SharedMountStatus,
	save func(context.Context) error,
	info *PVCInfo,
) error {
	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	if mounts == nil || info == nil || info.PV == nil || len(info.Consumers) == 0 {
		return nil
	}

	if info.PV.Spec.CSI == nil || info.PV.Spec.CSI.Driver != kube.OpenEBSLVMCSIDriver {
		return nil
	}

	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"backup",
			"OpenEBS LVM manager is required for an active OpenEBS LVM PVC",
		)
	}

	if existing, found := backupSharedMount(
		*mounts,
		info.PV,
	); found {
		needsEnable, err := inspectBackupSharedMount(
			ctx,
			manager,
			id,
			existing,
		)
		if err != nil {
			return err
		}

		if needsEnable {
			return manager.EnableShared(ctx, id, existing)
		}

		return nil
	}

	prepared, err := manager.PrepareShared(ctx, v1alpha1.ObjectReference{
		Kind: "PersistentVolume", Name: info.PV.Name, UID: info.PV.UID,
	})
	if err != nil {
		return err
	}

	if !prepared.NeedsChange {
		return nil
	}

	if !enableShared {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup",
			fmt.Sprintf(
				"source PVC %s/%s is active and its OpenEBS LVMVolume is unshared; retry with --openebs-lvm-enable-shared or stop consumers",
				info.PVC.Namespace,
				info.PVC.Name,
			),
		)
	}

	if save == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"backup shared mount",
			"checkpoint store is required before enabling shared mounts",
		)
	}

	mount := v1alpha1.SharedMountStatus{
		SourcePV: v1alpha1.LocalResourceReference{
			Kind: "PersistentVolume",
			Name: info.PV.Name,
			UID:  info.PV.UID,
		},
		LVMVolume:         prepared.LVMVolume,
		PreviousShared:    prepared.PreviousShared,
		PreviousSharedSet: prepared.PreviousSharedSet,
	}

	previous := *mounts

	*mounts = append(
		slices.Clone(previous),
		mount,
	)
	if err := save(ctx); err != nil {
		*mounts = previous
		return err
	}

	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	if enableErr := manager.EnableShared(ctx, id, mount); enableErr != nil {
		owned, ownershipErr := manager.Shared(
			ctx,
			v1alpha1.ObjectReference{Name: mount.SourcePV.Name, UID: mount.SourcePV.UID},
			mount.LVMVolume,
			id,
		)
		if (ownershipErr == nil && !owned) ||
			domain.CategoryOf(ownershipErr) == domain.ErrorConflict {
			checkpoint := *mounts
			*mounts = previous

			persistErr := save(ctx)
			if persistErr != nil {
				*mounts = checkpoint
			}

			return errors.Join(enableErr, persistErr)
		}

		return errors.Join(enableErr, ownershipErr)
	}

	return nil
}

func backupSharedMount(
	mounts []v1alpha1.SharedMountStatus,
	pv *corev1.PersistentVolume,
) (v1alpha1.SharedMountStatus, bool) {
	if pv == nil {
		return v1alpha1.SharedMountStatus{}, false
	}

	for _, mount := range mounts {
		if mount.SourcePV.Name == pv.Name && mount.SourcePV.UID == pv.UID {
			return mount, true
		}
	}

	return v1alpha1.SharedMountStatus{}, false
}

// A shared-mount checkpoint is persisted before the LVMVolume is changed. If
// the process exits between those operations, the unchanged original state is
// safe to enable when the backup resumes.
func inspectBackupSharedMount(
	ctx context.Context,
	manager kube.OpenEBSLVMSharedVolumeManager,
	sessionID string,
	mount v1alpha1.SharedMountStatus,
) (bool, error) {
	shared, err := manager.Shared(
		ctx,
		v1alpha1.ObjectReference{Name: mount.SourcePV.Name, UID: mount.SourcePV.UID},
		mount.LVMVolume,
		sessionID,
	)
	if err == nil {
		if !shared {
			return false, domain.NewError(
				domain.ErrorConflict,
				backupResumePhase,
				"session-managed OpenEBS LVM shared mount is no longer enabled",
			)
		}

		return false, nil
	}

	if domain.CategoryOf(err) != domain.ErrorConflict {
		return false, err
	}

	if restoreErr := manager.ValidateRestoreShared(
		ctx,
		sessionID,
		mount,
	); restoreErr != nil {
		return false, restoreErr
	}

	return true, nil
}

func validateBackupSharedSource(ctx context.Context, manager kube.OpenEBSLVMSharedVolumeManager,
	id string, enableShared bool, mounts []v1alpha1.SharedMountStatus, hasStore bool, info *PVCInfo,
) error {
	if info == nil || info.PV == nil || len(info.Consumers) == 0 ||
		info.PV.Spec.CSI == nil || info.PV.Spec.CSI.Driver != kube.OpenEBSLVMCSIDriver {
		return nil
	}

	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			backupPreflightPhase,
			"OpenEBS LVM manager is required to inspect an active OpenEBS LVM PVC",
		)
	}

	if existing, found := backupSharedMount(mounts, info.PV); found {
		_, err := inspectBackupSharedMount(ctx, manager, id, existing)
		return err
	}

	prepared, err := manager.PrepareShared(ctx, v1alpha1.ObjectReference{
		Kind: "PersistentVolume", Name: info.PV.Name, UID: info.PV.UID,
	})
	if err != nil {
		return err
	}

	if prepared.NeedsChange && !enableShared {
		return domain.NewError(
			domain.ErrorPrecondition,
			backupPreflightPhase,
			fmt.Sprintf(
				"source PVC %s/%s is active and its OpenEBS LVMVolume is unshared; retry with --openebs-lvm-enable-shared or stop consumers",
				info.PVC.Namespace,
				info.PVC.Name,
			),
		)
	}

	if prepared.NeedsChange && !hasStore {
		return domain.NewError(
			domain.ErrorPrecondition,
			backupPreflightPhase,
			"a session store is required to recover temporary OpenEBS LVM shared state",
		)
	}

	return nil
}
