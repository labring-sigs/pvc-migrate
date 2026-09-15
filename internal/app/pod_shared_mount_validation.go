package app

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/apimachinery/pkg/types"
)

func validatePodSharedMountCheckpoints(
	volumes []v1alpha1.VolumeSpec,
	mounts []v1alpha1.SharedMountStatus,
) error {
	sources := make(map[string]types.UID, len(volumes))
	for _, volume := range volumes {
		sources[volume.SourcePV.Name] = volume.SourcePV.UID
	}

	seen := make(map[string]bool, len(mounts))

	resources := make(map[string]bool, len(mounts))
	for _, mount := range mounts {
		resource := mount.LVMVolume.Namespace + "/" + mount.LVMVolume.Name

		uid, exists := sources[mount.SourcePV.Name]
		if !exists || uid != mount.SourcePV.UID || seen[mount.SourcePV.Name] ||
			mount.LVMVolume.Namespace == "" || mount.LVMVolume.Name == "" || mount.LVMVolume.UID == "" ||
			resources[resource] {
			return domain.NewError(
				domain.ErrorValidation,
				"pod migration",
				"shared-mount checkpoints require distinct planned source PVs and LVMVolume identities",
			)
		}

		seen[mount.SourcePV.Name] = true
		resources[resource] = true
	}

	return nil
}
